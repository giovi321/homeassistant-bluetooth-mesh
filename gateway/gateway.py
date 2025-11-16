import asyncio
import logging
import secrets
import argparse
import uuid
import os

from contextlib import AsyncExitStack, suppress

from bluetooth_mesh.application import Application, Element
from bluetooth_mesh.crypto import ApplicationKey, DeviceKey, NetworkKey
from bluetooth_mesh.messages.config import GATTNamespaceDescriptor
from bluetooth_mesh import models
from dbus_next.aio import MessageBus
from dbus_next.constants import BusType
from dbus_next.errors import DBusError, InterfaceNotFoundError

from tools import Config, Store, Tasks
from mesh import Node, NodeManager
from mqtt import HassMqttMessenger

from modules.provisioner import ProvisionerModule
from modules.scanner import ScannerModule
from modules.manager import ManagerModule

from mesh.nodes.light import Light


logging.basicConfig(level=logging.DEBUG)


MESH_MODULES = {
    "prov": ProvisionerModule(),
    "scan": ScannerModule(),
    "mgmt": ManagerModule(),
}


NODE_TYPES = {
    "generic": Node,
    "light": Light,
}


class MainElement(Element):
    """
    Represents the main element of the application node
    """

    LOCATION = GATTNamespaceDescriptor.MAIN
    MODELS = [
        models.ConfigClient,
        models.HealthClient,
        models.GenericOnOffClient,
        models.LightLightnessClient,
        models.LightCTLClient,
    ]


class MqttGateway(Application):

    COMPANY_ID = 0x05F1  # The Linux Foundation
    PRODUCT_ID = 1
    VERSION_ID = 1
    ELEMENTS = {
        0: MainElement,
    }
    CRPL = 32768
    PATH = "/org/hass/mesh"

    DEFAULT_NETWORK_TRANSMIT_COUNT = 0
    DEFAULT_NETWORK_TRANSMIT_INTERVAL_STEPS = 1

    def __init__(self, loop, basedir):
        super().__init__(loop)

        self._store = Store(location=os.path.join(basedir, "store.yaml"))
        self._config = Config(os.path.join(basedir, "config.yaml"))
        self._nodes = {}

        self._messenger = None

        self._app_keys = None
        self._dev_key = None
        self._primary_net_key = None
        self._new_keys = set()

        # load mesh modules
        for name, module in MESH_MODULES.items():
            module.initialize(self, self._store.section(name), self._config)

        self._initialize()

    @property
    def dev_key(self):
        if not self._dev_key:
            raise Exception("Device key not ready")
        return self._dev_key

    @property
    def primary_net_key(self):
        if not self._primary_net_key:
            raise Exception("Primary network key not ready")
        return 0, self._primary_net_key

    @property
    def app_keys(self):
        if not self._app_keys:
            raise Exception("Application keys not ready")
        return self._app_keys

    @property
    def nodes(self):
        return self._nodes

    def _load_key(self, keychain, name):
        if name not in keychain:
            logging.info(f"Generating {name}...")
            keychain[name] = secrets.token_hex(16)
            self._new_keys.add(name)
        try:
            return bytes.fromhex(keychain[name])
        except:
            raise Exception("Invalid device key")

    def _initialize(self):
        keychain = self._store.get("keychain") or {}
        local = self._store.section("local")
        nodes = self._store.section("nodes")

        # load or set application parameters
        self.address = local.get("address", 1)
        self.iv_index = local.get("iv_index", 5)

        # load or generate keys
        self._dev_key = DeviceKey(self._load_key(keychain, "device_key"))
        self._primary_net_key = NetworkKey(self._load_key(keychain, "network_key"))
        self._app_keys = [
            # currently just a single application key supported
            (0, 0, ApplicationKey(self._load_key(keychain, "app_key"))),
        ]

        # initialize node manager
        self._nodes = NodeManager(nodes, self._config, NODE_TYPES)

        # initialize MQTT messenger
        self._messenger = HassMqttMessenger(self._config, self._nodes)

        # persist changes
        self._store.set("keychain", keychain)
        self._store.persist()

    async def _import_keys(self):
        logging.info("Importing keys...")

        if "primary_net_key" in self._new_keys:
            # register primary network key as subnet key
            await self.management_interface.import_subnet(0, self.primary_net_key[1])
            logging.info("Imported primary net key as subnet key")

        if "app_key" in self._new_keys:
            # import application key into daemon
            await self.management_interface.import_app_key(*self.app_keys[0])
            logging.info("Imported app key")

        # update application key for client models
        client = self.elements[0][models.GenericOnOffClient]
        await client.bind(self.app_keys[0][0])
        client = self.elements[0][models.LightLightnessClient]
        await client.bind(self.app_keys[0][0])
        client = self.elements[0][models.LightCTLClient]
        await client.bind(self.app_keys[0][0])

    async def _try_bind_node(self, node):
        try:
            await node.bind(self)
            logging.info(f"Bound node {node}")
            node.ready.set()
        except:
            logging.exception(f"Failed to bind node {node}")

    def scan_result(self, rssi, data, options):
        MESH_MODULES["scan"]._scan_result(rssi, data, options)

    def request_prov_data(self, count):
        return MESH_MODULES["prov"]._request_prov_data(count)

    def add_node_complete(self, uuid, unicast, count):
        MESH_MODULES["prov"]._add_node_complete(uuid, unicast, count)

    def add_node_failed(self, uuid, reason):
        MESH_MODULES["prov"]._add_node_failed(uuid, reason)

    def shutdown(self, tasks):
        self._messenger.shutdown()

    async def run(self, args):
        async with AsyncExitStack() as stack:
            tasks = await stack.enter_async_context(Tasks())

            # connect to daemon
            await self._enter_dbus_context(stack)
            await self._connect_with_recovery()

            # leave network
            if args.leave:
                await self.leave()
                self._nodes.reset()
                self._nodes.persist()
                return

            await self._configure_network_transmit()

            try:
                # set overall application key
                await self.add_app_key(*self.app_keys[0])
            except:
                logging.exception(f"Failed to set app key {self._app_keys[0][2].bytes.hex()}")

                # try to re-add application key
                await self.delete_app_key(self.app_keys[0][0], self.app_keys[0][1])
                await self.add_app_key(*self.app_keys[0])

            # force reloading keys
            if args.reload:
                self._new_keys.add("primary_net_key")
                self._new_keys.add("app_key")

            # configure all keys
            await self._import_keys()

            # run user task if specified
            if "handler" in args:
                await args.handler(args)
                return

            # initialize all nodes
            for node in self._nodes.all():
                tasks.spawn(self._try_bind_node(node), f"bind {node}")

            # start MQTT task
            tasks.spawn(self._messenger.run(self), "run messenger")

            # wait for all tasks
            await tasks.gather()

    async def _configure_network_transmit(self):
        """Clamp and apply the network transmit configuration so we do not flood the mesh."""

        mgmt = getattr(self, "management_interface", None)
        if mgmt is None:
            logging.warning("Mesh management interface unavailable; cannot configure network transmit")
            return

        set_transmit = getattr(mgmt, "set_transmit", None)
        if set_transmit is None:
            logging.warning("Mesh management interface exposes no set_transmit method; skipping transmit tuning")
            return

        count = self._config.optional(
            "network_transmit.count",
            self.DEFAULT_NETWORK_TRANSMIT_COUNT,
        )
        interval_steps = self._config.optional(
            "network_transmit.interval_steps",
            self.DEFAULT_NETWORK_TRANSMIT_INTERVAL_STEPS,
        )

        try:
            count = int(count)
            interval_steps = int(interval_steps)
        except (TypeError, ValueError):
            logging.warning(
                "Invalid network_transmit configuration (%s, %s); falling back to defaults",
                count,
                interval_steps,
            )
            count = self.DEFAULT_NETWORK_TRANSMIT_COUNT
            interval_steps = self.DEFAULT_NETWORK_TRANSMIT_INTERVAL_STEPS

        count = max(0, min(7, count))
        interval_steps = max(0, min(31, interval_steps))

        async def _try_call(signature, *args, **kwargs):
            try:
                await set_transmit(*args, **kwargs)
                return True
            except TypeError:
                return None
            except Exception:
                logging.exception("Failed to configure network transmit using %s signature", signature)
                return False

        # Support older python-bluetooth-mesh releases whose signature may not
        # accept keywords yet.
        for signature, call_kwargs in (
            ("positional", {"args": (count, interval_steps)}),
            ("interval keyword", {"kwargs": {"count": count, "interval": interval_steps}}),
            ("interval_steps keyword", {"kwargs": {"count": count, "interval_steps": interval_steps}}),
        ):
            result = await _try_call(
                signature,
                *(call_kwargs.get("args", ())),
                **call_kwargs.get("kwargs", {}),
            )
            if result is True:
                logging.info(
                    "Configured network transmit: count=%s (total=%s) interval_steps=%s",
                    count,
                    count + 1,
                    interval_steps,
                )
                return
            if result is False:
                return

        logging.warning("Failed to call set_transmit; unexpected method signature")

    async def _connect_with_recovery(self):
        try:
            await self.connect()
        except DBusError as err:
            if await self._recover_from_connect_failure(err):
                return
            raise

    async def _enter_dbus_context(self, stack):
        """Ensure the Application enters its dbus context once the mesh service is ready."""

        await self._wait_for_mesh_network_interface()

        delay = 1
        while True:
            try:
                await stack.enter_async_context(self)
                return
            except InterfaceNotFoundError:
                logging.warning(
                    "Mesh service still missing org.bluez.mesh.Network1 interface; forcing re-introspection in %ss",
                    delay,
                )
                await asyncio.sleep(delay)
                delay = min(delay * 2, 30)
                await self._wait_for_mesh_network_interface()

    async def _recover_from_connect_failure(self, err):
        text = getattr(err, "text", str(err)) or ""
        name = getattr(err, "_name", "") or ""
        if "Node already exists" in text or name.endswith("AlreadyExists"):
            logging.warning(
                "Mesh daemon already knows this node, attempting to delete stale instance before retrying attach"
            )
            if not await self._delete_stale_mesh_node():
                return False
            logging.info("Deleted stale mesh application node, retrying connect")
            await asyncio.sleep(1)
            await self.connect()
            return True
        logging.error("Mesh attach/import failed: %s", text)
        return False

    async def _delete_stale_mesh_node(self):
        """Best-effort removal of the previously registered mesh node."""

        delete_node = getattr(self.management_interface, "delete_node", None)
        if delete_node:
            try:
                await delete_node(self.address)
                return True
            except Exception:
                logging.exception("Failed to delete stale application node from mesh daemon")
                return False

        # Older versions of python-bluetooth-mesh expose only the raw dbus-next
        # proxy interface. Attempt to invoke DeleteNode directly before giving
        # up so operators do not need to clean up state manually.
        proxy_interface = getattr(self.management_interface, "_interface", None)
        call_delete_node = getattr(proxy_interface, "call_delete_node", None)
        if call_delete_node:
            try:
                await call_delete_node(self.address)
                return True
            except Exception:
                logging.exception("Failed to delete stale application node via proxy interface")
                return False

        logging.error(
            "Mesh management interface exposes no delete_node method; manual cleanup required"
        )
        return False

    async def _wait_for_mesh_network_interface(self):
        """Block until org.bluez.mesh exports Network1 so Application entry will succeed."""

        delay = 1
        while True:
            bus = MessageBus(bus_type=BusType.SYSTEM)
            try:
                await bus.connect()
                introspection = await bus.introspect("org.bluez.mesh", "/org/bluez/mesh")
                if any(interface.name == "org.bluez.mesh.Network1" for interface in introspection.interfaces):
                    return
            except Exception:
                # fall through to retry logging below
                pass
            finally:
                with suppress(Exception):
                    bus.disconnect()

            logging.warning(
                "Mesh service missing org.bluez.mesh.Network1 interface; bluetooth-meshd may still be starting. Retrying in %ss",
                delay,
            )
            await asyncio.sleep(delay)
            delay = min(delay * 2, 30)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--leave", action="store_true")
    parser.add_argument("--reload", action="store_true")
    parser.add_argument("--basedir", default="..")

    # module specific CLI interfaces
    subparsers = parser.add_subparsers()
    for name, module in MESH_MODULES.items():
        subparser = subparsers.add_parser(name)
        subparser.set_defaults(handler=module.handle_cli)
        module.setup_cli(subparser)

    args = parser.parse_args()

    loop = asyncio.get_event_loop()
    app = MqttGateway(loop, args.basedir)

    with suppress(KeyboardInterrupt):
        loop.run_until_complete(app.run(args))


if __name__ == "__main__":
    main()
