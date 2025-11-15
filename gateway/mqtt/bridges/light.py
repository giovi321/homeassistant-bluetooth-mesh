from sre_constants import BIGCHARSET
from mqtt.bridge import HassMqttBridge
from mesh.nodes.light import Light


class GenericLightBridge(HassMqttBridge):
    """
    Generic bridge for lights
    """

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    @property
    def component(self):
        return "light"

    async def config(self, node):
        color_modes = set()
        message = {
            "~": self._messenger.node_topic(self.component, node),
            "name": node.config.optional("name"),
            "unique_id": node.config.require("id"),
            "object_id": node.config.require("id"),
            "command_topic": "~/set",
            "state_topic": "~/state",
            "schema": "json",
        }

        if node.supports(Light.BrightnessProperty):
            message["brightness_scale"] = 50
            message["brightness"] = True

        if node.supports(Light.TemperatureProperty):
            color_modes.add("color_temp")
            # convert from Kelvin to mireds
            # TODO: look up max/min values from device
            # message['min_mireds'] = 1000000 // 7000
            # message['max_mireds'] = 1000000 // 2000

        if color_modes:
            message["color_mode"] = True
            message["supported_color_modes"] = list(color_modes)

        await self._messenger.publish(self.component, node, "config", message)

    async def _state(self, node, onoff):
        """
        Send a generic state message covering the nodes full state

        If the light is on, all properties are set to their retained state.
        If the light is off, properties are not passed at all.
        """
        message = {"state": "ON" if onoff else "OFF"}

        if onoff and node.supports(Light.BrightnessProperty):
            message["brightness"] = node.retained(Light.BrightnessProperty, 100)
        if onoff and node.supports(Light.TemperatureProperty):
            message["color_temp"] = node.retained(Light.TemperatureProperty, 100)

        await self._messenger.publish(self.component, node, "state", message, retain=True)

    async def _mqtt_set(self, node, payload):
        color_temp = payload.get("color_temp")
        brightness = payload.get("brightness")

        # Home Assistant sends color temperature and brightness together when the
        # user adjusts the color temperature slider. Dispatch both properties in
        # a single CTL message to avoid flooding the mesh with multiple
        # back-to-back commands.
        if node.supports(Light.TemperatureProperty) and (color_temp is not None or brightness is not None):
            temperature = None
            if color_temp is not None:
                temperature = 1000000 // color_temp
            await node.set_ctl_unack(temperature=temperature, brightness=brightness)
        else:
            if color_temp is not None:
                await node.set_mireds(color_temp)
            if brightness is not None:
                await node.set_brightness(brightness)

        if payload.get("state") == "ON":
            await node.turn_on()
        if payload.get("state") == "OFF":
            await node.turn_off()

    async def _notify_onoff(self, node, onoff):
        await self._state(node, onoff)

    async def _notify_brightness(self, node, brightness):
        await self._state(node, brightness > 0)
