from netbox.plugins import PluginConfig


class NetboxForwardConfig(PluginConfig):
    name = "forward_netbox"
    verbose_name = "NetBox Forward Enterprise SoT Plugin"
    description = "Sync Forward Enterprise into NetBox"
    version = "1.0.0"
    base_url = "forward"
    min_version = "4.4.0"

config = NetboxForwardConfig
