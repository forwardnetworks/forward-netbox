from netbox.plugins import PluginConfig


class NetboxForwardConfig(PluginConfig):
    name = "forward_netbox"
    verbose_name = "NetBox Forward Networks SoT Plugin"
    description = "Sync Forward Networks into NetBox"
    version = "4.3.0"
    base_url = "forward"
    min_version = "4.4.0"

config = NetboxForwardConfig
