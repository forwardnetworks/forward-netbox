from netbox.plugins import PluginConfig


class NetboxForwardConfig(PluginConfig):
    name = "forward_netbox"
    verbose_name = "NetBox Forward Networks SoT Plugin"
    description = "Sync Forward Networks into NetBox"
    version = "0.9.0"
    base_url = "forward"
    min_version = "4.3.0"


config = NetboxForwardConfig
