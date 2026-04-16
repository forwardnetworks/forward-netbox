from netbox.plugins import PluginConfig


class NetboxForwardConfig(PluginConfig):
    name = "forward_netbox"
    verbose_name = "NetBox Forward Networks Plugin"
    description = "Sync Forward Networks data into NetBox using built-in NQE queries."
    version = "0.1.2"
    base_url = "forward"
    min_version = "4.5.0"

    def ready(self):
        super().ready()
        from . import signals  # noqa: F401


config = NetboxForwardConfig
