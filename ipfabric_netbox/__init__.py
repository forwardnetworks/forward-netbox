from django.db.models.signals import post_delete
from netbox.plugins import PluginConfig


class NetboxIPFabricConfig(PluginConfig):
    name = "ipfabric_netbox"
    verbose_name = "NetBox IP Fabric SoT Plugin"
    description = "Sync IP Fabric into NetBox"
    version = "4.3.0"
    base_url = "ipfabric"
    min_version = "4.4.0"

    def ready(self):
        super().ready()

        from ipfabric_netbox.signals import remove_group_from_syncs

        post_delete.connect(
            remove_group_from_syncs,
            sender="ipfabric_netbox.IPFabricTransformMapGroup",
            dispatch_uid="remove_group_from_syncs",
        )


config = NetboxIPFabricConfig
