from django.db.models.signals import post_delete
from netbox.plugins import PluginConfig


class NetboxForwardConfig(PluginConfig):
    name = "forward_netbox"
    verbose_name = "NetBox Forward SoT Plugin"
    description = "Sync Forward into NetBox"
    version = "4.3.0"
    base_url = "forward"
    min_version = "4.4.0"

    def ready(self):
        super().ready()

        from forward_netbox.signals import remove_group_from_syncs

        post_delete.connect(
            remove_group_from_syncs,
            sender="forward_netbox.ForwardTransformMapGroup",
            dispatch_uid="remove_group_from_syncs",
        )


config = NetboxForwardConfig
