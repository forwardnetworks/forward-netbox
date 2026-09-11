from django.utils.translation import gettext as _
from netbox.plugins import PluginMenu
from netbox.plugins import PluginMenuButton
from netbox.plugins import PluginMenuItem


source = PluginMenuItem(
    link="plugins:forward_netbox:forwardsource_list",
    link_text=_("Sources"),
    buttons=[
        PluginMenuButton(
            link="plugins:forward_netbox:forwardsource_add",
            title=_("Add"),
            icon_class="mdi mdi-plus-thick",
            permissions=["forward_netbox.add_forwardsource"],
        )
    ],
    permissions=["forward_netbox.view_forwardsource"],
)

sync = PluginMenuItem(
    link="plugins:forward_netbox:forwardsync_list",
    link_text=_("Syncs"),
    buttons=[
        PluginMenuButton(
            link="plugins:forward_netbox:forwardsync_add",
            title=_("Add"),
            icon_class="mdi mdi-plus-thick",
            permissions=["forward_netbox.add_forwardsync"],
        )
    ],
    permissions=["forward_netbox.view_forwardsync"],
)

ingestion = PluginMenuItem(
    link="plugins:forward_netbox:forwardingestion_list",
    link_text=_("Ingestions"),
    permissions=["forward_netbox.view_forwardingestion"],
)

# The route existed and nothing reached it: issues were visible only through
# the ingestion that produced them, so "which failures are blocking anything,
# anywhere" had no page at all and was answered by a command.
ingestion_issue = PluginMenuItem(
    link="plugins:forward_netbox:forwardingestionissue_list",
    link_text=_("Ingestion Issues"),
    permissions=["forward_netbox.view_forwardingestionissue"],
)

validation_run = PluginMenuItem(
    link="plugins:forward_netbox:forwardvalidationrun_list",
    link_text=_("Validation Runs"),
    permissions=["forward_netbox.view_forwardvalidationrun"],
)

nqe_map = PluginMenuItem(
    link="plugins:forward_netbox:forwardnqemap_list",
    link_text=_("NQE Maps"),
    buttons=[
        PluginMenuButton(
            link="plugins:forward_netbox:forwardnqemap_add",
            title=_("Add"),
            icon_class="mdi mdi-plus-thick",
            permissions=["forward_netbox.add_forwardnqemap"],
        ),
    ],
    permissions=["forward_netbox.view_forwardnqemap"],
)

drift_policy = PluginMenuItem(
    link="plugins:forward_netbox:forwarddriftpolicy_list",
    link_text=_("Drift Policies"),
    buttons=[
        PluginMenuButton(
            link="plugins:forward_netbox:forwarddriftpolicy_add",
            title=_("Add"),
            icon_class="mdi mdi-plus-thick",
            permissions=["forward_netbox.add_forwarddriftpolicy"],
        )
    ],
    permissions=["forward_netbox.view_forwarddriftpolicy"],
)

device_analysis = PluginMenuItem(
    link="plugins:forward_netbox:forwarddeviceanalysis_list",
    link_text=_("Device Analysis"),
    permissions=["forward_netbox.view_forwarddeviceanalysis"],
)

menu = PluginMenu(
    label="Forward",
    icon_class="mdi mdi-cloud-sync",
    groups=(
        ("Data Sync", (source, sync, ingestion, ingestion_issue, validation_run)),
        ("Analysis", (device_analysis,)),
        ("Configuration", (nqe_map, drift_policy)),
    ),
)
