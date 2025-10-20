from netbox.plugins import PluginMenu
from netbox.plugins import PluginMenuButton
from netbox.plugins import PluginMenuItem


sync_buttons = [
    PluginMenuButton(
        link="plugins:forward_netbox:forwardsync_add",
        title="Add",
        icon_class="mdi mdi-plus-thick",
        permissions=["forward_netbox.add_forwardsync"],
    )
]

source_buttons = [
    PluginMenuButton(
        link="plugins:forward_netbox:forwardsource_add",
        title="Add",
        icon_class="mdi mdi-plus-thick",
        permissions=["forward_netbox.add_forwardsource"],
    )
]

source = PluginMenuItem(
    link="plugins:forward_netbox:forwardsource_list",
    link_text="Sources",
    buttons=source_buttons,
    permissions=["forward_netbox.view_forwardsource"],
)

snapshot = PluginMenuItem(
    link="plugins:forward_netbox:forwardsnapshot_list",
    link_text="Snapshots",
    permissions=["forward_netbox.view_forwardsnapshot"],
)


ingestion = PluginMenuItem(
    link="plugins:forward_netbox:forwardsync_list",
    link_text="Syncs",
    buttons=sync_buttons,
    permissions=["forward_netbox.view_forwardsync"],
)

nqe_buttons = [
    PluginMenuButton(
        link="plugins:forward_netbox:forwardnqequery_add",
        title="Add",
        icon_class="mdi mdi-plus-thick",
        permissions=["forward_netbox.add_forwardnqequery"],
    )
]

nqe_map = PluginMenuItem(
    link="plugins:forward_netbox:forwardnqequery_list",
    link_text="NQE Maps",
    permissions=["forward_netbox.view_forwardnqequery"],
    buttons=nqe_buttons,
)
menu = PluginMenu(
    label="Forward Enterprise",
    icon_class="mdi mdi-cloud-sync",
    groups=(("Forward Enterprise", (source, snapshot, ingestion, nqe_map)),),
)
