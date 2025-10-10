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

tmg = PluginMenuItem(
    link="plugins:forward_netbox:forwardtransformmapgroup_list",
    link_text="Transform Map Groups",
    permissions=["forward_netbox.view_forwardtransformmapgroup"],
    buttons=[
        PluginMenuButton(
            link="plugins:forward_netbox:forwardtransformmapgroup_add",
            title="Add",
            icon_class="mdi mdi-plus-thick",
            permissions=["forward_netbox.add_forwardtransformmapgroup"],
        )
    ],
)

tm = PluginMenuItem(
    link="plugins:forward_netbox:forwardtransformmap_list",
    link_text="Transform Maps",
    permissions=["forward_netbox.view_forwardtransformmap"],
    buttons=[
        PluginMenuButton(
            link="plugins:forward_netbox:forwardtransformmap_add",
            title="Add",
            icon_class="mdi mdi-plus-thick",
            permissions=["forward_netbox.add_forwardtransformmap"],
        )
    ],
)
menu = PluginMenu(
    label="Forward",
    icon_class="mdi mdi-cloud-sync",
    groups=(("Forward", (source, snapshot, ingestion, tmg, tm)),),
)
