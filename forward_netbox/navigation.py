from netbox.plugins import PluginMenu, PluginMenuButton, PluginMenuItem

# Buttons
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

nqe_map_buttons = [
    PluginMenuButton(
        link="plugins:forward_netbox:forwardnqemap_add",
        title="Add",
        icon_class="mdi mdi-plus-thick",
        permissions=["forward_netbox.add_forwardnqemap"],
    )
]

# Menu Items
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
    link_text="Ingestion",
    buttons=sync_buttons,
    permissions=["forward_netbox.view_forwardsync"],
)

nqem = PluginMenuItem(
    link="plugins:forward_netbox:forwardnqemap_list",
    link_text="NQE Maps",
    permissions=["forward_netbox.view_forwardnqemap"],
    buttons=nqe_map_buttons,
)

# Plugin Menu
menu = PluginMenu(
    label="Forward Networks",
    icon_class="mdi mdi-cloud-sync",
    groups=(("Forward Enterprise", (source, snapshot, ingestion, nqem)),),
)
