# Add your plugins and plugin settings here.
# Of course uncomment this file out.
# To learn how to build images with your required plugins
# See https://github.com/netbox-community/netbox-docker/wiki/Using-Netbox-Plugins

PLUGINS = [
    "forward_netbox",
    "netbox_branching",
    "netbox_routing",
    "netbox_peering_manager",
]

PLUGINS_CONFIG = {  # type: ignore
    "forward_netbox": {
        "enable_bgp_sync": True,
    },
}
