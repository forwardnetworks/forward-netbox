# Add your plugins and plugin settings here.
# Of course uncomment this file out.
# To learn how to build images with your required plugins
# See https://github.com/netbox-community/netbox-docker/wiki/Using-Netbox-Plugins
# The development and CI runtime enables every supported optional integration at
# the exact version pinned in constraints.txt. Production installs still choose
# extras explicitly, but adapter regressions cannot hide behind skipped tests.
PLUGINS = [
    "netbox_branching",
    "netbox_dlm",
    "netbox_routing",
    "netbox_peering_manager",
    "netbox_cisco_aci",
    "forward_netbox",
]

PLUGINS_CONFIG = {  # type: ignore
    "forward_netbox": {
        "enable_bgp_sync": True,
    },
}
