# Add your plugins and plugin settings here.
# Of course uncomment this file out.
# To learn how to build images with your required plugins
# See https://github.com/netbox-community/netbox-docker/wiki/Using-Netbox-Plugins
# On NetBox 4.6 this enabled every supported optional integration, so adapter
# regressions could not hide behind skipped tests. On 4.7 none of them can be
# enabled: netbox-dlm, netbox-cisco-aci, netbox-peering-manager, netbox-routing
# and netbox-validity all declare `max_version = "4.6.99"` and NetBox refuses to
# start with a plugin outside its range.
#
# The optional-integration tests therefore skip on this runtime rather than
# fail. That is a real loss of coverage and is stated as such: the 2.9.x lane on
# 4.6 remains where those adapters are exercised until an upstream moves.
PLUGINS = [
    "netbox_branching",
    "forward_netbox",
]

PLUGINS_CONFIG = {  # type: ignore
    "forward_netbox": {
        "enable_bgp_sync": True,
    },
}
