# Add your plugins and plugin settings here.
# Of course uncomment this file out.
# To learn how to build images with your required plugins
# See https://github.com/netbox-community/netbox-docker/wiki/Using-Netbox-Plugins
# The development and CI runtime enables every optional integration that can
# boot on this NetBox, so adapter regressions cannot hide behind skipped tests.
# On 4.7 that is four of the five: netbox-dlm 0.10.0, netbox-validity 3.6.0,
# netbox-peering-manager 0.3.1, and netbox-routing from upstream main (0.4.4,
# 4.7 support merged but not yet tagged). netbox-cisco-aci still declares
# `max_version = "4.6.99"` and NetBox refuses to start with a plugin outside
# its range, so its tests skip on this runtime rather than fail - a real loss
# of coverage, stated as such; the 2.9.x lane on 4.6 exercises that adapter
# until its upstream moves.
PLUGINS = [
    "netbox_branching",
    "netbox_dlm",
    "netbox_routing",
    "netbox_peering_manager",
    "validity",
    "forward_netbox",
]

PLUGINS_CONFIG = {  # type: ignore
    "forward_netbox": {
        "enable_bgp_sync": True,
    },
}
