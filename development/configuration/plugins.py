import os

# Add your plugins and plugin settings here.
# Of course uncomment this file out.
# To learn how to build images with your required plugins
# See https://github.com/netbox-community/netbox-docker/wiki/Using-Netbox-Plugins

netbox_ver = os.environ.get("NETBOX_VER", "")

PLUGINS = [
    "forward_netbox",
    "netbox_branching",
]

# netbox-routing 0.4.2 and netbox-peering-manager 0.2.2 (the pinned versions in
# development/Dockerfile) are NOT compatible with NetBox 4.6.x: routing 0.4.2 uses
# the pre-Django-5.1 `CheckConstraint(check=...)` kwarg (renamed to `condition=`)
# and peering-manager 0.2.2 caps at max_version 4.5.99. Enabling either on 4.6
# breaks plugin load, so they are only enabled on a 4.5 image. On the supported
# 4.6.x line the routing/peering optional-integration tests skip until upstream
# ships 4.6-compatible releases and the Dockerfile pins are bumped.
if netbox_ver.startswith("v4.5"):
    PLUGINS.extend(
        [
            "netbox_routing",
            "netbox_peering_manager",
        ]
    )

if os.environ.get("FORWARD_NETBOX_ENABLE_ACI_PLUGIN", "").lower() in (
    "1",
    "true",
    "yes",
    "on",
):
    PLUGINS.append("netbox_cisco_aci")

PLUGINS_CONFIG = {  # type: ignore
    "forward_netbox": {
        "enable_bgp_sync": True,
    },
}
