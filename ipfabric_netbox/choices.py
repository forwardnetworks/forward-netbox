from django.utils.translation import gettext_lazy as _
from utilities.choices import ChoiceSet

transform_field_source_columns = {
    "site": [
        "id",
        "siteName",
        "devicesCount",
        "usersCount",
        "stpDCount",
        "switchesCount",
        "vlanCount",
        "rDCount",
        "routersCount",
        "networksCount",
    ],
    "device": [
        "id",
        "sn",
        "hostname",
        "siteName",
        "rd",
        "stpDomain",
        "snHw",
        "loginIp",
        "objectId",
        "loginType",
        "uptime",
        "reload",
        "memoryUtilization",
        "memoryTotalBytes",
        "memoryUsedBytes",
        "vendor",
        "family",
        "platform",
        "model",
        "configReg",
        "version",
        "image",
        "processor",
        "mac",
        "devType",
        "hostnameOriginal",
        "hostnameProcessed",
        "domain",
        "fqdn",
    ],
    "inventory": [
        "id",
        "deviceSn",
        "hostname",
        "siteName",
        "deviceId",
        "name",
        "dscr",
        "pid",
        "sn",
        "vid",
        "vendor",
        "platform",
        "model",
    ],
    "interface": [
        "id",
        "dscr",
        "duplex",
        "errDisabled",
        "hasTransceiver",
        "hostname",
        "intName",
        "intNameAlias",
        "l1",
        "l2",
        "loginIp",
        "loginType",
        "mac",
        "media",
        "mtu",
        "nameOriginal",
        "primaryIp",
        "reason",
        "rel",
        "siteName",
        "sn",
        "speed",
        "speedValue",
        "transceiverPn",
        "transceiverSn",
        "transceiverType",
    ],
    "part_number": [
        "id",
        "deviceSn",
        "hostname",
        "siteName",
        "deviceId",
        "name",
        "dscr",
        "pid",
        "sn",
        "vid",
        "vendor",
        "platform",
        "model",
    ],
    "vlan": ["id", "siteName", "vlanId", "vlanName", "dscr", "devCount"],
    "vrf": ["id", "sn", "hostname", "siteName", "vrf", "rd", "intCount"],
    "prefix": ["id", "siteName", "net", "hosts", "gw", "gwV", "vrf", "vlanId"],
    "virtualchassis": [
        "id",
        "sn",
        "master",
        "siteName",
        "uptime",
        "member",
        "connectionsCount",
        "pn",
        "memberSn",
        "role",
        "state",
        "mac",
        "ver",
        "image",
        "hwVer",
    ],
    "ipaddress": [
        "hostname",
        "sn",
        "intName",
        "stateL1",
        "stateL2",
        "siteName",
        "dnsName",
        "dnsHostnameMatch",
        "vlanId",
        "dnsReverseMatch",
        "mac",
        "ip",
        "net",
        "type",
        "vrf",
    ],
}

required_transform_map_contenttypes = [
    ("dcim", "site"),
    ("dcim", "manufacturer"),
    ("dcim", "platform"),
    ("dcim", "devicerole"),
    ("dcim", "devicetype"),
    ("dcim", "device"),
    ("dcim", "virtualchassis"),
    ("dcim", "interface"),
    ("dcim", "macaddress"),
    ("ipam", "vlan"),
    ("ipam", "vrf"),
    ("ipam", "prefix"),
    ("ipam", "ipaddress"),
    ("dcim", "inventoryitem"),
]


class IPFabricTransformMapSourceModelChoices(ChoiceSet):
    SITE = "site"
    INVENTORY = "inventory"
    DEVICE = "device"
    VIRTUALCHASSIS = "virtualchassis"
    INTERFACE = "interface"
    VLAN = "vlan"
    VRF = "vrf"
    PREFIX = "prefix"
    IPADDRESS = "ipaddress"
    # PARTNUMBERS = "part_number"

    CHOICES = (
        (SITE, "Site", "cyan"),
        (INVENTORY, "Inventory", "gray"),
        (DEVICE, "Device", "gray"),
        (VIRTUALCHASSIS, "Virtual Chassis", "grey"),
        (INTERFACE, "Interface", "gray"),
        (VLAN, "VLAN", "gray"),
        (VRF, "VRF", "gray"),
        (PREFIX, "Prefix", "gray"),
        (IPADDRESS, "IP Address", "gray"),
        # (PARTNUMBERS, "Part Number", "gray"),
    )


class IPFabricSnapshotStatusModelChoices(ChoiceSet):
    key = "IPFabricSnapshot.status"

    STATUS_LOADED = "loaded"
    STATUS_UNLOADED = "unloaded"

    CHOICES = [
        (STATUS_LOADED, _("Loaded"), "green"),
        (STATUS_UNLOADED, _("Unloaded"), "red"),
    ]


class IPFabricSourceTypeChoices(ChoiceSet):
    LOCAL = "local"
    REMOTE = "remote"

    CHOICES = (
        (LOCAL, "Local", "cyan"),
        (REMOTE, "Remote", "gray"),
    )


class IPFabricRawDataTypeChoices(ChoiceSet):
    DEVICE = "device"
    VLAN = "vlan"
    VRF = "vrf"
    VIRTUALCHASSIS = "virtualchassis"
    PREFIX = "prefix"
    INTERFACE = "interface"
    IPADDRESS = "ipaddress"
    INVENTORYITEM = "inventoryitem"
    SITE = "site"

    CHOICES = (
        (DEVICE, "Local", "cyan"),
        (VLAN, "VLAN", "gray"),
        (VIRTUALCHASSIS, "Virtual Chassis", "gray"),
        (PREFIX, "Prefix", "gray"),
        (INTERFACE, "Interface", "gray"),
        (INVENTORYITEM, "Inventory Item", "gray"),
        (IPADDRESS, "IP Address", "gray"),
        (SITE, "Site", "gray"),
    )
