from django.utils.translation import gettext_lazy as _
from utilities.choices import ChoiceSet

class ForwardSnapshotStatusModelChoices(ChoiceSet):
    key = "ForwardSnapshot.status"

    STATUS_LOADED = "loaded"
    STATUS_UNLOADED = "unloaded"

    CHOICES = [
        (STATUS_LOADED, _("Loaded"), "green"),
        (STATUS_UNLOADED, _("Unloaded"), "red"),
    ]


class ForwardSourceTypeChoices(ChoiceSet):
    LOCAL = "local"
    REMOTE = "remote"

    CHOICES = (
        (LOCAL, "Local", "cyan"),
        (REMOTE, "Remote", "gray"),
    )


class ForwardRawDataTypeChoices(ChoiceSet):
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
