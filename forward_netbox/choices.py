from django.utils.translation import gettext_lazy as _
from utilities.choices import ChoiceSet


class ForwardSnapshotStatusModelChoices(ChoiceSet):
    key = "ForwardSnapshot.status"

    STATUS_PROCESSED = "processed"
    STATUS_UNPROCESSED = "unprocessed"

    CHOICES = [
        (STATUS_PROCESSED, _("Processed"), "green"),
        (STATUS_UNPROCESSED, _("Unprocessed"), "red"),
    ]


class ForwardSyncTypeChoices(ChoiceSet):
    ALL = "all"
    DCIM = "dcim"
    IPAM = "ipam"

    CHOICES = (
        (ALL, _("All"), "gray"),
        (DCIM, _("DCIM"), "blue"),
        (IPAM, _("IPAM"), "blue"),
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
