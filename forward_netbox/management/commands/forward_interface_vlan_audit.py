import json

from django.core.management.base import BaseCommand

from forward_netbox.utilities.interface_vlan_audit import audit_interface_untagged_vlans


class Command(BaseCommand):
    help = (
        "Read-only audit of interfaces NetBox will refuse on their untagged VLAN. "
        "Reports each interface whose untagged VLAN belongs to a different site "
        "than its device (and is not global), and each interface carrying an "
        "untagged VLAN with no 802.1Q mode. A sync records which rule rejected a "
        "row but not which row, because persisted diagnostics carry no customer "
        "data — this names the interfaces on your console instead. Never writes."
    )

    def add_arguments(self, parser):
        parser.add_argument(
            "--limit",
            type=int,
            default=25,
            help="Rows listed per rule. Counts are always exact; 0 lists none.",
        )

    def handle(self, *args, **options):
        payload = audit_interface_untagged_vlans(
            sample_limit=int(options.get("limit") or 0)
        )
        self.stdout.write(json.dumps(payload, indent=2, default=str))
