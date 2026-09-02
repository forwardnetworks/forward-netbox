import json

from django.core.management.base import BaseCommand

from forward_netbox.utilities.changediff_audit import audit_change_diffs


class Command(BaseCommand):
    help = (
        "Read-only audit of netbox_branching ChangeDiff rows whose serialized "
        "payload belongs to a different model than their object_type - the "
        "2.8.6 mixed-model corruption. Its plan expected no persistent damage in "
        "main; this measures it. Reports key NAMES only, never values."
    )

    def add_arguments(self, parser):
        parser.add_argument(
            "--limit",
            type=int,
            default=25,
            help="Flagged rows listed. Counts are always exact; 0 lists none.",
        )

    def handle(self, *args, **options):
        payload = audit_change_diffs(sample_limit=int(options.get("limit") or 0))
        self.stdout.write(json.dumps(payload, indent=2, default=str))
