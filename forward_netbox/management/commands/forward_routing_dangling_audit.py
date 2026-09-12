import json

from django.core.management.base import BaseCommand
from django.core.management.base import CommandError

from forward_netbox.utilities.routing_dangling_audit import audit_routing_dangling_rows

# The same report is a page on the sync (Audits > Dangling routing rows).


class Command(BaseCommand):
    help = (
        "Report netbox_routing rows whose device references dangle "
        "(read-only; the post-prune sweep only covers plugin-pruned devices)."
    )

    def add_arguments(self, parser):
        parser.add_argument(
            "--fail-on-dangling",
            action="store_true",
            help="Exit non-zero when any dangling row is found (for CI).",
        )

    def handle(self, *args, **options):
        report = audit_routing_dangling_rows()
        self.stdout.write(json.dumps(report, indent=2))
        if "skipped" in report:
            return
        total = sum(report["dangling"].values())
        if options["fail_on_dangling"] and total:
            raise CommandError(f"{total} dangling netbox_routing rows found.")
