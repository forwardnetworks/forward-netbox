import json

from django.core.management.base import BaseCommand
from django.core.management.base import CommandError

from ...utilities.ownership import ownership_integrity_summary


class Command(BaseCommand):
    help = "Audit durable Forward sync ownership claims (read-only)."

    def add_arguments(self, parser):
        parser.add_argument(
            "--fail-on-inconsistent",
            action="store_true",
            help="Exit non-zero when claims and NetBox assignments disagree.",
        )
        parser.add_argument(
            "--require-no-open-branches",
            action="store_true",
            help="Also exit non-zero when any nonterminal Branching branch exists.",
        )

    def handle(self, *args, **options):
        report = ownership_integrity_summary()
        inconsistent = sum(
            report[key]
            for key in (
                "missing_tag_assignments",
                "unclaimed_managed_assignments",
                "pending_reconciliations",
                "missing_required_reconciliations",
                "pending_managed_tag_domains",
                "pending_virtual_parent_domain",
                "parent_conflicts",
                "parent_mismatches",
                "parent_claims_missing_virtual_context",
                "virtual_context_parent_mismatches",
                "unclaimed_parent_assignments",
                "orphan_managed_virtual_contexts",
                "provenance_sync_mismatches",
                "pending_migration_branches",
            )
        )
        open_branch_blockers = (
            report["open_branches"] if options["require_no_open_branches"] else 0
        )
        report["consistent"] = inconsistent == 0
        report["release_ready"] = inconsistent + report["open_branches"] == 0
        self.stdout.write(json.dumps(report, indent=2, sort_keys=True))
        if options["fail_on_inconsistent"] and inconsistent + open_branch_blockers:
            raise CommandError(
                f"{inconsistent} Forward ownership inconsistencies and "
                f"{open_branch_blockers} open branches found."
            )
