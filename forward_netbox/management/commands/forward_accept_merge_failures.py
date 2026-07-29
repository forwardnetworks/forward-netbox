"""Complete a merge that a deterministic row failure has permanently stalled.

When any row fails, the merge returns the branch to READY and never attests, so
`merge_applied_at` stays null — which also blocks `resume_post_merge_bookkeeping`,
because that requires durable merge-applied evidence. The only exit is a retry
with zero failures.

When the failure is deterministic — a unique-constraint clash on a row the source
keeps re-sending, or a validation error on data the operator cannot change — that
retry never arrives, and the ingestion never promotes a baseline. Without a
baseline there is no drift measurement and no diff-based sync, so a handful of
rows can hold an otherwise healthy instance in a permanent partial state.

This is deliberately a command and not a button: accepting failures is an
operator decision that should be typed, attributed, and auditable. It reports
exactly what will be accepted and requires `--confirm` before doing anything.
"""

from django.contrib.auth import get_user_model
from django.core.management.base import BaseCommand
from django.core.management.base import CommandError

from forward_netbox.choices import ForwardIngestionPhaseChoices
from forward_netbox.models import ForwardIngestion
from forward_netbox.models import ForwardIngestionIssue


class Command(BaseCommand):
    help = (
        "Complete a stalled merge by accepting its reported row failures. The "
        "failures remain recorded and the acceptance is written to durable "
        "evidence; the baseline is promoted over known exceptions."
    )

    def add_arguments(self, parser):
        parser.add_argument(
            "--ingestion",
            type=int,
            required=True,
            help="ForwardIngestion primary key",
        )
        parser.add_argument(
            "--confirm",
            action="store_true",
            help="Actually accept. Without it this only reports.",
        )
        parser.add_argument(
            "--user",
            default="",
            help="Username to attribute the acceptance to (defaults to the sync owner).",
        )

    def handle(self, *args, **options):
        try:
            ingestion = ForwardIngestion.objects.select_related("sync").get(
                pk=options["ingestion"]
            )
        except ForwardIngestion.DoesNotExist:
            raise CommandError(f"No ingestion with pk {options['ingestion']}.")

        failed = int(ingestion.failed_change_count or 0)
        issues = list(
            ForwardIngestionIssue.objects.filter(
                ingestion=ingestion,
                phase=ForwardIngestionPhaseChoices.MERGE,
            ).order_by("pk")
        )

        self.stdout.write(f"Ingestion {ingestion.pk} ({ingestion.sync.name})")
        self.stdout.write(f"  applied            : {ingestion.applied_change_count}")
        self.stdout.write(f"  failed             : {failed}")
        self.stdout.write(f"  baseline_ready     : {ingestion.baseline_ready}")
        self.stdout.write(f"  merge_applied_at   : {ingestion.merge_applied_at}")
        for issue in issues:
            self.stdout.write(
                f"    - {issue.model or '-'}: {issue.exception} "
                f"{issue.raw_data or ''}"
            )

        if ingestion.baseline_ready:
            self.stdout.write(
                self.style.SUCCESS("Baseline is already promoted; nothing to do.")
            )
            return
        if not failed:
            self.stdout.write(
                self.style.WARNING(
                    "No failed changes recorded. If the merge is stalled for "
                    "another reason this command is not the remedy."
                )
            )
            return
        if ingestion.branch is None:
            raise CommandError(
                "Ingestion has no staged branch; there is nothing to merge."
            )

        if not options["confirm"]:
            self.stdout.write(
                self.style.WARNING(
                    f"\nWould accept {failed} failure(s) and promote the baseline. "
                    "Re-run with --confirm to proceed."
                )
            )
            return

        user = None
        username = str(options["user"] or "").strip()
        if username:
            try:
                user = get_user_model().objects.get(username=username)
            except get_user_model().DoesNotExist:
                raise CommandError(f"No user named {username!r}.")

        from forward_netbox.utilities.ingestion_merge import sync_merge_ingestion

        sync_merge_ingestion(
            ingestion,
            accept_reported_failures=True,
            claimed_job=None,
            user=user,
        )
        ingestion.refresh_from_db()
        self.stdout.write(
            self.style.SUCCESS(
                f"Accepted {failed} failure(s). baseline_ready="
                f"{ingestion.baseline_ready}. The failures remain recorded as "
                "ingestion issues and the acceptance is in the ingestion's "
                "durable evidence."
            )
        )
