# The dependency-skip rollup said "their NetBox parent is not synced yet" and
# recommended enabling the parent sync for EVERY skip in a model - including a
# protected-delete skip, where a surviving child is refusing the prune and the
# remedy is the opposite. The per-row path had learned the difference
# (`dependency_phrase`); the rollup had not. Carried as open through three
# release plans.
#
# Two smaller things ride along: the tenth per-row issue says the cap is here,
# so ten rows cannot masquerade as the count; and the catalogued skip reason
# now reaches the database, derived from what twenty-four raisers already say.
from types import SimpleNamespace

from django.test import TestCase

from forward_netbox.exceptions import ForwardDependencySkipError
from forward_netbox.models import ForwardIngestion
from forward_netbox.models import ForwardIngestionIssue
from forward_netbox.models import ForwardSource
from forward_netbox.models import ForwardSync
from forward_netbox.utilities.sync_reporting import dependency_skip_reason
from forward_netbox.utilities.sync_reporting import emit_dependency_skip_issue_summary
from forward_netbox.utilities.sync_reporting import record_issue

MODEL = "netbox_dlm.softwareversion"


class _Logger:
    def __init__(self):
        self.lines = []

    def log_info(self, message, obj=None):
        self.lines.append(("info", message))

    def log_warning(self, message, obj=None):
        self.lines.append(("warning", message))

    def log_failure(self, message, obj=None):
        self.lines.append(("failure", message))


class SkipRollupDirectionTest(TestCase):
    def setUp(self):
        source = ForwardSource.objects.create(
            name="rollup-src", type="saas", url="https://fwd.app", status="ready"
        )
        sync = ForwardSync.objects.create(name="rollup-sync", source=source)
        self.ingestion = ForwardIngestion.objects.create(sync=sync, snapshot_id="s")
        self.runner = SimpleNamespace(
            ingestion=self.ingestion,
            sync=sync,
            logger=_Logger(),
            DEPENDENCY_SKIP_ISSUE_DETAIL_LIMIT=3,
            _dependency_skip_issue_counts={},
            _dependency_skip_issue_samples={},
            _recorded_issue_ids=set(),
        )

    def _skip(self, *, protecting, dependency="netbox_dlm.inventoryitemsoftware", n=1):
        for index in range(n):
            record_issue(
                self.runner,
                MODEL,
                "skipped",
                {"platform_slug": "ios", "version": f"1.{index}"},
                exception=ForwardDependencySkipError(
                    "skipped",
                    model_string=MODEL,
                    dependency=dependency,
                    dependency_is_protecting=protecting,
                ),
                context={"version": f"1.{index}"},
            )

    def _summary(self):
        emit_dependency_skip_issue_summary(self.runner, MODEL)
        return ForwardIngestionIssue.objects.filter(
            ingestion=self.ingestion, coalesce_fields__dependency_skip_summary=True
        ).first()

    def test_a_protected_delete_rollup_names_the_child_and_the_right_remedy(self):
        self._skip(protecting=True, n=5)

        summary = self._summary()

        self.assertIsNotNone(summary)
        self.assertIn("surviving NetBox child", summary.message)
        self.assertIn(
            "still referenced by netbox_dlm.inventoryitemsoftware", summary.message
        )
        self.assertIn("refused deletes, not missing parents", summary.message)
        self.assertNotIn("not synced yet", summary.message)
        self.assertNotIn("Enable the parent sync", summary.message)
        self.assertEqual(summary.coalesce_fields["protected_delete_count"], 5)
        self.assertEqual(summary.coalesce_fields["missing_parent_count"], 0)

    def test_a_missing_parent_rollup_keeps_the_parent_remedy(self):
        self._skip(protecting=False, dependency="dcim.device", n=4)

        summary = self._summary()

        self.assertIn("not synced yet", summary.message)
        self.assertIn("waiting on dcim.device", summary.message)
        self.assertIn("Enable the parent sync", summary.message)
        self.assertNotIn("surviving", summary.message)

    def test_a_mixed_model_gets_one_sentence_per_direction(self):
        self._skip(protecting=False, dependency="dcim.device", n=2)
        self._skip(protecting=True, n=3)

        summary = self._summary()

        self.assertIn("2 skipped because a NetBox parent", summary.message)
        self.assertIn("3 skipped because a surviving NetBox child", summary.message)
        self.assertIn("5 netbox_dlm.softwareversion row(s) skipped", summary.message)
        self.assertIn("(2 beyond the first 3 shown individually)", summary.message)

    def test_exactly_the_cap_emits_no_rollup_and_is_the_true_count(self):
        # At the cap nothing is suppressed, so ten IS the count. What changes
        # is that the last per-row issue says so.
        self._skip(protecting=True, n=3)

        self.assertIsNone(self._summary())
        rows = list(
            ForwardIngestionIssue.objects.filter(ingestion=self.ingestion).order_by(
                "pk"
            )
        )
        self.assertEqual(len(rows), 3)
        self.assertIn("rolled up into one summary issue", rows[-1].message)
        self.assertNotIn("rolled up", rows[0].message)

    def test_the_catalogued_reason_reaches_the_database(self):
        self._skip(protecting=True, n=1)
        self._skip(protecting=False, dependency="dcim.interface", n=1)

        rows = list(
            ForwardIngestionIssue.objects.filter(ingestion=self.ingestion).order_by(
                "pk"
            )
        )
        self.assertEqual(
            rows[0].coalesce_fields["skip_reason"],
            "still-referenced-by-inventoryitemsoftware",
        )
        self.assertEqual(rows[0].coalesce_fields["skip_direction"], "protecting")
        self.assertEqual(rows[1].coalesce_fields["skip_reason"], "missing-interface")
        self.assertEqual(rows[1].coalesce_fields["skip_direction"], "missing")

    def test_a_raiser_that_names_nothing_is_recorded_honestly(self):
        self.assertEqual(
            dependency_skip_reason(ForwardDependencySkipError("x")),
            "dependency-unnamed",
        )
