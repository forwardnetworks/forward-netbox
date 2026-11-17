# A merged run's branch is gone, so its change counts come from NetBox's change log.
#
# Before this, the explainer said "branch_missing" for every completed run, and a
# support bundle could not say which models a run had changed. The change log keeps
# every change under the ingestion's request id.
import uuid

from core.choices import ObjectChangeActionChoices
from core.models import ObjectChange
from core.models import ObjectType
from django.test import TestCase
from django.utils import timezone
from netbox_branching.models import Branch
from netbox_branching.models import ChangeDiff

from forward_netbox.models import ForwardIngestion
from forward_netbox.models import ForwardSource
from forward_netbox.models import ForwardSync
from forward_netbox.utilities.change_explainability import change_explainability_summary


class ChangeLogCountsTest(TestCase):
    def setUp(self):
        source = ForwardSource.objects.create(
            name="cl-source",
            type="saas",
            url="https://forward.example.com",
            status="ready",
            parameters={"network_id": "net-1"},
        )
        sync = ForwardSync.objects.create(name="cl-sync", source=source, parameters={})
        self.request_id = uuid.uuid4()
        self.ingestion = ForwardIngestion.objects.create(
            sync=sync, change_request_id=self.request_id
        )

    def _log(self, app_label, model, action, n, *, request_id=None):
        object_type = ObjectType.objects.get(app_label=app_label, model=model)
        for index in range(n):
            ObjectChange.objects.create(
                request_id=request_id or self.request_id,
                user_name="forward",
                changed_object_type=object_type,
                changed_object_id=index + 1,
                object_repr=f"{model}-{index}",
                action=action,
                prechange_data=None,
                postchange_data={},
            )

    def test_a_run_with_no_branch_is_counted_from_the_change_log(self):
        self._log("dcim", "interface", "create", 5)
        self._log("dcim", "interface", "update", 2)
        self._log("ipam", "vlan", "create", 3)
        self._log("dcim", "site", "delete", 1)

        summary = change_explainability_summary(self.ingestion)

        self.assertTrue(summary["available"])
        self.assertEqual(summary["source"], "core.objectchange")
        self.assertEqual(summary["total_change_count"], 11)
        self.assertEqual(
            summary["action_counts"], {"create": 8, "delete": 1, "update": 2}
        )
        self.assertEqual(
            summary["model_counts"],
            {"dcim.interface": 7, "dcim.site": 1, "ipam.vlan": 3},
        )
        self.assertEqual(
            summary["model_action_counts"]["dcim.interface"],
            {"create": 5, "update": 2},
        )
        self.assertFalse(summary["truncated"])
        self.assertTrue(summary["counts_cover_all_changes"])
        self.assertFalse(summary["field_detail_available"])

    def test_another_runs_changes_are_not_counted(self):
        self._log("dcim", "interface", "create", 4)
        self._log("dcim", "interface", "create", 9, request_id=uuid.uuid4())

        summary = change_explainability_summary(self.ingestion)

        self.assertEqual(summary["total_change_count"], 4)

    def test_an_empty_change_log_is_unavailable_never_zero(self):
        self.ingestion.merge_applied_at = timezone.now()
        self.ingestion.save()

        summary = change_explainability_summary(self.ingestion)

        self.assertFalse(summary["available"])
        self.assertEqual(summary["reason"], "no_change_log_for_run")
        self.assertNotIn("total_change_count", summary)

    def test_an_ingestion_without_a_request_id_stays_branch_missing(self):
        self.ingestion.change_request_id = None
        self.ingestion.save()

        summary = change_explainability_summary(self.ingestion)

        self.assertFalse(summary["available"])
        self.assertEqual(summary["reason"], "branch_missing")

    def test_a_merged_run_with_an_empty_log_gets_the_retention_reason(self):
        self.ingestion.merge_applied_at = timezone.now()
        self.ingestion.save()

        summary = change_explainability_summary(self.ingestion)

        self.assertEqual(summary["reason"], "no_change_log_for_run")

    def test_a_run_that_never_merged_is_not_blamed_on_retention(self):
        self.assertIsNone(self.ingestion.merge_applied_at)

        summary = change_explainability_summary(self.ingestion)

        self.assertEqual(summary["reason"], "branch_missing")

    def test_a_fast_baseline_is_not_blamed_on_retention(self):
        self.ingestion.merge_applied_at = timezone.now()
        self.ingestion.snapshot_info = {"fast_baseline_load": {"rows": 1}}
        self.ingestion.save()

        summary = change_explainability_summary(self.ingestion)

        self.assertEqual(summary["reason"], "fast_baseline_no_change_log")


class BranchPathStillWinsTest(TestCase):
    def setUp(self):
        source = ForwardSource.objects.create(
            name="bw-source",
            type="saas",
            url="https://forward.example.com",
            status="ready",
            parameters={"network_id": "net-1"},
        )
        sync = ForwardSync.objects.create(name="bw-sync", source=source, parameters={})
        self.request_id = uuid.uuid4()
        self.branch = Branch.objects.create(name="bw-branch", schema_id="bw_branch")
        self.ingestion = ForwardIngestion.objects.create(
            sync=sync, branch=self.branch, change_request_id=self.request_id
        )
        ChangeDiff.objects.create(
            branch=self.branch,
            object_type=ObjectType.objects.get(app_label="dcim", model="site"),
            object_id=1,
            object_repr="site-1",
            action=ObjectChangeActionChoices.ACTION_CREATE,
            original=None,
            modified={"name": "x"},
            current=None,
        )
        ObjectChange.objects.create(
            request_id=self.request_id,
            user_name="forward",
            changed_object_type=ObjectType.objects.get(app_label="dcim", model="site"),
            changed_object_id=1,
            object_repr="site-1",
            action="create",
            prechange_data=None,
            postchange_data={},
        )

    def test_a_run_that_still_has_its_branch_is_explained_from_the_branch(self):
        summary = change_explainability_summary(self.ingestion)

        self.assertEqual(summary["source"], "netbox_branching.changediff")

    def test_the_change_log_result_has_every_key_the_branch_result_has(self):
        branch_result = change_explainability_summary(self.ingestion)
        self.ingestion.branch = None
        self.ingestion.save()

        log_result = change_explainability_summary(self.ingestion)

        self.assertTrue(log_result["available"])
        self.assertEqual(set(branch_result) - set(log_result), set())
