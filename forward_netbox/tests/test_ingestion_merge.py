from contextlib import nullcontext
from unittest.mock import MagicMock
from unittest.mock import patch
from uuid import uuid4

from core.choices import JobStatusChoices
from core.choices import ObjectChangeActionChoices
from core.exceptions import SyncError
from core.models import Job
from core.models import ObjectChange
from django.contrib.auth import get_user_model
from django.contrib.contenttypes.models import ContentType
from django.test import TestCase
from django.utils import timezone
from netbox_branching.choices import BranchStatusChoices
from netbox_branching.models import AppliedChange
from netbox_branching.models import Branch
from netbox_branching.models import BranchEvent
from netbox_branching.models import ChangeDiff

from forward_netbox.choices import ForwardCatchupStatusChoices
from forward_netbox.choices import ForwardIngestionPhaseChoices
from forward_netbox.choices import ForwardSyncStatusChoices
from forward_netbox.exceptions import ForwardPartialMergeError
from forward_netbox.models import ForwardIngestion
from forward_netbox.models import ForwardIngestionIssue
from forward_netbox.models import ForwardOwnershipReconciliation
from forward_netbox.models import ForwardSource
from forward_netbox.models import ForwardSync
from forward_netbox.models import ForwardWorkloadState
from forward_netbox.utilities.forward_api import LATEST_PROCESSED_SNAPSHOT
from forward_netbox.utilities.ingestion_issues import has_blocking_issues
from forward_netbox.utilities.ingestion_merge import cleanup_merged_branch
from forward_netbox.utilities.ingestion_merge import enqueue_merge_job
from forward_netbox.utilities.ingestion_merge import reconcile_ingestion_catchup
from forward_netbox.utilities.ingestion_merge import record_change_totals
from forward_netbox.utilities.ingestion_merge import resume_post_merge_bookkeeping
from forward_netbox.utilities.ingestion_merge import sync_merge_ingestion
from forward_netbox.utilities.merge import merge_branch


class ForwardIngestionMergeHelperTest(TestCase):
    def setUp(self):
        self.user = get_user_model().objects.create_user(
            username="ingestion-merge-owner"
        )
        self.source = ForwardSource.objects.create(
            name="source-ingestion-merge",
            type="saas",
            url="https://fwd.app",
            parameters={
                "username": "user@example.com",
                "password": "secret",
                "verify": True,
                "timeout": 1200,
                "network_id": "test-network",
            },
        )
        self.sync = ForwardSync.objects.create(
            name="sync-ingestion-merge",
            source=self.source,
            user=self.user,
            auto_merge=False,
            parameters={
                "snapshot_id": LATEST_PROCESSED_SNAPSHOT,
                "dcim.device": True,
            },
        )

    def _complete_ownership(self, ingestion):
        ForwardOwnershipReconciliation.objects.filter(
            sync=ingestion.sync,
            ingestion=ingestion,
        ).update(
            status=ForwardOwnershipReconciliation.Status.COMPLETED,
            completed_at=timezone.now(),
        )

    @staticmethod
    def _bulk_merge_result(applied, failed, action_counts):
        def result(*_args, result_metadata=None, **_kwargs):
            result_metadata.update(
                logical_total=applied + failed,
                logical_action_counts=action_counts,
            )
            return applied, failed, set()

        return result

    @staticmethod
    def _mock_changes(logical_total, model_rows=()):
        changes = MagicMock()
        changes.order_by.return_value = changes
        changes.exists.return_value = logical_total > 0
        changes.values.return_value.distinct.return_value.count.return_value = (
            logical_total
        )
        changes.values_list.return_value.distinct.return_value = list(model_rows)
        return changes

    def test_merge_attestation_uses_invoking_job_user(self):
        invoking_user = get_user_model().objects.create_user(
            username="ingestion-merge-invoker"
        )
        branch = Branch.objects.create(
            name=f"attributed-merge-{uuid4().hex[:12]}",
            schema_id=f"attributed_merge_{uuid4().hex[:12]}",
            status=BranchStatusChoices.READY,
        )
        ingestion = ForwardIngestion.objects.create(sync=self.sync, branch=branch)
        changes = self._mock_changes(0)

        with patch.object(Branch, "get_unmerged_changes", return_value=changes):
            merge_branch(ingestion, user=invoking_user)

        branch.refresh_from_db()
        self.assertEqual(branch.merged_by, invoking_user)
        self.assertTrue(
            BranchEvent.objects.filter(
                branch=branch,
                user=invoking_user,
                type="merged",
            ).exists()
        )

    def test_merge_statistics_use_logical_object_count_per_model(self):
        branch = Branch.objects.create(
            name=f"merge-model-counts-{uuid4().hex[:12]}",
            schema_id=f"merge_model_counts_{uuid4().hex[:12]}",
            status=BranchStatusChoices.READY,
        )
        ingestion = ForwardIngestion.objects.create(sync=self.sync, branch=branch)
        changes = MagicMock()
        changes.order_by.return_value = changes
        changes.exists.return_value = True
        model_groups = MagicMock()
        model_groups.annotate.return_value = [
            {
                "changed_object_type__app_label": "dcim",
                "changed_object_type__model": "device",
                "total": 1000,
            },
            {
                "changed_object_type__app_label": "dcim",
                "changed_object_type__model": "site",
                "total": 2,
            },
        ]
        logical_objects = MagicMock()
        logical_objects.distinct.return_value.count.return_value = 1002

        def values(*fields):
            if "changed_object_type__app_label" in fields:
                return model_groups
            return logical_objects

        changes.values.side_effect = values
        sync_logger = MagicMock()

        with (
            patch.object(Branch, "get_unmerged_changes", return_value=changes),
            patch(
                "forward_netbox.utilities.merge.bulk_merge_changes",
                side_effect=self._bulk_merge_result(
                    1002,
                    0,
                    {"create": 1002},
                ),
            ),
        ):
            merge_branch(ingestion, sync_logger=sync_logger)

        sync_logger.init_statistics.assert_any_call("dcim.device", total=1000)
        sync_logger.init_statistics.assert_any_call("dcim.site", total=2)
        self.assertEqual(sync_logger.init_statistics.call_count, 2)

    @staticmethod
    def _attest_mock_merge(*, ingestion, **_kwargs):
        merged_at = timezone.now()
        ForwardIngestion.objects.filter(pk=ingestion.pk).update(
            merge_applied_at=merged_at
        )
        ingestion.merge_applied_at = merged_at

    def test_sync_merge_ingestion_completes_single_branch_baseline(self):
        ingestion = ForwardIngestion.objects.create(
            sync=self.sync,
            snapshot_selector=LATEST_PROCESSED_SNAPSHOT,
            snapshot_id="snapshot-before",
        )
        with (
            patch(
                "forward_netbox.utilities.merge.merge_branch",
                side_effect=self._attest_mock_merge,
            ),
            patch(
                "forward_netbox.utilities.ingestion_merge.suppress_branch_merge_side_effect_signals",
                return_value=nullcontext(),
            ),
        ):
            sync_merge_ingestion(ingestion)

        self.sync.refresh_from_db()
        ingestion.refresh_from_db()

        self.assertEqual(self.sync.status, ForwardSyncStatusChoices.COMPLETED)
        self.assertTrue(ingestion.baseline_ready)

    def test_successful_merge_promotes_only_its_pending_workload_state(self):
        previous = ForwardIngestion.objects.create(
            sync=self.sync,
            snapshot_id="snapshot-previous-state",
            baseline_ready=True,
        )
        old_state = ForwardWorkloadState.objects.create(
            sync=self.sync,
            ingestion=previous,
            model_string="dcim.site",
            parameter_hash="a" * 64,
            identity_contract_hash="b" * 64,
            payload=b"old",
            payload_checksum="c" * 64,
            row_count=1,
            is_current=True,
        )
        ingestion = ForwardIngestion.objects.create(
            sync=self.sync,
            snapshot_id="snapshot-new-state",
        )
        pending = ForwardWorkloadState.objects.create(
            sync=self.sync,
            ingestion=ingestion,
            model_string="dcim.site",
            parameter_hash="d" * 64,
            identity_contract_hash="e" * 64,
            payload=b"new",
            payload_checksum="f" * 64,
            row_count=2,
        )

        with (
            patch(
                "forward_netbox.utilities.merge.merge_branch",
                side_effect=self._attest_mock_merge,
            ),
            patch(
                "forward_netbox.utilities.ingestion_merge.suppress_branch_merge_side_effect_signals",
                return_value=nullcontext(),
            ),
        ):
            sync_merge_ingestion(ingestion)

        pending.refresh_from_db()
        self.assertTrue(pending.is_current)
        self.assertFalse(ForwardWorkloadState.objects.filter(pk=old_state.pk).exists())

    def test_cleanup_merged_branch_refreshes_stale_status_before_delete(self):
        branch = Branch.objects.create(
            name=f"merged-branch-{uuid4().hex[:12]}",
            schema_id=f"merged_branch_{uuid4().hex[:12]}",
            status=BranchStatusChoices.MERGING,
        )
        ingestion = ForwardIngestion.objects.create(
            sync=self.sync,
            snapshot_selector=LATEST_PROCESSED_SNAPSHOT,
            snapshot_id="snapshot-merged-branch",
            branch=branch,
        )
        Branch.objects.filter(pk=branch.pk).update(status=BranchStatusChoices.MERGED)
        branch.status = BranchStatusChoices.MERGING

        cleanup_merged_branch(ingestion)

        self.assertFalse(Branch.objects.filter(pk=branch.pk).exists())
        ingestion.refresh_from_db()
        self.assertIsNone(ingestion.branch)

    def test_cleanup_merged_branch_set_deletes_branch_evidence_only(self):
        branch = Branch.objects.create(
            name=f"merged-evidence-{uuid4().hex[:12]}",
            schema_id=f"merged_evidence_{uuid4().hex[:12]}",
            status=BranchStatusChoices.MERGED,
        )
        ingestion = ForwardIngestion.objects.create(
            sync=self.sync,
            snapshot_selector=LATEST_PROCESSED_SNAPSHOT,
            snapshot_id="snapshot-merged-evidence",
            branch=branch,
        )
        source_type = ContentType.objects.get_for_model(ForwardSource)
        change = ObjectChange.objects.create(
            user=self.user,
            user_name=self.user.username,
            request_id=uuid4(),
            action=ObjectChangeActionChoices.ACTION_UPDATE,
            changed_object_type=source_type,
            changed_object_id=self.source.pk,
            object_repr=str(self.source),
            message="merged branch audit",
            prechange_data={"name": self.source.name},
            postchange_data={"name": self.source.name},
        )
        AppliedChange.objects.create(branch=branch, change=change)
        ChangeDiff.objects.create(
            branch=branch,
            object_type=source_type,
            object_id=self.source.pk,
            object_repr=str(self.source),
            action=ObjectChangeActionChoices.ACTION_UPDATE,
            original={"name": self.source.name},
            modified={"name": self.source.name},
            current={"name": self.source.name},
        )
        events = []
        original_raw_delete = AppliedChange.objects.all()._raw_delete.__func__
        original_branch_delete = Branch.delete

        def record_raw_delete(queryset, *, using):
            events.append(queryset.model)
            return original_raw_delete(queryset, using=using)

        def record_branch_delete(instance, *args, **kwargs):
            events.append(Branch)
            return original_branch_delete(instance, *args, **kwargs)

        with (
            patch(
                "django.db.models.query.QuerySet._raw_delete",
                autospec=True,
                side_effect=record_raw_delete,
            ),
            patch.object(
                Branch,
                "delete",
                autospec=True,
                side_effect=record_branch_delete,
            ),
        ):
            cleanup_merged_branch(ingestion)

        self.assertEqual(events[:3], [AppliedChange, ChangeDiff, Branch])
        self.assertFalse(Branch.objects.filter(pk=branch.pk).exists())
        self.assertFalse(AppliedChange.objects.filter(branch_id=branch.pk).exists())
        self.assertFalse(ChangeDiff.objects.filter(branch_id=branch.pk).exists())
        self.assertTrue(ObjectChange.objects.filter(pk=change.pk).exists())
        ingestion.refresh_from_db()
        self.assertIsNone(ingestion.branch)

    def test_cleanup_merged_branch_rolls_back_all_state_when_delete_fails(self):
        branch = Branch.objects.create(
            name=f"merged-rollback-{uuid4().hex[:12]}",
            schema_id=f"merged_rollback_{uuid4().hex[:12]}",
            status=BranchStatusChoices.MERGED,
        )
        ingestion = ForwardIngestion.objects.create(
            sync=self.sync,
            snapshot_selector=LATEST_PROCESSED_SNAPSHOT,
            snapshot_id="snapshot-merged-rollback",
            branch=branch,
        )
        source_type = ContentType.objects.get_for_model(ForwardSource)
        change = ObjectChange.objects.create(
            user=self.user,
            user_name=self.user.username,
            request_id=uuid4(),
            action=ObjectChangeActionChoices.ACTION_UPDATE,
            changed_object_type=source_type,
            changed_object_id=self.source.pk,
            object_repr=str(self.source),
            message="merged branch rollback audit",
            prechange_data={"name": self.source.name},
            postchange_data={"name": self.source.name},
        )
        applied_change = AppliedChange.objects.create(branch=branch, change=change)
        change_diff = ChangeDiff.objects.create(
            branch=branch,
            object_type=source_type,
            object_id=self.source.pk,
            object_repr=str(self.source),
            action=ObjectChangeActionChoices.ACTION_UPDATE,
            original={"name": self.source.name},
            modified={"name": self.source.name},
            current={"name": self.source.name},
        )

        with (
            patch.object(Branch, "delete", side_effect=RuntimeError("teardown failed")),
            self.assertRaisesMessage(RuntimeError, "teardown failed"),
        ):
            cleanup_merged_branch(ingestion)

        ingestion.refresh_from_db()
        self.assertEqual(ingestion.branch_id, branch.pk)
        self.assertTrue(Branch.objects.filter(pk=branch.pk).exists())
        self.assertTrue(AppliedChange.objects.filter(pk=applied_change.pk).exists())
        self.assertTrue(ChangeDiff.objects.filter(pk=change_diff.pk).exists())
        self.assertTrue(ObjectChange.objects.filter(pk=change.pk).exists())

    def test_cleanup_merged_branch_rejects_nonterminal_branch(self):
        branch = Branch.objects.create(
            name=f"ready-cleanup-{uuid4().hex[:12]}",
            schema_id=f"ready_cleanup_{uuid4().hex[:12]}",
            status=BranchStatusChoices.READY,
        )
        ingestion = ForwardIngestion.objects.create(
            sync=self.sync,
            snapshot_selector=LATEST_PROCESSED_SNAPSHOT,
            snapshot_id="snapshot-ready-cleanup",
            branch=branch,
        )

        with self.assertRaisesMessage(
            SyncError,
            "Merged branch cleanup requires a persisted merged branch state.",
        ):
            cleanup_merged_branch(ingestion)

        ingestion.refresh_from_db()
        self.assertEqual(ingestion.branch_id, branch.pk)
        self.assertTrue(Branch.objects.filter(pk=branch.pk).exists())

    def test_sync_merge_ingestion_deletes_stale_merged_branch(self):
        branch = Branch.objects.create(
            name=f"stale-merged-branch-{uuid4().hex[:12]}",
            schema_id=f"stale_merged_branch_{uuid4().hex[:12]}",
            status=BranchStatusChoices.MERGING,
        )
        ingestion = ForwardIngestion.objects.create(
            sync=self.sync,
            snapshot_selector=LATEST_PROCESSED_SNAPSHOT,
            snapshot_id="snapshot-stale-merged-branch",
            branch=branch,
        )
        Branch.objects.filter(pk=branch.pk).update(status=BranchStatusChoices.MERGED)
        branch.status = BranchStatusChoices.MERGING

        with (
            patch(
                "forward_netbox.utilities.merge.merge_branch",
                side_effect=self._attest_mock_merge,
            ),
            patch(
                "forward_netbox.utilities.ingestion_merge.suppress_branch_merge_side_effect_signals",
                return_value=nullcontext(),
            ),
        ):
            sync_merge_ingestion(ingestion)

        self.assertFalse(Branch.objects.filter(pk=branch.pk).exists())
        ingestion.refresh_from_db()
        self.assertIsNone(ingestion.branch)
        self.sync.refresh_from_db()
        self.assertEqual(self.sync.status, ForwardSyncStatusChoices.COMPLETED)

    def test_partial_merge_stays_ready_without_completing_baseline(self):
        branch = Branch.objects.create(
            name=f"partial-merge-{uuid4().hex[:12]}",
            schema_id=f"partial_merge_{uuid4().hex[:12]}",
            status=BranchStatusChoices.READY,
        )
        ingestion = ForwardIngestion.objects.create(
            sync=self.sync,
            snapshot_selector=LATEST_PROCESSED_SNAPSHOT,
            snapshot_id="snapshot-partial-merge",
            branch=branch,
        )
        pending = ForwardWorkloadState.objects.create(
            sync=self.sync,
            ingestion=ingestion,
            model_string="dcim.site",
            parameter_hash="a" * 64,
            identity_contract_hash="b" * 64,
            payload=b"pending",
            payload_checksum="c" * 64,
            row_count=1,
        )

        with (
            patch(
                "forward_netbox.utilities.merge.merge_branch",
                side_effect=ForwardPartialMergeError(
                    "one row failed", applied=1, failed=1
                ),
            ),
            self.assertRaises(ForwardPartialMergeError),
        ):
            sync_merge_ingestion(ingestion)

        self.sync.refresh_from_db()
        self.source.refresh_from_db()
        ingestion.refresh_from_db()
        self.assertEqual(self.sync.status, ForwardSyncStatusChoices.READY_TO_MERGE)
        self.assertEqual(self.source.status, "ready")
        self.assertFalse(ingestion.baseline_ready)
        pending.refresh_from_db()
        self.assertFalse(pending.is_current)

    def test_merge_branch_partial_attempt_never_emits_merged_event(self):
        branch = Branch.objects.create(
            name=f"strict-partial-{uuid4().hex[:12]}",
            schema_id=f"strict_partial_{uuid4().hex[:12]}",
            status=BranchStatusChoices.READY,
        )
        ingestion = ForwardIngestion.objects.create(
            sync=self.sync,
            snapshot_selector=LATEST_PROCESSED_SNAPSHOT,
            snapshot_id="snapshot-strict-partial",
            branch=branch,
        )
        changes = self._mock_changes(
            2,
            (("dcim", "device"), ("dcim", "site")),
        )

        with (
            patch.object(Branch, "get_unmerged_changes", return_value=changes),
            patch(
                "forward_netbox.utilities.merge.bulk_merge_changes",
                side_effect=self._bulk_merge_result(
                    1,
                    1,
                    {"create": 1, "update": 1},
                ),
            ),
            self.assertRaises(ForwardPartialMergeError),
        ):
            merge_branch(ingestion)

        branch.refresh_from_db()
        ingestion.refresh_from_db()
        self.assertEqual(branch.status, BranchStatusChoices.READY)
        self.assertIsNone(branch.merged_time)
        self.assertFalse(
            BranchEvent.objects.filter(
                branch=branch,
                type="merged",
            ).exists()
        )
        self.assertEqual(ingestion.applied_change_count, 1)
        self.assertEqual(ingestion.failed_change_count, 1)
        self.assertEqual(ingestion.created_change_count, 1)
        self.assertEqual(ingestion.updated_change_count, 1)

        retry_changes = self._mock_changes(
            2,
            (("dcim", "device"), ("dcim", "site")),
        )
        with (
            patch.object(
                Branch,
                "get_unmerged_changes",
                return_value=retry_changes,
            ),
            patch(
                "forward_netbox.utilities.merge.bulk_merge_changes",
                side_effect=self._bulk_merge_result(
                    2,
                    0,
                    {"create": 1, "update": 1},
                ),
            ),
        ):
            merge_branch(ingestion)

        branch.refresh_from_db()
        ingestion.refresh_from_db()
        self.assertEqual(branch.status, BranchStatusChoices.MERGED)
        self.assertEqual(ingestion.applied_change_count, 2)
        self.assertEqual(ingestion.failed_change_count, 0)
        self.assertEqual(ingestion.created_change_count, 1)
        self.assertEqual(ingestion.updated_change_count, 1)
        self.assertEqual(
            BranchEvent.objects.filter(branch=branch, type="merged").count(),
            1,
        )

    def test_successful_partial_retry_archives_only_prior_merge_issues(self):
        branch = Branch.objects.create(
            name=f"resolved-partial-{uuid4().hex[:12]}",
            schema_id=f"resolved_partial_{uuid4().hex[:12]}",
            status=BranchStatusChoices.READY,
        )
        ingestion = ForwardIngestion.objects.create(
            sync=self.sync,
            snapshot_selector=LATEST_PROCESSED_SNAPSHOT,
            snapshot_id="snapshot-resolved-partial",
            branch=branch,
            applied_change_count=1,
            failed_change_count=1,
            created_change_count=2,
        )
        merge_issue = ForwardIngestionIssue.objects.create(
            ingestion=ingestion,
            phase=ForwardIngestionPhaseChoices.MERGE,
            model="dcim.device",
            message="failed merge attempt",
            exception="ValueError",
            raw_data={"traceback": "prior failure"},
        )
        sync_issue = ForwardIngestionIssue.objects.create(
            ingestion=ingestion,
            phase=ForwardIngestionPhaseChoices.SYNC,
            model="dcim.site",
            message="independent sync issue",
            exception="ValueError",
        )
        retry_changes = self._mock_changes(
            2,
            (("dcim", "device"), ("dcim", "site")),
        )

        with (
            patch.object(
                Branch,
                "get_unmerged_changes",
                return_value=retry_changes,
            ),
            patch(
                "forward_netbox.utilities.merge.bulk_merge_changes",
                side_effect=self._bulk_merge_result(2, 0, {"create": 2}),
            ),
        ):
            merge_branch(ingestion)

        ingestion.refresh_from_db()
        self.assertFalse(
            ForwardIngestionIssue.objects.filter(pk=merge_issue.pk).exists()
        )
        self.assertTrue(ForwardIngestionIssue.objects.filter(pk=sync_issue.pk).exists())
        archive = ingestion.snapshot_info["resolved_merge_issues"]
        self.assertEqual(len(archive), 1)
        self.assertEqual(archive[0]["issue_id"], merge_issue.pk)
        self.assertEqual(archive[0]["message"], "failed merge attempt")
        self.assertEqual(archive[0]["raw_data"], {"traceback": "prior failure"})
        self.assertEqual(ingestion.applied_change_count, 2)
        self.assertEqual(ingestion.failed_change_count, 0)
        self.assertTrue(has_blocking_issues(ingestion))

        sync_issue.delete()
        self.assertFalse(has_blocking_issues(ingestion))

    def test_repeated_partial_retry_does_not_inflate_logical_totals(self):
        branch = Branch.objects.create(
            name=f"repeated-partial-{uuid4().hex[:12]}",
            schema_id=f"repeated_partial_{uuid4().hex[:12]}",
            status=BranchStatusChoices.READY,
        )
        ingestion = ForwardIngestion.objects.create(
            sync=self.sync,
            snapshot_selector=LATEST_PROCESSED_SNAPSHOT,
            snapshot_id="snapshot-repeated-partial",
            branch=branch,
            applied_change_count=1,
            failed_change_count=1,
            created_change_count=1,
            updated_change_count=1,
        )
        retry_changes = self._mock_changes(
            2,
            (("dcim", "device"), ("dcim", "site")),
        )

        with (
            patch.object(
                Branch,
                "get_unmerged_changes",
                return_value=retry_changes,
            ),
            patch(
                "forward_netbox.utilities.merge.bulk_merge_changes",
                side_effect=self._bulk_merge_result(
                    1,
                    1,
                    {"create": 1, "update": 1},
                ),
            ),
            self.assertRaises(ForwardPartialMergeError),
        ):
            merge_branch(ingestion)

        ingestion.refresh_from_db()
        self.assertEqual(
            (ingestion.applied_change_count, ingestion.failed_change_count),
            (1, 1),
        )
        self.assertEqual(
            (ingestion.created_change_count, ingestion.updated_change_count),
            (1, 1),
        )

    def test_partial_retry_rejects_truncated_logical_change_set(self):
        branch = Branch.objects.create(
            name=f"truncated-partial-{uuid4().hex[:12]}",
            schema_id=f"truncated_partial_{uuid4().hex[:12]}",
            status=BranchStatusChoices.READY,
        )
        ingestion = ForwardIngestion.objects.create(
            sync=self.sync,
            snapshot_selector=LATEST_PROCESSED_SNAPSHOT,
            snapshot_id="snapshot-truncated-partial",
            branch=branch,
            applied_change_count=4,
            failed_change_count=1,
            created_change_count=5,
        )
        retry_changes = self._mock_changes(1, (("dcim", "device"),))

        with (
            patch.object(
                Branch,
                "get_unmerged_changes",
                return_value=retry_changes,
            ),
            patch("forward_netbox.utilities.merge.bulk_merge_changes") as bulk_merge,
            self.assertRaisesRegex(
                RuntimeError,
                "Refusing to overwrite cumulative merge evidence",
            ),
        ):
            merge_branch(ingestion)

        bulk_merge.assert_not_called()
        ingestion.refresh_from_db()
        branch.refresh_from_db()
        self.assertEqual(
            (ingestion.applied_change_count, ingestion.failed_change_count),
            (4, 1),
        )
        self.assertEqual(branch.status, BranchStatusChoices.READY)

    def test_zero_change_retained_branch_completes_with_merged_lifecycle(self):
        branch = Branch.objects.create(
            name=f"zero-change-{uuid4().hex[:12]}",
            schema_id=f"zero_change_{uuid4().hex[:12]}",
            status=BranchStatusChoices.READY,
        )
        ingestion = ForwardIngestion.objects.create(
            sync=self.sync,
            snapshot_selector=LATEST_PROCESSED_SNAPSHOT,
            snapshot_id="snapshot-zero-change",
            branch=branch,
        )
        changes = self._mock_changes(0)

        with patch.object(
            Branch,
            "get_unmerged_changes",
            return_value=changes,
        ):
            sync_merge_ingestion(ingestion, remove_branch=False)

        branch.refresh_from_db()
        ingestion.refresh_from_db()
        self.sync.refresh_from_db()
        self.assertEqual(branch.status, BranchStatusChoices.MERGED)
        self.assertIsNotNone(branch.merged_time)
        self.assertEqual(
            BranchEvent.objects.filter(branch=branch, type="merged").count(),
            1,
        )
        self.assertEqual(self.sync.status, ForwardSyncStatusChoices.COMPLETED)
        self.assertTrue(ingestion.baseline_ready)
        self.assertEqual(ingestion.applied_change_count, 0)
        self.assertEqual(ingestion.failed_change_count, 0)

    def test_resume_post_merge_bookkeeping_is_idempotent_for_merged_branch(self):
        branch = Branch.objects.create(
            name=f"crash-merged-{uuid4().hex[:12]}",
            schema_id=f"crash_merged_{uuid4().hex[:12]}",
            status=BranchStatusChoices.MERGED,
            merged_time=timezone.now(),
        )
        ingestion = ForwardIngestion.objects.create(
            sync=self.sync,
            snapshot_selector=LATEST_PROCESSED_SNAPSHOT,
            snapshot_id="snapshot-crash-merged",
            branch=branch,
            merge_applied_at=branch.merged_time,
        )
        self.sync.status = ForwardSyncStatusChoices.MERGING
        self.sync.save(update_fields=["status"])

        with patch(
            "forward_netbox.utilities.ingestion_merge.latest_processed_catchup_decision",
            return_value={"should_queue": False},
        ) as mock_catchup:
            self.assertTrue(resume_post_merge_bookkeeping(ingestion))
            ingestion.refresh_from_db()
            self.sync.refresh_from_db()
            first_last_synced = self.sync.last_synced
            self.assertEqual(
                ingestion.catchup_status,
                ForwardCatchupStatusChoices.PENDING,
            )
            mock_catchup.assert_not_called()
            self._complete_ownership(ingestion)
            self.assertTrue(resume_post_merge_bookkeeping(ingestion))

        ingestion.refresh_from_db()
        self.sync.refresh_from_db()
        self.assertTrue(ingestion.baseline_ready)
        self.assertEqual(self.sync.status, ForwardSyncStatusChoices.COMPLETED)
        self.assertEqual(self.sync.last_synced, first_last_synced)
        self.assertIsNone(ingestion.branch)
        self.assertFalse(Branch.objects.filter(pk=branch.pk).exists())
        mock_catchup.assert_called_once()
        self.assertEqual(
            ingestion.catchup_status,
            ForwardCatchupStatusChoices.CURRENT,
        )

    def test_failed_post_merge_catchup_is_durable_and_resumable(self):
        branch = Branch.objects.create(
            name=f"catchup-failure-{uuid4().hex[:12]}",
            schema_id=f"catchup_failure_{uuid4().hex[:12]}",
            status=BranchStatusChoices.MERGED,
            merged_time=timezone.now(),
        )
        ingestion = ForwardIngestion.objects.create(
            sync=self.sync,
            snapshot_selector=LATEST_PROCESSED_SNAPSHOT,
            snapshot_id="snapshot-catchup-failure",
            branch=branch,
            merge_applied_at=branch.merged_time,
        )
        self.sync.status = ForwardSyncStatusChoices.MERGING
        self.sync.save(update_fields=["status"])

        self.assertTrue(resume_post_merge_bookkeeping(ingestion))
        ingestion.refresh_from_db()
        self.assertEqual(
            ingestion.catchup_status,
            ForwardCatchupStatusChoices.PENDING,
        )
        self._complete_ownership(ingestion)
        with (
            patch(
                "forward_netbox.utilities.ingestion_merge.latest_processed_catchup_decision",
                side_effect=RuntimeError("credential unavailable"),
            ),
            self.assertRaisesMessage(RuntimeError, "credential unavailable"),
        ):
            resume_post_merge_bookkeeping(ingestion)

        ingestion.refresh_from_db()
        self.sync.refresh_from_db()
        self.assertEqual(self.sync.status, ForwardSyncStatusChoices.COMPLETED)
        self.assertTrue(ingestion.baseline_ready)
        self.assertIsNone(ingestion.branch)
        self.assertEqual(
            ingestion.catchup_status,
            ForwardCatchupStatusChoices.FAILED,
        )
        self.assertEqual(ingestion.catchup_reason, "catchup_check_exception")
        self.assertEqual(ingestion.catchup_error_type, "RuntimeError")
        self.assertIsNotNone(ingestion.catchup_checked_at)

        with patch(
            "forward_netbox.utilities.ingestion_merge.latest_processed_catchup_decision",
            return_value={"should_queue": False, "reason": "already_current"},
        ) as decision:
            self.assertTrue(resume_post_merge_bookkeeping(ingestion))

        ingestion.refresh_from_db()
        self.assertEqual(
            ingestion.catchup_status,
            ForwardCatchupStatusChoices.CURRENT,
        )
        self.assertEqual(ingestion.catchup_reason, "already_current")
        self.assertEqual(ingestion.catchup_error_type, "")
        decision.assert_called_once()

    def test_post_merge_catchup_queue_is_persisted(self):
        branch = Branch.objects.create(
            name=f"catchup-queue-{uuid4().hex[:12]}",
            schema_id=f"catchup_queue_{uuid4().hex[:12]}",
            status=BranchStatusChoices.MERGED,
            merged_time=timezone.now(),
        )
        ingestion = ForwardIngestion.objects.create(
            sync=self.sync,
            snapshot_selector=LATEST_PROCESSED_SNAPSHOT,
            snapshot_id="snapshot-catchup-current",
            branch=branch,
            merge_applied_at=branch.merged_time,
        )
        self.sync.status = ForwardSyncStatusChoices.MERGING
        self.sync.save(update_fields=["status"])
        queued_job = MagicMock(pk=991)
        decision = {
            "should_queue": True,
            "reason": "latest_processed_advanced",
            "snapshot_selector": LATEST_PROCESSED_SNAPSHOT,
            "current_snapshot_id": ingestion.snapshot_id,
            "latest_processed_snapshot_id": "snapshot-catchup-target",
        }

        with (
            patch(
                "forward_netbox.utilities.ingestion_merge.latest_processed_catchup_decision",
                return_value=decision,
            ),
            patch.object(
                ForwardSync,
                "enqueue_sync_job",
                return_value=queued_job,
            ) as enqueue,
        ):
            self.assertTrue(resume_post_merge_bookkeeping(ingestion))
            ingestion.refresh_from_db()
            self.assertEqual(
                ingestion.catchup_status,
                ForwardCatchupStatusChoices.PENDING,
            )
            enqueue.assert_not_called()
            self._complete_ownership(ingestion)
            self.assertTrue(resume_post_merge_bookkeeping(ingestion))

        ingestion.refresh_from_db()
        self.assertEqual(
            ingestion.catchup_status,
            ForwardCatchupStatusChoices.QUEUED,
        )
        self.assertEqual(
            ingestion.catchup_target_snapshot_id,
            "snapshot-catchup-target",
        )
        self.assertEqual(ingestion.catchup_reason, "latest_processed_advanced")
        enqueue.assert_called_once()

    def test_catchup_does_not_report_current_while_sync_is_active(self):
        ingestion = ForwardIngestion.objects.create(
            sync=self.sync,
            snapshot_selector=LATEST_PROCESSED_SNAPSHOT,
            snapshot_id="snapshot-active",
        )
        self.sync.status = ForwardSyncStatusChoices.SYNCING
        self.sync.save(update_fields=["status"])

        with patch(
            "forward_netbox.utilities.ingestion_merge.latest_processed_catchup_decision",
            return_value={"should_queue": False, "reason": "sync_not_completed"},
        ):
            reconcile_ingestion_catchup(ingestion)

        ingestion.refresh_from_db()
        self.assertEqual(
            ingestion.catchup_status,
            ForwardCatchupStatusChoices.QUEUED,
        )
        self.assertEqual(ingestion.catchup_reason, "sync_not_completed")

    def test_catchup_marks_nonterminal_check_failed_when_sync_is_not_active(self):
        ingestion = ForwardIngestion.objects.create(
            sync=self.sync,
            snapshot_selector=LATEST_PROCESSED_SNAPSHOT,
            snapshot_id="snapshot-failed",
        )
        self.sync.status = ForwardSyncStatusChoices.FAILED
        self.sync.save(update_fields=["status"])

        with patch(
            "forward_netbox.utilities.ingestion_merge.latest_processed_catchup_decision",
            return_value={"should_queue": False, "reason": "sync_not_completed"},
        ):
            reconcile_ingestion_catchup(ingestion)

        ingestion.refresh_from_db()
        self.assertEqual(
            ingestion.catchup_status,
            ForwardCatchupStatusChoices.FAILED,
        )
        self.assertEqual(ingestion.catchup_reason, "sync_not_completed")

    def test_resume_post_merge_bookkeeping_requires_missing_branch_attestation(self):
        ingestion = ForwardIngestion.objects.create(
            sync=self.sync,
            snapshot_selector=LATEST_PROCESSED_SNAPSHOT,
            snapshot_id="snapshot-crash-missing-branch",
        )
        self.sync.status = ForwardSyncStatusChoices.MERGING
        self.sync.save(update_fields=["status"])

        self.assertFalse(resume_post_merge_bookkeeping(ingestion))
        ingestion.refresh_from_db()
        self.sync.refresh_from_db()
        self.assertFalse(ingestion.baseline_ready)
        self.assertEqual(self.sync.status, ForwardSyncStatusChoices.MERGING)

        merged_at = timezone.now()
        ForwardIngestion.objects.filter(pk=ingestion.pk).update(
            merge_applied_at=merged_at
        )
        ingestion.merge_applied_at = merged_at
        with patch(
            "forward_netbox.utilities.ingestion_merge.latest_processed_catchup_decision",
            return_value={"should_queue": False},
        ):
            self.assertTrue(
                resume_post_merge_bookkeeping(ingestion, remove_branch=False)
            )

        ingestion.refresh_from_db()
        self.sync.refresh_from_db()
        self.assertTrue(ingestion.baseline_ready)
        self.assertEqual(self.sync.status, ForwardSyncStatusChoices.COMPLETED)

    @patch("forward_netbox.models.ForwardIngestion.objects.filter")
    @patch("forward_netbox.utilities.ingestion_merge.enqueue_forward_job")
    def test_enqueue_merge_job_persists_merge_job_reference(
        self,
        mock_enqueue,
        mock_filter,
    ):
        ingestion = ForwardIngestion.objects.create(
            sync=self.sync,
            snapshot_selector=LATEST_PROCESSED_SNAPSHOT,
            snapshot_id="snapshot-merge",
        )
        mock_enqueue.return_value = Job.objects.create(
            object_type=ContentType.objects.get_for_model(ForwardIngestion),
            object_id=ingestion.pk,
            name=f"{ingestion.sync.name} Merge",
            user=None,
            status=JobStatusChoices.STATUS_COMPLETED,
            job_id=uuid4(),
            created=timezone.now(),
            started=timezone.now(),
            completed=timezone.now(),
            data={},
        )
        mock_filter.return_value.update.return_value = 1

        job = enqueue_merge_job(ingestion, user=None, remove_branch=True)

        self.sync.refresh_from_db()
        self.assertEqual(self.sync.status, ForwardSyncStatusChoices.QUEUED)
        self.assertEqual(ingestion.merge_job, job)
        mock_enqueue.assert_called_once()
        mock_filter.assert_called()

    @patch("forward_netbox.models.ForwardIngestion.objects.filter")
    @patch("forward_netbox.utilities.ingestion_merge.enqueue_forward_job")
    @patch.object(Branch, "get_unmerged_changes")
    def test_enqueue_merge_job_sizes_timeout_from_unmerged_changes(
        self,
        get_unmerged_changes,
        mock_enqueue,
        mock_filter,
    ):
        branch = Branch.objects.create(
            name="merge-timeout-sizing",
            schema_id="merge_timeout_sizing",
            status=BranchStatusChoices.READY,
        )
        ingestion = ForwardIngestion.objects.create(
            sync=self.sync,
            branch=branch,
            snapshot_selector=LATEST_PROCESSED_SNAPSHOT,
            snapshot_id="snapshot-merge-timeout",
        )
        get_unmerged_changes.return_value.count.return_value = 750_000
        mock_enqueue.return_value = Job.objects.create(
            object_type=ContentType.objects.get_for_model(ForwardIngestion),
            object_id=ingestion.pk,
            name=f"{ingestion.sync.name} Merge",
            status=JobStatusChoices.STATUS_COMPLETED,
            job_id=uuid4(),
            completed=timezone.now(),
        )

        enqueue_merge_job(ingestion, user=None)

        self.assertEqual(mock_enqueue.call_args.kwargs["job_timeout"], 75_000)
        mock_filter.assert_called()

    def test_record_change_totals_persists_counts(self):
        ingestion = ForwardIngestion.objects.create(
            sync=self.sync,
            snapshot_selector=LATEST_PROCESSED_SNAPSHOT,
            snapshot_id="snapshot-counts",
        )

        record_change_totals(
            ingestion,
            applied=12,
            failed=3,
            created=4,
            updated=5,
            deleted=6,
        )

        ingestion.refresh_from_db()
        self.assertEqual(ingestion.applied_change_count, 12)
        self.assertEqual(ingestion.failed_change_count, 3)
        self.assertEqual(ingestion.created_change_count, 4)
        self.assertEqual(ingestion.updated_change_count, 5)
        self.assertEqual(ingestion.deleted_change_count, 6)

    def test_merge_progress_updates_heartbeat_and_sparse_log(self):
        from forward_netbox.utilities.merge import _report_merge_progress

        ingestion = ForwardIngestion.objects.create(
            sync=self.sync,
            snapshot_selector=LATEST_PROCESSED_SNAPSHOT,
            snapshot_id="snapshot-merge-progress",
        )
        sync_logger = MagicMock()

        with patch("forward_netbox.utilities.merge.time.monotonic", return_value=101.0):
            heartbeat_at, log_at = _report_merge_progress(
                ingestion,
                sync_logger=sync_logger,
                model_string="ipam.prefix",
                processed=5000,
                total_changes=9295,
                last_heartbeat_at=100.0,
                last_log_at=100.0,
            )

        sync_logger.log_info.assert_called_once_with(
            "Merged 5000/9295 branch changes (current model `ipam.prefix`)."
        )
        self.assertEqual(heartbeat_at, 101.0)
        self.assertEqual(log_at, 101.0)

    def test_merge_progress_skips_noisy_updates_between_intervals(self):
        from forward_netbox.utilities.merge import _report_merge_progress

        ingestion = ForwardIngestion.objects.create(
            sync=self.sync,
            snapshot_selector=LATEST_PROCESSED_SNAPSHOT,
            snapshot_id="snapshot-merge-progress-quiet",
        )
        sync_logger = MagicMock()

        with patch("forward_netbox.utilities.merge.time.monotonic", return_value=101.0):
            heartbeat_at, log_at = _report_merge_progress(
                ingestion,
                sync_logger=sync_logger,
                model_string="ipam.prefix",
                processed=999,
                total_changes=9295,
                last_heartbeat_at=100.0,
                last_log_at=100.0,
            )

        sync_logger.log_info.assert_not_called()
        self.assertEqual(heartbeat_at, 100.0)
        self.assertEqual(log_at, 100.0)


class MergeIssueRecorderTest(TestCase):
    def setUp(self):
        self.source = ForwardSource.objects.create(
            name="source-merge-issue-recorder",
            type="saas",
            url="https://fwd.app",
            parameters={
                "username": "user@example.com",
                "password": "secret",
                "verify": True,
                "timeout": 1200,
                "network_id": "test-network",
            },
        )
        self.sync = ForwardSync.objects.create(
            name="sync-merge-issue-recorder",
            source=self.source,
            parameters={"snapshot_id": LATEST_PROCESSED_SNAPSHOT, "dcim.module": True},
        )
        self.ingestion = ForwardIngestion.objects.create(
            sync=self.sync,
            snapshot_selector=LATEST_PROCESSED_SNAPSHOT,
            snapshot_id="snapshot-merge-issue",
        )

    def test_module_bay_failures_are_recorded_as_blocking_merge_issues(self):
        from forward_netbox.utilities.merge import _MergeIssueRecorder

        recorder = _MergeIssueRecorder(self.ingestion, None)
        exc = Exception("Save with update_fields did not affect any rows.")
        recorder.record(
            model_string="dcim.modulebay",
            exc=exc,
        )

        issues = list(self.ingestion.issues.all())
        self.assertEqual(len(issues), 1)
        issue = issues[0]
        self.assertEqual(issue.model, "dcim.modulebay")
        self.assertEqual(issue.exception, "Exception")
        self.assertEqual(issue.message, "Merge for dcim.modulebay failed (Exception).")
        self.assertEqual(issue.raw_data, {})
        self.assertNotIn("Save with update_fields", issue.message)
        self.assertTrue(has_blocking_issues(self.ingestion))

    def test_synced_model_failures_recorded_per_change(self):
        from forward_netbox.utilities.merge import _MergeIssueRecorder

        recorder = _MergeIssueRecorder(self.ingestion, None)
        recorder.record(
            model_string="dcim.device",
            exc=ValueError("boom"),
        )
        recorder.record(
            model_string="dcim.device",
            exc=ValueError("boom2"),
        )
        device_issues = list(self.ingestion.issues.filter(model="dcim.device"))
        self.assertEqual(len(device_issues), 2)
        self.assertEqual(device_issues[0].exception, "ValueError")
        self.assertFalse(self.ingestion.issues.filter(model="dcim.modulebay").exists())
