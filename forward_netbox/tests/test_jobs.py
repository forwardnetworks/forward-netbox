import threading
from datetime import datetime
from datetime import timedelta
from types import SimpleNamespace
from unittest.mock import Mock
from unittest.mock import patch
from uuid import uuid4

from core.choices import JobStatusChoices
from core.exceptions import SyncError
from core.models import Job
from dcim.models import Site
from django.contrib.auth import get_user_model
from django.contrib.contenttypes.models import ContentType
from django.db import close_old_connections
from django.db import connections
from django.db import transaction
from django.test import TestCase
from django.test import TransactionTestCase
from django.utils import timezone
from netbox.context import current_request
from netbox.context import query_cache
from netbox_branching.choices import BranchStatusChoices
from netbox_branching.models import Branch
from rq.timeouts import JobTimeoutException

from forward_netbox.choices import ForwardIngestionPhaseChoices
from forward_netbox.choices import ForwardSyncStatusChoices
from forward_netbox.exceptions import ForwardOwnershipDispatchError
from forward_netbox.exceptions import ForwardPartialMergeError
from forward_netbox.jobs import _resolve_authoritative_merge_failure
from forward_netbox.jobs import DependencyPreviewJob
from forward_netbox.jobs import ForwardJobRunner
from forward_netbox.jobs import merge_forwardingestion
from forward_netbox.jobs import record_timeout_issue
from forward_netbox.jobs import safe_save_job_data
from forward_netbox.jobs import sync_forwardsync
from forward_netbox.jobs import terminate_job_once
from forward_netbox.jobs import ValidationJob
from forward_netbox.models import ForwardIngestion
from forward_netbox.models import ForwardIngestionIssue
from forward_netbox.models import ForwardOwnershipReconciliation
from forward_netbox.models import ForwardSource
from forward_netbox.models import ForwardSync
from forward_netbox.utilities.stuck_recovery import _completed_mark
from forward_netbox.utilities.stuck_recovery import _terminal_mark
from forward_netbox.utilities.sync_facade import enqueue_preview_schedule
from forward_netbox.utilities.sync_facade import enqueue_validation_job
from forward_netbox.utilities.sync_facade import reconcile_standing_schedules


class ForwardJobsTest(TestCase):
    def setUp(self):
        self.addCleanup(current_request.set, None)
        self.user = get_user_model().objects.create_user(username="sync-jobs-owner")
        self.source = ForwardSource.objects.create(
            name="source-jobs",
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
            name="sync-jobs",
            source=self.source,
            user=self.user,
            auto_merge=False,
            parameters={"snapshot_id": "latestProcessed"},
        )
        self.ingestion = ForwardIngestion.objects.create(sync=self.sync)

    def _sync_job(self, name="sync-job"):
        return Job.enqueue(
            sync_forwardsync,
            instance=self.sync,
            user=self.user,
            name=name,
            adhoc=True,
        )

    def test_job_termination_is_idempotent_after_terminal_race(self):
        from extras.models import Notification

        job = self._sync_job("terminal-race")
        job.start()
        job.terminate(status=JobStatusChoices.STATUS_ERRORED)

        self.assertFalse(
            terminate_job_once(job, status=JobStatusChoices.STATUS_ERRORED)
        )
        job.refresh_from_db()
        job_type = ContentType.objects.get_for_model(Job)
        self.assertEqual(job.status, JobStatusChoices.STATUS_ERRORED)
        self.assertEqual(
            Notification.objects.filter(
                object_type=job_type,
                object_id=job.pk,
                user=self.user,
            ).count(),
            1,
        )

    def test_handled_sync_failure_terminates_job_as_errored(self):
        from extras.models import Notification

        job = self._sync_job("handled-sync-failure")

        def handled_failure(sync, **kwargs):
            sync.status = ForwardSyncStatusChoices.FAILED
            ForwardSync.objects.filter(pk=sync.pk).update(status=sync.status)

        with (
            patch.object(ForwardSync, "sync", new=handled_failure),
            patch("forward_netbox.jobs._enqueue_post_sync_overlays") as overlays,
        ):
            sync_forwardsync(job, adhoc=True)

        job.refresh_from_db()
        job_type = ContentType.objects.get_for_model(Job)
        self.assertEqual(job.status, JobStatusChoices.STATUS_ERRORED)
        self.assertIn("failed", job.error)
        self.assertEqual(
            Notification.objects.filter(
                object_type=job_type,
                object_id=job.pk,
                user=self.user,
            ).count(),
            1,
        )
        overlays.assert_not_called()

    def test_sync_job_timeout_persists_state_and_propagates_to_rq(self):
        job = self._sync_job("sync-timeout")

        with (
            patch.object(
                ForwardSync,
                "sync",
                side_effect=JobTimeoutException("sync timed out"),
            ),
            patch("forward_netbox.jobs._enqueue_post_sync_overlays") as overlays,
            self.assertRaisesRegex(JobTimeoutException, "sync timed out"),
        ):
            sync_forwardsync(job, adhoc=True)

        job.refresh_from_db()
        self.sync.refresh_from_db()
        self.assertEqual(job.status, JobStatusChoices.STATUS_ERRORED)
        self.assertIn("sync timed out", job.error)
        self.assertEqual(self.sync.status, ForwardSyncStatusChoices.TIMEOUT)
        overlays.assert_not_called()

    def test_scope_tag_reconciliation_enqueues_after_completed_sync(self):
        from forward_netbox.jobs import _maybe_enqueue_backfilled_tag_refresh

        self.sync.status = ForwardSyncStatusChoices.COMPLETED
        self.sync.save(update_fields=["status"])

        with patch(
            "forward_netbox.jobs.DeviceScopeTagReconciliationJob.enqueue"
        ) as enqueue:
            _maybe_enqueue_backfilled_tag_refresh(
                self.sync,
                snapshot_id="snapshot-1",
            )
            enqueue.assert_called_once()
            self.assertEqual(enqueue.call_args.kwargs["instance"], self.sync)
            self.assertEqual(enqueue.call_args.kwargs["snapshot_id"], "snapshot-1")

    def test_scope_tag_reconciliation_requires_completed_sync_and_snapshot(self):
        from forward_netbox.jobs import _maybe_enqueue_backfilled_tag_refresh

        self.source.parameters = {
            **self.source.parameters,
            "apply_device_scope_tags": True,
        }
        self.source.save(update_fields=["parameters"])

        for status in (
            ForwardSyncStatusChoices.FAILED,
            ForwardSyncStatusChoices.READY_TO_MERGE,
        ):
            self.sync.status = status
            with patch(
                "forward_netbox.jobs.DeviceScopeTagReconciliationJob.enqueue"
            ) as enqueue:
                _maybe_enqueue_backfilled_tag_refresh(
                    self.sync,
                    snapshot_id="snapshot-1",
                )
                enqueue.assert_not_called()

        self.sync.status = ForwardSyncStatusChoices.COMPLETED
        with patch(
            "forward_netbox.jobs.DeviceScopeTagReconciliationJob.enqueue"
        ) as enqueue:
            _maybe_enqueue_backfilled_tag_refresh(self.sync)
            enqueue.assert_not_called()

    def test_post_sync_overlays_require_completed_sync(self):
        from forward_netbox.jobs import _enqueue_post_sync_overlays

        with (
            patch(
                "forward_netbox.jobs._maybe_enqueue_device_analysis_refresh"
            ) as analysis,
            patch(
                "forward_netbox.jobs._maybe_enqueue_backfilled_tag_refresh"
            ) as scope_tags,
            patch("forward_netbox.jobs._maybe_enqueue_vsys_parent_link") as parents,
        ):
            helpers = (analysis, scope_tags, parents)
            for status in (
                ForwardSyncStatusChoices.FAILED,
                ForwardSyncStatusChoices.READY_TO_MERGE,
            ):
                ForwardSync.objects.filter(pk=self.sync.pk).update(status=status)
                _enqueue_post_sync_overlays(
                    self.sync,
                    snapshot_id="snapshot-1",
                )
            for helper in helpers:
                helper.assert_not_called()

            ForwardSync.objects.filter(pk=self.sync.pk).update(
                status=ForwardSyncStatusChoices.COMPLETED
            )
            self.ingestion.snapshot_id = "snapshot-1"
            self.ingestion.baseline_ready = True
            self.ingestion.save(update_fields=["snapshot_id", "baseline_ready"])
            _enqueue_post_sync_overlays(
                self.sync,
                snapshot_id="snapshot-1",
            )
            for helper in helpers:
                helper.assert_called_once()

    def test_vsys_parent_link_enqueues_by_default_and_respects_opt_out(self):
        from forward_netbox.jobs import _maybe_enqueue_vsys_parent_link

        self.sync.status = ForwardSyncStatusChoices.COMPLETED
        # DEFAULT-ON: no parameter -> the parent-link overlay enqueues (a blank
        # Parent Device on every vsys/vdom is a confusing default).
        with patch(
            "forward_netbox.jobs.VirtualParentReconciliationJob.enqueue"
        ) as enqueue:
            _maybe_enqueue_vsys_parent_link(
                self.sync,
                snapshot_id="snapshot-1",
            )
            enqueue.assert_called_once()
            self.assertEqual(enqueue.call_args.kwargs["instance"], self.sync)
            self.assertEqual(enqueue.call_args.kwargs["snapshot_id"], "snapshot-1")

        # Explicit opt-out with auto_link_vsys_parents=False -> no enqueue.
        self.sync.parameters = {
            **self.sync.parameters,
            "auto_link_vsys_parents": False,
        }
        with patch(
            "forward_netbox.jobs.VirtualParentReconciliationJob.enqueue"
        ) as enqueue:
            _maybe_enqueue_vsys_parent_link(
                self.sync,
                snapshot_id="snapshot-1",
            )
            enqueue.assert_not_called()

    def test_overlay_enqueue_persists_pending_ownership_generation(self):
        from forward_netbox.jobs import _enqueue_post_sync_overlays

        self.ingestion.snapshot_id = "snapshot-1"
        self.ingestion.baseline_ready = True
        self.ingestion.save(update_fields=["snapshot_id", "baseline_ready"])
        self.sync.status = ForwardSyncStatusChoices.COMPLETED
        self.sync.save(update_fields=["status"])
        with (
            patch("forward_netbox.jobs._maybe_enqueue_device_analysis_refresh"),
            patch("forward_netbox.jobs._maybe_enqueue_backfilled_tag_refresh"),
            patch("forward_netbox.jobs._maybe_enqueue_vsys_parent_link"),
        ):
            _enqueue_post_sync_overlays(
                self.sync,
                snapshot_id="snapshot-1",
                ingestion_id=self.ingestion.pk,
            )

        reconciliation = ForwardOwnershipReconciliation.objects.get(
            sync=self.sync,
            domain=ForwardOwnershipReconciliation.Domain.VIRTUAL_PARENTS,
        )
        self.assertEqual(reconciliation.generation, self.ingestion.pk)
        self.assertEqual(
            reconciliation.status,
            ForwardOwnershipReconciliation.Status.PENDING,
        )

    def test_required_overlay_enqueue_failure_remains_durable_and_visible(self):
        from forward_netbox.jobs import _finish_completed_job_with_overlays

        self.ingestion.snapshot_id = "snapshot-1"
        self.ingestion.baseline_ready = True
        self.ingestion.save(update_fields=["snapshot_id", "baseline_ready"])
        self.sync.status = ForwardSyncStatusChoices.COMPLETED
        self.sync.save(update_fields=["status"])
        self.sync.logger = Mock()
        job = Mock(pk=771)

        with patch(
            "forward_netbox.jobs._maybe_enqueue_vsys_parent_link",
            side_effect=RuntimeError("queue unavailable"),
        ):
            completed = _finish_completed_job_with_overlays(
                job,
                self.sync,
                snapshot_id="snapshot-1",
                ingestion_id=self.ingestion.pk,
            )

        self.assertFalse(completed)
        job.terminate.assert_called_once_with(status=JobStatusChoices.STATUS_ERRORED)
        reconciliation = ForwardOwnershipReconciliation.objects.get(
            sync=self.sync,
            domain=ForwardOwnershipReconciliation.Domain.VIRTUAL_PARENTS,
        )
        self.assertEqual(
            reconciliation.status,
            ForwardOwnershipReconciliation.Status.PENDING,
        )
        self.sync.logger.log_failure.assert_called_once()

    def test_post_sync_dispatch_raises_typed_error_without_baseline(self):
        from forward_netbox.jobs import _enqueue_post_sync_overlays

        self.sync.status = ForwardSyncStatusChoices.COMPLETED
        self.sync.save(update_fields=["status"])

        with self.assertRaises(ForwardOwnershipDispatchError):
            _enqueue_post_sync_overlays(
                self.sync,
                snapshot_id="snapshot-1",
            )

    def test_parent_overlay_failure_is_durable_aggregate_evidence(self):
        from forward_netbox import jobs
        from forward_netbox.utilities.ownership import mark_ownership_pending

        self.ingestion.snapshot_id = "snapshot-1"
        self.ingestion.baseline_ready = True
        self.ingestion.save(update_fields=["snapshot_id", "baseline_ready"])
        mark_ownership_pending(
            self.sync,
            self.ingestion.pk,
            self.ingestion.snapshot_id,
        )
        job = Mock(object_id=self.sync.pk)
        with (
            patch(
                "forward_netbox.utilities.vsys_parent.link_vsys_parents",
                side_effect=RuntimeError("parent conflict"),
            ),
            self.assertRaises(RuntimeError),
        ):
            jobs._link_forward_vsys_parents_work(
                job,
                snapshot_id="snapshot-1",
                ingestion_id=self.ingestion.pk,
            )

        reconciliation = ForwardOwnershipReconciliation.objects.get(
            sync=self.sync,
            domain=ForwardOwnershipReconciliation.Domain.VIRTUAL_PARENTS,
        )
        self.assertEqual(
            reconciliation.status,
            ForwardOwnershipReconciliation.Status.FAILED,
        )
        self.assertEqual(reconciliation.error_type, "RuntimeError")

    def test_overlay_workers_forward_the_pinned_snapshot(self):
        from forward_netbox import jobs

        cases = (
            (
                jobs._refresh_forward_device_analysis_work,
                "forward_netbox.utilities.device_analysis.refresh_device_analysis",
            ),
            (
                jobs._reconcile_forward_device_scope_tags_work,
                "forward_netbox.utilities.scope_reconciliation.tag_backfilled_devices",
            ),
            (
                jobs._link_forward_vsys_parents_work,
                "forward_netbox.utilities.vsys_parent.link_vsys_parents",
            ),
        )
        for worker, utility_path in cases:
            with self.subTest(worker=worker.__name__):
                job = Mock(object_id=self.sync.pk)
                with patch(utility_path, return_value={"ok": True}) as utility:
                    worker(job, snapshot_id="snapshot-pinned")
                self.assertEqual(
                    utility.call_args.kwargs["snapshot_id"],
                    "snapshot-pinned",
                )

    def test_overlay_workers_complete_stale_snapshot_and_request_catch_up(self):
        from forward_netbox import jobs
        from forward_netbox.utilities.post_sync import StalePostSyncSnapshotError

        cases = (
            (
                jobs._refresh_forward_device_analysis_work,
                "forward_netbox.utilities.device_analysis.refresh_device_analysis",
            ),
            (
                jobs._reconcile_forward_device_scope_tags_work,
                "forward_netbox.utilities.scope_reconciliation.tag_backfilled_devices",
            ),
            (
                jobs._link_forward_vsys_parents_work,
                "forward_netbox.utilities.vsys_parent.link_vsys_parents",
            ),
        )
        for worker, utility_path in cases:
            with self.subTest(worker=worker.__name__):
                job = Mock(object_id=self.sync.pk)
                with (
                    patch(
                        utility_path,
                        side_effect=StalePostSyncSnapshotError("stale"),
                    ),
                    patch(
                        "forward_netbox.jobs._complete_stale_post_sync_overlay"
                    ) as catch_up,
                ):
                    worker(job, snapshot_id="snapshot-stale")
                catch_up.assert_called_once()
                self.assertEqual(catch_up.call_args.args[1].pk, self.sync.pk)

    def test_stale_overlay_completion_redacts_snapshots_and_enqueues_latest(self):
        from forward_netbox.jobs import _complete_stale_post_sync_overlay

        self.ingestion.snapshot_id = "snapshot-latest"
        self.ingestion.baseline_ready = True
        self.ingestion.save(update_fields=["snapshot_id", "baseline_ready"])
        self.sync.status = ForwardSyncStatusChoices.COMPLETED
        self.sync.save(update_fields=["status"])
        job = Mock()

        with patch("forward_netbox.jobs._enqueue_post_sync_overlays") as enqueue:
            _complete_stale_post_sync_overlay(job, self.sync)

        self.assertEqual(
            job.data,
            {
                "skipped": "stale_post_sync_snapshot",
                "catch_up_requested": True,
            },
        )
        enqueue.assert_called_once_with(
            self.sync,
            snapshot_id="snapshot-latest",
            ingestion_id=self.ingestion.pk,
            exclude_job_id=job.pk,
        )

    def test_stale_running_overlay_does_not_suppress_its_successor(self):
        from forward_netbox.jobs import _complete_stale_post_sync_overlay

        self.ingestion.snapshot_id = "snapshot-latest"
        self.ingestion.baseline_ready = True
        self.ingestion.save(update_fields=["snapshot_id", "baseline_ready"])
        self.sync.status = ForwardSyncStatusChoices.COMPLETED
        self.sync.save(update_fields=["status"])
        name = f"{self.sync.name} - reconcile device scope tags (auto)"
        current_job = Job.objects.create(
            object_type=ContentType.objects.get_for_model(ForwardSync),
            object_id=self.sync.pk,
            name=name,
            status=JobStatusChoices.STATUS_RUNNING,
            job_id=uuid4(),
        )

        with (
            patch(
                "forward_netbox.jobs.DeviceScopeTagReconciliationJob.enqueue"
            ) as scope_enqueue,
            patch("forward_netbox.jobs.VirtualParentReconciliationJob.enqueue"),
            patch("forward_netbox.jobs.DeviceAnalysisRefreshJob.enqueue"),
        ):
            _complete_stale_post_sync_overlay(current_job, self.sync)

        scope_enqueue.assert_called_once()
        self.assertEqual(
            scope_enqueue.call_args.kwargs["ingestion_id"],
            self.ingestion.pk,
        )

    def test_overlay_enqueue_skips_when_active_job_exists(self):
        # Regression (hung-pending pile-up): the default-on vsys overlay enqueues
        # after every sync; if one is still pending/running it must NOT enqueue a
        # duplicate that stacks up behind it.
        from django.contrib.contenttypes.models import ContentType

        from forward_netbox.jobs import _maybe_enqueue_vsys_parent_link

        self.sync.status = ForwardSyncStatusChoices.COMPLETED
        name = f"{self.sync.name} - link vsys/vdom parents (auto)"
        Job.objects.create(
            object_type=ContentType.objects.get_for_model(ForwardSync),
            object_id=self.sync.pk,
            name=name,
            status=JobStatusChoices.STATUS_PENDING,
            job_id=uuid4(),
        )
        with patch(
            "forward_netbox.jobs.VirtualParentReconciliationJob.enqueue"
        ) as enqueue:
            _maybe_enqueue_vsys_parent_link(
                self.sync,
                snapshot_id="snapshot-1",
            )
            enqueue.assert_not_called()

        # A completed job of the same name does NOT block a fresh enqueue.
        Job.objects.filter(object_id=self.sync.pk, name=name).update(
            status=JobStatusChoices.STATUS_COMPLETED
        )
        with patch(
            "forward_netbox.jobs.VirtualParentReconciliationJob.enqueue"
        ) as enqueue:
            _maybe_enqueue_vsys_parent_link(
                self.sync,
                snapshot_id="snapshot-1",
            )
            enqueue.assert_called_once()

    def test_orphan_prune_job_reports_current_protection_evidence(self):
        from forward_netbox.jobs import _prune_forward_orphans_work

        job = Mock(object_id=self.sync.pk)
        with (
            patch(
                "forward_netbox.utilities.scope_reconciliation.compute_scope_reconciliation",
                return_value={"out_of_scope": ["device-1"]},
            ),
            patch(
                "forward_netbox.utilities.scope_reconciliation.prune_orphan_devices",
                return_value={
                    "pruned_device_count": 1,
                    "pruned_object_count": 4,
                    "out_of_scope_sample": ["device-1"],
                    "ownership_blocked_device_count": 2,
                    "protected_device_count": 3,
                    "protected_by_model": {"netbox_routing.bgppeer": 3},
                },
            ),
            patch(
                "forward_netbox.utilities.scope_reconciliation.prune_orphan_sites",
                return_value={"pruned_site_count": 1},
            ),
        ):
            _prune_forward_orphans_work(job)

        self.assertEqual(job.data["ownership_blocked_device_count"], 2)
        self.assertEqual(job.data["protected_device_count"], 3)
        self.assertEqual(
            job.data["protected_by_model"],
            {"netbox_routing.bgppeer": 3},
        )
        self.assertNotIn("pruned_dependent_rows", job.data)
        self.assertNotIn("pruned_dangling_rows", job.data)

    def _make_active_job(self):
        from django.contrib.contenttypes.models import ContentType

        return Job.objects.create(
            object_type=ContentType.objects.get_for_model(ForwardSync),
            object_id=self.sync.pk,
            name=f"{self.sync.name} - sync",
            status=JobStatusChoices.STATUS_RUNNING,
            job_id=uuid4(),
        )

    def test_stuck_job_alert_flags_a_dead_active_job(self):
        import json
        from io import StringIO

        from django.core.management import call_command

        self._make_active_job()
        out = StringIO()
        with patch(
            "forward_netbox.management.commands.forward_stuck_job_alert."
            "job_has_live_execution",
            return_value=False,
        ):
            call_command("forward_stuck_job_alert", stdout=out)
        payload = json.loads(out.getvalue())
        self.assertEqual(payload["stuck_job_count"], 1)
        self.assertIn("alert", payload)

    def test_stuck_job_alert_ignores_a_live_active_job(self):
        import json
        from io import StringIO

        from django.core.management import call_command

        self._make_active_job()
        out = StringIO()
        with patch(
            "forward_netbox.management.commands.forward_stuck_job_alert."
            "job_has_live_execution",
            return_value=True,
        ):
            call_command("forward_stuck_job_alert", stdout=out)
        payload = json.loads(out.getvalue())
        self.assertEqual(payload["stuck_job_count"], 0)
        self.assertNotIn("alert", payload)

    def test_metrics_command_emits_prometheus_exposition(self):
        from io import StringIO

        from django.core.management import call_command

        out = StringIO()
        call_command("forward_metrics", stdout=out)
        text = out.getvalue()
        # Prometheus exposition shape: HELP/TYPE lines plus a sample per metric.
        self.assertIn("# TYPE forward_sources_total gauge", text)
        self.assertIn("forward_syncs_total ", text)
        self.assertIn("forward_jobs{status=", text)
        self.assertIn("forward_stuck_jobs ", text)
        # At least one source/sync exists (setUp), so the counts are >= 1.
        self.assertRegex(text, r"forward_syncs_total [1-9]")

    def test_record_timeout_issue_creates_single_issue_per_ingestion_phase(self):
        issue_1 = record_timeout_issue(
            self.ingestion,
            ForwardIngestionPhaseChoices.SYNC,
            "timeout",
        )
        issue_2 = record_timeout_issue(
            self.ingestion,
            ForwardIngestionPhaseChoices.SYNC,
            "timeout again",
        )

        self.assertEqual(issue_1.pk, issue_2.pk)
        self.assertEqual(
            ForwardIngestionIssue.objects.filter(
                ingestion=self.ingestion,
                phase=ForwardIngestionPhaseChoices.SYNC,
                exception=JobTimeoutException.__name__,
            ).count(),
            1,
        )

    def test_safe_save_job_data_persists_job_log_entries(self):
        class DummyJob:
            pk = 52

            def __init__(self):
                self.data = None
                self.log_entries = []
                self.saved_update_fields = None

            def save(self, update_fields=None):
                self.saved_update_fields = update_fields

        job = DummyJob()
        sync_logger = Mock()
        sync_logger.log_data = {
            "logs": [
                [
                    "2026-05-03T14:34:00+00:00",
                    "success",
                    "ui-harness-sync",
                    "/plugins/forward/sync/2/",
                    "Synthetic UI harness ingestion completed.",
                ]
            ],
            "statistics": {},
        }
        obj_with_logger = SimpleNamespace(logger=sync_logger)

        safe_save_job_data(job, obj_with_logger)

        self.assertEqual(
            job.data["logs"][0][4],
            "Synthetic UI harness ingestion completed.",
        )
        self.assertEqual(len(job.log_entries), 1)
        self.assertEqual(job.log_entries[0]["level"], "info")
        self.assertEqual(
            job.log_entries[0]["message"],
            "Synthetic UI harness ingestion completed.",
        )
        self.assertEqual(job.saved_update_fields, ["data", "log_entries"])
        sync_logger.flush.assert_called_once_with()

    def test_safe_save_job_data_flushes_debounced_statistics_first(self):
        job = Mock(pk=54, data=None, log_entries=[])
        logger = Mock()
        logger.log_data = {"logs": [], "statistics": {"dcim.interface": 1000}}

        safe_save_job_data(job, SimpleNamespace(logger=logger))

        logger.flush.assert_called_once_with()
        job.save.assert_called_once_with(update_fields=["data", "log_entries"])
        self.assertEqual(job.data["statistics"]["dcim.interface"], 1000)

    def test_safe_save_job_data_serializes_nested_model_values(self):
        class DummyJob:
            pk = 53

            def __init__(self):
                self.data = None
                self.log_entries = []
                self.saved_update_fields = None

            def save(self, update_fields=None):
                self.saved_update_fields = update_fields

        site = Site.objects.create(name="site-1", slug="site-1")
        job = DummyJob()
        sync_logger = Mock()
        sync_logger.log_data = {
            "logs": [
                [
                    datetime.fromisoformat("2026-05-04T14:00:00+00:00").isoformat(),
                    "success",
                    site,
                    "/plugins/forward/sync/2/",
                    "Synthetic UI harness ingestion completed.",
                ]
            ],
            "statistics": {"dcim.site": {"last_object": site}},
        }
        obj_with_logger = SimpleNamespace(logger=sync_logger)

        safe_save_job_data(job, obj_with_logger)

        self.assertEqual(job.data["logs"][0][2]["model"], "dcim.site")
        self.assertEqual(
            job.data["statistics"]["dcim.site"]["last_object"]["pk"], site.pk
        )
        self.assertEqual(job.saved_update_fields, ["data", "log_entries"])
        sync_logger.flush.assert_called_once_with()

    def test_merge_forwardingestion_rejects_missing_branch_without_attestation(self):
        class DummyJob:
            pk = 56
            object_id = self.ingestion.pk
            user = None
            job_id = "duplicate-merge-job"

            def __init__(self):
                self.data = None
                self.log_entries = []
                self.started = None
                self.terminated_status = None

            def start(self):
                self.started = True

            def terminate(self, status=None):
                self.terminated_status = status

            def save(self, update_fields=None):
                self.saved_update_fields = update_fields

        self.ingestion.baseline_ready = True
        self.ingestion.save(update_fields=["baseline_ready"])
        ForwardSync.objects.filter(pk=self.sync.pk).update(
            status=ForwardSyncStatusChoices.COMPLETED
        )
        job = DummyJob()

        with patch("forward_netbox.utilities.merge.merge_branch") as mock_merge:
            merge_forwardingestion(job)

        mock_merge.assert_called_once()
        self.ingestion.refresh_from_db()
        self.assertIsNone(self.ingestion.merge_job)
        self.assertEqual(job.terminated_status, JobStatusChoices.STATUS_ERRORED)
        self.sync.refresh_from_db()
        self.assertEqual(self.sync.status, ForwardSyncStatusChoices.FAILED)

    def test_merge_forwardingestion_generic_error_logs_reason_and_fails_branch(self):
        branch = Branch.objects.create(
            name=f"merge-generic-error-{uuid4().hex[:12]}",
            schema_id=f"merge_generic_error_{uuid4().hex[:12]}",
        )
        self.ingestion.branch = branch
        self.ingestion.save(update_fields=["branch"])
        branch.status = BranchStatusChoices.MERGING
        branch.save(update_fields=["status", "last_updated"])
        job = Job.objects.create(
            object_type=ContentType.objects.get_for_model(ForwardIngestion),
            object_id=self.ingestion.pk,
            name="merge generic error job",
            user=None,
            status=JobStatusChoices.STATUS_PENDING,
            job_id=uuid4(),
            created=timezone.now(),
            data={},
        )

        with (
            patch.object(
                ForwardIngestion,
                "sync_merge",
                side_effect=RuntimeError("post merge bookkeeping failed"),
            ),
            self.assertRaisesRegex(RuntimeError, "post merge bookkeeping failed"),
        ):
            merge_forwardingestion(job)

        job.refresh_from_db()
        branch.refresh_from_db()
        self.sync.refresh_from_db()
        messages = [entry[4] for entry in (job.data or {}).get("logs", [])]
        self.assertEqual(job.status, JobStatusChoices.STATUS_ERRORED)
        self.assertIn("post merge bookkeeping failed", job.error)
        self.assertEqual(branch.status, BranchStatusChoices.FAILED)
        self.assertEqual(self.sync.status, ForwardSyncStatusChoices.FAILED)
        self.assertTrue(
            any("post merge bookkeeping failed" in message for message in messages)
        )

    def test_merge_forwardingestion_partial_error_remains_retryable(self):
        branch = Branch.objects.create(
            name=f"merge-partial-error-{uuid4().hex[:12]}",
            schema_id=f"merge_partial_error_{uuid4().hex[:12]}",
            status=BranchStatusChoices.READY,
        )
        self.ingestion.branch = branch
        self.ingestion.save(update_fields=["branch"])
        job = Job.objects.create(
            object_type=ContentType.objects.get_for_model(ForwardIngestion),
            object_id=self.ingestion.pk,
            name="merge partial error job",
            user=None,
            status=JobStatusChoices.STATUS_PENDING,
            job_id=uuid4(),
            created=timezone.now(),
            data={},
        )

        def _partial_merge(*args, **kwargs):
            Branch.objects.filter(pk=branch.pk).update(status=BranchStatusChoices.READY)
            raise ForwardPartialMergeError("one merge row failed", applied=4, failed=1)

        with (
            patch.object(
                ForwardIngestion,
                "sync_merge",
                side_effect=_partial_merge,
            ),
            patch("forward_netbox.jobs._enqueue_post_sync_overlays") as overlays,
        ):
            merge_forwardingestion(job)

        job.refresh_from_db()
        branch.refresh_from_db()
        self.sync.refresh_from_db()
        self.assertEqual(job.status, JobStatusChoices.STATUS_ERRORED)
        self.assertIn("one merge row failed", job.error)
        self.assertEqual(branch.status, BranchStatusChoices.READY)
        self.assertEqual(self.sync.status, ForwardSyncStatusChoices.READY_TO_MERGE)
        self.assertTrue(self.ingestion.can_queue_merge)
        overlays.assert_not_called()

    def test_merge_forwardingestion_timeout_remains_retryable(self):
        branch = Branch.objects.create(
            name=f"merge-timeout-{uuid4().hex[:12]}",
            schema_id=f"merge_timeout_{uuid4().hex[:12]}",
            status=BranchStatusChoices.MERGING,
        )
        self.ingestion.branch = branch
        self.ingestion.save(update_fields=["branch"])
        job = Job.objects.create(
            object_type=ContentType.objects.get_for_model(ForwardIngestion),
            object_id=self.ingestion.pk,
            name="merge timeout job",
            user=None,
            status=JobStatusChoices.STATUS_PENDING,
            job_id=uuid4(),
            created=timezone.now(),
            data={},
        )

        with (
            patch.object(
                ForwardIngestion,
                "sync_merge",
                side_effect=JobTimeoutException("merge timed out"),
            ),
            patch("forward_netbox.jobs._enqueue_post_sync_overlays") as overlays,
            self.assertRaisesRegex(JobTimeoutException, "merge timed out"),
        ):
            merge_forwardingestion(job)

        job.refresh_from_db()
        branch.refresh_from_db()
        self.sync.refresh_from_db()
        self.ingestion.refresh_from_db()
        self.assertEqual(job.status, JobStatusChoices.STATUS_ERRORED)
        self.assertIn("timed out", job.error)
        self.assertEqual(branch.status, BranchStatusChoices.READY)
        self.assertEqual(self.sync.status, ForwardSyncStatusChoices.READY_TO_MERGE)
        self.assertTrue(self.ingestion.can_queue_merge)
        self.assertTrue(
            self.ingestion.issues.filter(
                phase=ForwardIngestionPhaseChoices.MERGE,
                exception=JobTimeoutException.__name__,
            ).exists()
        )
        overlays.assert_not_called()

    def test_timeout_reset_uses_authoritative_branch_status(self):
        branch = Branch.objects.create(
            name=f"merge-timeout-stale-cache-{uuid4().hex[:12]}",
            schema_id=f"merge_timeout_stale_cache_{uuid4().hex[:12]}",
            status=BranchStatusChoices.READY,
        )
        self.ingestion.branch = branch
        self.ingestion.save(update_fields=["branch"])
        self.assertEqual(self.ingestion.branch.status, BranchStatusChoices.READY)
        Branch.objects.filter(pk=branch.pk).update(status=BranchStatusChoices.MERGING)

        outcome, status, transitioned = _resolve_authoritative_merge_failure(
            self.ingestion,
            retry_interrupted=True,
        )

        branch.refresh_from_db()
        self.sync.refresh_from_db()
        self.assertEqual(outcome, "retryable")
        self.assertEqual(status, BranchStatusChoices.READY)
        self.assertTrue(transitioned)
        self.assertEqual(branch.status, BranchStatusChoices.READY)
        self.assertEqual(self.sync.status, ForwardSyncStatusChoices.READY_TO_MERGE)

    def test_nonretryable_failure_uses_authoritative_branch_status(self):
        branch = Branch.objects.create(
            name=f"merge-failure-stale-cache-{uuid4().hex[:12]}",
            schema_id=f"merge_failure_stale_cache_{uuid4().hex[:12]}",
            status=BranchStatusChoices.READY,
        )
        self.ingestion.branch = branch
        self.ingestion.save(update_fields=["branch"])
        self.assertEqual(self.ingestion.branch.status, BranchStatusChoices.READY)
        Branch.objects.filter(pk=branch.pk).update(status=BranchStatusChoices.MERGING)

        outcome, status, transitioned = _resolve_authoritative_merge_failure(
            self.ingestion,
            retry_interrupted=False,
        )

        branch.refresh_from_db()
        self.sync.refresh_from_db()
        self.assertEqual(outcome, "failed")
        self.assertEqual(status, BranchStatusChoices.FAILED)
        self.assertTrue(transitioned)
        self.assertEqual(branch.status, BranchStatusChoices.FAILED)
        self.assertEqual(self.sync.status, ForwardSyncStatusChoices.FAILED)

    def test_timeout_reset_does_not_overwrite_authoritative_ready_status(self):
        branch = Branch.objects.create(
            name=f"merge-timeout-race-{uuid4().hex[:12]}",
            schema_id=f"merge_timeout_race_{uuid4().hex[:12]}",
            status=BranchStatusChoices.MERGING,
        )
        self.ingestion.branch = branch
        self.ingestion.save(update_fields=["branch"])
        Branch.objects.filter(pk=branch.pk).update(status=BranchStatusChoices.READY)

        outcome, status, transitioned = _resolve_authoritative_merge_failure(
            self.ingestion,
            retry_interrupted=True,
        )

        branch.refresh_from_db()
        self.sync.refresh_from_db()
        self.assertEqual(outcome, "retryable")
        self.assertEqual(status, BranchStatusChoices.READY)
        self.assertFalse(transitioned)
        self.assertEqual(branch.status, BranchStatusChoices.READY)
        self.assertEqual(self.sync.status, ForwardSyncStatusChoices.READY_TO_MERGE)

    def test_nonretryable_failure_does_not_overwrite_authoritative_ready_status(self):
        branch = Branch.objects.create(
            name=f"merge-failure-race-{uuid4().hex[:12]}",
            schema_id=f"merge_failure_race_{uuid4().hex[:12]}",
            status=BranchStatusChoices.MERGING,
        )
        self.ingestion.branch = branch
        self.ingestion.save(update_fields=["branch"])
        Branch.objects.filter(pk=branch.pk).update(status=BranchStatusChoices.READY)

        outcome, status, transitioned = _resolve_authoritative_merge_failure(
            self.ingestion,
            retry_interrupted=False,
        )

        branch.refresh_from_db()
        self.sync.refresh_from_db()
        self.assertEqual(outcome, "failed")
        self.assertEqual(status, BranchStatusChoices.READY)
        self.assertFalse(transitioned)
        self.assertEqual(branch.status, BranchStatusChoices.READY)
        self.assertEqual(self.sync.status, ForwardSyncStatusChoices.FAILED)

    def test_merge_timeout_does_not_overwrite_concurrent_merged_state(self):
        branch = Branch.objects.create(
            name=f"merge-timeout-merged-race-{uuid4().hex[:12]}",
            schema_id=f"merge_timeout_merged_race_{uuid4().hex[:12]}",
            status=BranchStatusChoices.MERGING,
        )
        self.ingestion.branch = branch
        self.ingestion.save(update_fields=["branch"])
        job = Job.objects.create(
            object_type=ContentType.objects.get_for_model(ForwardIngestion),
            object_id=self.ingestion.pk,
            name="merge timeout merged race job",
            user=None,
            status=JobStatusChoices.STATUS_PENDING,
            job_id=uuid4(),
            created=timezone.now(),
            data={},
        )

        def _merge_then_timeout(*args, **kwargs):
            merged_at = timezone.now()
            with transaction.atomic():
                Branch.objects.filter(pk=branch.pk).update(
                    status=BranchStatusChoices.MERGED
                )
                ForwardIngestion.objects.filter(pk=self.ingestion.pk).update(
                    merge_applied_at=merged_at
                )
            raise JobTimeoutException("merge timed out")

        with (
            patch.object(
                ForwardIngestion,
                "sync_merge",
                side_effect=_merge_then_timeout,
            ),
            patch("forward_netbox.jobs._enqueue_post_sync_overlays") as overlays,
            self.assertRaisesRegex(JobTimeoutException, "merge timed out"),
        ):
            merge_forwardingestion(job)

        job.refresh_from_db()
        branch.refresh_from_db()
        self.sync.refresh_from_db()
        self.ingestion.refresh_from_db()
        self.assertEqual(job.status, JobStatusChoices.STATUS_ERRORED)
        self.assertIn("finalization requires recovery", job.error)
        self.assertEqual(branch.status, BranchStatusChoices.MERGED)
        self.assertEqual(self.sync.status, ForwardSyncStatusChoices.MERGING)
        self.assertIsNotNone(self.ingestion.merge_applied_at)
        self.assertFalse(self.ingestion.baseline_ready)
        overlays.assert_not_called()

    def test_merge_timeout_does_not_overwrite_concurrent_failed_state(self):
        branch = Branch.objects.create(
            name=f"merge-timeout-failed-race-{uuid4().hex[:12]}",
            schema_id=f"merge_timeout_failed_race_{uuid4().hex[:12]}",
            status=BranchStatusChoices.MERGING,
        )
        self.ingestion.branch = branch
        self.ingestion.save(update_fields=["branch"])
        job = Job.objects.create(
            object_type=ContentType.objects.get_for_model(ForwardIngestion),
            object_id=self.ingestion.pk,
            name="merge timeout failed race job",
            user=None,
            status=JobStatusChoices.STATUS_PENDING,
            job_id=uuid4(),
            created=timezone.now(),
            data={},
        )

        def _fail_then_timeout(*args, **kwargs):
            Branch.objects.filter(pk=branch.pk).update(
                status=BranchStatusChoices.FAILED
            )
            raise JobTimeoutException("merge timed out")

        with (
            patch.object(
                ForwardIngestion,
                "sync_merge",
                side_effect=_fail_then_timeout,
            ),
            patch("forward_netbox.jobs._enqueue_post_sync_overlays") as overlays,
            self.assertRaisesRegex(JobTimeoutException, "merge timed out"),
        ):
            merge_forwardingestion(job)

        job.refresh_from_db()
        branch.refresh_from_db()
        self.sync.refresh_from_db()
        self.assertEqual(job.status, JobStatusChoices.STATUS_ERRORED)
        self.assertIn("authoritative branch state is failed", job.error)
        self.assertEqual(branch.status, BranchStatusChoices.FAILED)
        self.assertEqual(self.sync.status, ForwardSyncStatusChoices.FAILED)
        overlays.assert_not_called()

    def test_merge_timeout_preserves_concurrent_finalized_state(self):
        branch = Branch.objects.create(
            name=f"merge-timeout-finalized-race-{uuid4().hex[:12]}",
            schema_id=f"merge_timeout_finalized_race_{uuid4().hex[:12]}",
            status=BranchStatusChoices.MERGING,
        )
        self.ingestion.branch = branch
        self.ingestion.save(update_fields=["branch"])
        job = Job.objects.create(
            object_type=ContentType.objects.get_for_model(ForwardIngestion),
            object_id=self.ingestion.pk,
            name="merge timeout finalized race job",
            user=None,
            status=JobStatusChoices.STATUS_PENDING,
            job_id=uuid4(),
            created=timezone.now(),
            data={},
        )

        def _finalize_then_timeout(*args, **kwargs):
            finalized_at = timezone.now()
            with transaction.atomic():
                Branch.objects.filter(pk=branch.pk).update(
                    status=BranchStatusChoices.MERGED
                )
                ForwardIngestion.objects.filter(pk=self.ingestion.pk).update(
                    merge_applied_at=finalized_at,
                    merge_finalized_at=finalized_at,
                    baseline_ready=True,
                )
                ForwardSync.objects.filter(pk=self.sync.pk).update(
                    status=ForwardSyncStatusChoices.COMPLETED
                )
            raise JobTimeoutException("merge timed out")

        with (
            patch.object(
                ForwardIngestion,
                "sync_merge",
                side_effect=_finalize_then_timeout,
            ),
            patch("forward_netbox.jobs._enqueue_post_sync_overlays") as overlays,
            self.assertRaisesRegex(JobTimeoutException, "merge timed out"),
        ):
            merge_forwardingestion(job)

        job.refresh_from_db()
        branch.refresh_from_db()
        self.sync.refresh_from_db()
        self.ingestion.refresh_from_db()
        self.assertEqual(job.status, JobStatusChoices.STATUS_ERRORED)
        self.assertIn("completed sync state was preserved", job.error)
        self.assertEqual(branch.status, BranchStatusChoices.MERGED)
        self.assertEqual(self.sync.status, ForwardSyncStatusChoices.COMPLETED)
        self.assertTrue(self.ingestion.baseline_ready)
        self.assertIsNotNone(self.ingestion.merge_finalized_at)
        overlays.assert_not_called()

    def test_merge_recovery_preserves_finalization_with_stale_branch_state(self):
        finalized_at = timezone.now()
        ForwardIngestion.objects.filter(pk=self.ingestion.pk).update(
            merge_applied_at=finalized_at,
            merge_finalized_at=finalized_at,
            baseline_ready=True,
        )

        for branch_status in (
            BranchStatusChoices.MERGING,
            BranchStatusChoices.READY,
        ):
            with self.subTest(branch_status=branch_status):
                branch = Branch.objects.create(
                    name=f"finalized-stale-{branch_status}-{uuid4().hex[:12]}",
                    schema_id=f"finalized_stale_{uuid4().hex[:12]}",
                    status=branch_status,
                )
                ForwardIngestion.objects.filter(pk=self.ingestion.pk).update(
                    branch=branch
                )
                ForwardSync.objects.filter(pk=self.sync.pk).update(
                    status=ForwardSyncStatusChoices.MERGING
                )
                self.ingestion.refresh_from_db()

                outcome, status, transitioned = _resolve_authoritative_merge_failure(
                    self.ingestion,
                    retry_interrupted=True,
                )

                branch.refresh_from_db()
                self.sync.refresh_from_db()
                self.assertEqual(outcome, "finalized")
                self.assertEqual(status, branch_status)
                self.assertFalse(transitioned)
                self.assertEqual(branch.status, branch_status)
                self.assertEqual(
                    self.sync.status,
                    ForwardSyncStatusChoices.COMPLETED,
                )

    def test_merge_forwardingestion_readiness_guard_remains_retryable(self):
        branch = Branch.objects.create(
            name=f"merge-readiness-{uuid4().hex[:12]}",
            schema_id=f"merge_readiness_{uuid4().hex[:12]}",
            status=BranchStatusChoices.MERGING,
        )
        self.ingestion.branch = branch
        self.ingestion.save(update_fields=["branch"])
        job = Job.objects.create(
            object_type=ContentType.objects.get_for_model(ForwardIngestion),
            object_id=self.ingestion.pk,
            name="merge readiness job",
            user=None,
            status=JobStatusChoices.STATUS_PENDING,
            job_id=uuid4(),
            created=timezone.now(),
            data={},
        )

        with (
            patch.object(
                ForwardIngestion,
                "sync_merge",
                side_effect=SyncError("Branch is not ready to merge."),
            ),
            patch("forward_netbox.jobs._enqueue_post_sync_overlays") as overlays,
        ):
            merge_forwardingestion(job)

        job.refresh_from_db()
        branch.refresh_from_db()
        self.sync.refresh_from_db()
        self.ingestion.refresh_from_db()
        self.assertEqual(job.status, JobStatusChoices.STATUS_ERRORED)
        self.assertIn("not ready", job.error)
        self.assertEqual(branch.status, BranchStatusChoices.READY)
        self.assertEqual(self.sync.status, ForwardSyncStatusChoices.READY_TO_MERGE)
        self.assertTrue(self.ingestion.can_queue_merge)
        overlays.assert_not_called()

    def test_merge_forwardingestion_uses_job_user_when_sync_user_missing(self):
        branch = Branch.objects.create(
            name=f"merge-user-{uuid4().hex[:12]}",
            schema_id=f"merge_user_{uuid4().hex[:12]}",
        )
        self.ingestion.branch = branch
        self.ingestion.save(update_fields=["branch"])
        self.sync.user = None
        self.sync.save(update_fields=["user"])
        user = get_user_model().objects.create_user(
            username=f"merge-user-{uuid4().hex[:12]}"
        )
        job = Job.objects.create(
            object_type=ContentType.objects.get_for_model(ForwardIngestion),
            object_id=self.ingestion.pk,
            name="merge user fallback job",
            user=user,
            status=JobStatusChoices.STATUS_PENDING,
            job_id=uuid4(),
            created=timezone.now(),
            data={},
        )

        def _assert_request_user(*args, **kwargs):
            request = current_request.get()
            assert request is not None
            assert request.user == user

        with patch.object(
            ForwardIngestion, "sync_merge", side_effect=_assert_request_user
        ):
            merge_forwardingestion(job)


class JobTerminationConcurrencyTest(TransactionTestCase):
    def setUp(self):
        ContentType.objects.clear_cache()
        query_cache.set(None)
        self.user = get_user_model().objects.create_user(
            username="job-termination-owner"
        )
        self.source = ForwardSource.objects.create(
            name="job-termination-source",
            type="saas",
            url="https://fwd.app",
            parameters={
                "username": "user@example.com",
                "password": "secret",
                "verify": True,
                "timeout": 1200,
                "network_id": "job-termination-network",
            },
        )
        self.sync = ForwardSync.objects.create(
            name="job-termination-sync",
            source=self.source,
            user=self.user,
            auto_merge=False,
            parameters={"snapshot_id": "latestProcessed"},
        )
        self.ingestion = ForwardIngestion.objects.create(sync=self.sync)

    def test_merge_recovery_pairs_branch_and_sync_before_competing_merge(self):
        branch = Branch.objects.create(
            name=f"merge-recovery-lock-{uuid4().hex[:12]}",
            schema_id=f"merge_recovery_lock_{uuid4().hex[:12]}",
            status=BranchStatusChoices.MERGING,
        )
        self.ingestion.branch = branch
        self.ingestion.save(update_fields=["branch"])
        ForwardSync.objects.filter(pk=self.sync.pk).update(
            status=ForwardSyncStatusChoices.MERGING
        )
        recovery_holds_locks = threading.Barrier(2)
        release_recovery = threading.Event()
        competitor_started = threading.Event()
        competitor_done = threading.Event()
        errors = []

        from django.db.models.query import QuerySet

        original_update = QuerySet.update

        def guarded_update(queryset, **kwargs):
            if (
                queryset.model is ForwardSync
                and kwargs.get("status") == ForwardSyncStatusChoices.READY_TO_MERGE
            ):
                recovery_holds_locks.wait(timeout=5)
                if not release_recovery.wait(timeout=5):
                    raise AssertionError("recovery release barrier timed out")
            return original_update(queryset, **kwargs)

        def recover():
            close_old_connections()
            try:
                ingestion = ForwardIngestion.objects.get(pk=self.ingestion.pk)
                with patch.object(QuerySet, "update", new=guarded_update):
                    _resolve_authoritative_merge_failure(
                        ingestion,
                        retry_interrupted=True,
                    )
            except Exception as exc:
                errors.append(exc)
            finally:
                close_old_connections()

        def complete_competing_merge():
            close_old_connections()
            try:
                competitor_started.set()
                merged_at = timezone.now()
                with transaction.atomic():
                    with connections["default"].cursor() as cursor:
                        cursor.execute(
                            f"UPDATE {Branch._meta.db_table} SET status = %s "
                            "WHERE id = %s",
                            [BranchStatusChoices.MERGED, branch.pk],
                        )
                        cursor.execute(
                            f"UPDATE {ForwardIngestion._meta.db_table} "
                            "SET merge_applied_at = %s WHERE id = %s",
                            [merged_at, self.ingestion.pk],
                        )
                        cursor.execute(
                            f"UPDATE {ForwardSync._meta.db_table} SET status = %s "
                            "WHERE id = %s",
                            [ForwardSyncStatusChoices.MERGING, self.sync.pk],
                        )
                competitor_done.set()
            except Exception as exc:
                errors.append(exc)
            finally:
                close_old_connections()

        recovery_thread = threading.Thread(target=recover, name="merge-recovery")
        recovery_thread.start()
        recovery_holds_locks.wait(timeout=5)
        competitor_thread = threading.Thread(
            target=complete_competing_merge,
            name="competing-merge",
        )
        competitor_thread.start()
        self.assertTrue(competitor_started.wait(timeout=5))
        self.assertFalse(competitor_done.wait(timeout=0.2))

        release_recovery.set()
        recovery_thread.join(timeout=10)
        competitor_thread.join(timeout=10)

        self.assertFalse(recovery_thread.is_alive())
        self.assertFalse(competitor_thread.is_alive())
        self.assertEqual(errors, [])
        branch.refresh_from_db()
        self.sync.refresh_from_db()
        self.ingestion.refresh_from_db()
        self.assertEqual(branch.status, BranchStatusChoices.MERGED)
        self.assertEqual(self.sync.status, ForwardSyncStatusChoices.MERGING)
        self.assertIsNotNone(self.ingestion.merge_applied_at)

    def _pending_job(
        self,
        *,
        user,
        name,
        func=sync_forwardsync,
        instance=None,
        interval=None,
    ):
        return Job.enqueue(
            func,
            instance=instance or self.sync,
            user=user,
            name=name,
            notifications="never",
            interval=interval,
            adhoc=True,
        )

    def _running_job(self, *, user, name):
        job = self._pending_job(user=user, name=name)
        job.start()
        return job

    def test_concurrent_interval_updates_keep_intent_and_job_aligned(self):
        from forward_netbox.utilities import sync_facade

        first_persisted = threading.Event()
        release_first = threading.Event()
        second_completed = threading.Event()
        errors = []
        original_persist = sync_facade.persist_standing_schedule_interval

        def blocking_persist(sync, kind, interval):
            original_persist(sync, kind, interval)
            if interval == 60:
                first_persisted.set()
                if not release_first.wait(timeout=10):
                    raise TimeoutError("first interval update was not released")

        def update(interval, completed=None):
            close_old_connections()
            try:
                sync = ForwardSync.objects.get(pk=self.sync.pk)
                enqueue_validation_job(sync, user=self.user, interval=interval)
                if completed is not None:
                    completed.set()
            except Exception as exc:  # pragma: no cover - assertion reports detail
                errors.append(exc)
            finally:
                close_old_connections()

        with patch.object(
            sync_facade,
            "persist_standing_schedule_interval",
            side_effect=blocking_persist,
        ):
            first = threading.Thread(target=update, args=(60,))
            first.start()
            self.assertTrue(first_persisted.wait(timeout=10))

            second = threading.Thread(target=update, args=(120, second_completed))
            second.start()
            self.assertFalse(second_completed.wait(timeout=1))

            release_first.set()
            first.join(timeout=10)
            second.join(timeout=10)

        self.assertFalse(first.is_alive())
        self.assertFalse(second.is_alive())
        self.assertEqual(errors, [])
        self.sync.refresh_from_db()
        self.assertEqual(self.sync.parameters["validation_schedule_interval"], 120)
        active = self.sync.jobs.filter(
            name="validation",
            status__in=JobStatusChoices.ENQUEUED_STATE_CHOICES,
        )
        self.assertEqual(active.count(), 1)
        self.assertEqual(active.get().interval, 120)

    def test_interval_update_cannot_enter_terminal_recurrence_window(self):
        parameters = {
            **self.sync.parameters,
            "validation_schedule_interval": 30,
        }
        ForwardSync.objects.filter(pk=self.sync.pk).update(parameters=parameters)
        job = self._pending_job(
            user=self.user,
            name="validation",
            interval=30,
        )
        terminal_written = threading.Event()
        release_terminal = threading.Event()
        update_completed = threading.Event()
        errors = []
        original_terminate = terminate_job_once

        def blocking_terminate(*args, **kwargs):
            result = original_terminate(*args, **kwargs)
            terminal_written.set()
            if not release_terminal.wait(timeout=10):
                raise TimeoutError("terminal recurrence window was not released")
            return result

        def handle():
            close_old_connections()
            try:
                ValidationJob.handle(Job.objects.get(pk=job.pk))
            except Exception as exc:  # pragma: no cover - assertion reports detail
                errors.append(exc)
            finally:
                close_old_connections()

        def update_interval():
            close_old_connections()
            try:
                sync = ForwardSync.objects.get(pk=self.sync.pk)
                enqueue_validation_job(sync, user=self.user, interval=60)
                update_completed.set()
            except Exception as exc:  # pragma: no cover - assertion reports detail
                errors.append(exc)
            finally:
                close_old_connections()

        with patch(
            "forward_netbox.jobs.terminate_job_once",
            side_effect=blocking_terminate,
        ):
            worker = threading.Thread(target=handle)
            worker.start()
            self.assertTrue(terminal_written.wait(timeout=10))
            updater = threading.Thread(target=update_interval)
            updater.start()
            update_entered_gap = update_completed.wait(timeout=1)
            release_terminal.set()
            worker.join(timeout=10)
            updater.join(timeout=10)

        self.assertFalse(update_entered_gap)
        self.assertFalse(worker.is_alive())
        self.assertFalse(updater.is_alive())
        self.assertEqual(errors, [])
        self.sync.refresh_from_db()
        self.assertEqual(self.sync.parameters["validation_schedule_interval"], 60)
        successors = self.sync.jobs.filter(
            name="validation",
            status__in=JobStatusChoices.ENQUEUED_STATE_CHOICES,
        ).exclude(pk=job.pk)
        self.assertEqual(successors.count(), 1)
        self.assertEqual(successors.get().interval, 60)

    def test_reconcile_replaces_running_row_without_live_rq_execution(self):
        parameters = {
            **self.sync.parameters,
            "validation_schedule_interval": 30,
        }
        ForwardSync.objects.filter(pk=self.sync.pk).update(parameters=parameters)
        stale = Job.objects.create(
            object_type=ContentType.objects.get_for_model(ForwardSync),
            object_id=self.sync.pk,
            name="validation",
            user=self.user,
            status=JobStatusChoices.STATUS_RUNNING,
            started=timezone.now(),
            interval=30,
            job_id=uuid4(),
            queue_name="default",
        )

        reconcile_standing_schedules(self.sync, user=self.user)

        stale.refresh_from_db()
        self.assertEqual(stale.status, JobStatusChoices.STATUS_ERRORED)
        self.assertIn("no live RQ execution", stale.error)
        successors = self.sync.jobs.filter(
            name="validation",
            status__in=JobStatusChoices.ENQUEUED_STATE_CHOICES,
        ).exclude(pk=stale.pk)
        self.assertEqual(successors.count(), 1)
        self.assertEqual(successors.get().interval, 30)

    def test_pending_immediate_runner_cannot_start_during_sync_deletion(self):
        job = self._pending_job(
            user=self.user,
            name=f"{self.sync.name} - validation",
        )
        delete_holds_lock = threading.Event()
        release_delete = threading.Event()
        body_started = threading.Event()
        errors = []
        original_delete = Job.delete

        def blocking_delete(job_to_delete, *args, **kwargs):
            if job_to_delete.pk == job.pk and not delete_holds_lock.is_set():
                delete_holds_lock.set()
                if not release_delete.wait(timeout=10):
                    raise TimeoutError("sync deletion was not released")
            return original_delete(job_to_delete, *args, **kwargs)

        def delete_sync():
            close_old_connections()
            try:
                ForwardSync.objects.get(pk=self.sync.pk).delete()
            except Exception as exc:  # pragma: no cover - assertion reports detail
                errors.append(exc)
            finally:
                close_old_connections()

        def handle():
            close_old_connections()
            try:
                ValidationJob.handle(Job.objects.get(pk=job.pk))
            except Job.DoesNotExist:
                pass
            except Exception as exc:  # pragma: no cover - assertion reports detail
                errors.append(exc)
            finally:
                close_old_connections()

        with (
            patch.object(Job, "delete", new=blocking_delete),
            patch(
                "forward_netbox.jobs._validate_forwardsync_work",
                side_effect=lambda _job: body_started.set(),
            ) as work,
        ):
            deleter = threading.Thread(target=delete_sync)
            deleter.start()
            self.assertTrue(delete_holds_lock.wait(timeout=10))
            worker = threading.Thread(target=handle)
            worker.start()
            worker_entered_body = body_started.wait(timeout=1)
            release_delete.set()
            deleter.join(timeout=10)
            worker.join(timeout=10)

        self.assertFalse(worker_entered_body)
        self.assertFalse(deleter.is_alive())
        self.assertFalse(worker.is_alive())
        self.assertEqual(errors, [])
        work.assert_not_called()
        self.assertFalse(ForwardSync.objects.filter(pk=self.sync.pk).exists())
        self.assertFalse(Job.objects.filter(pk=job.pk).exists())

    def test_outer_transaction_enqueue_cannot_dispatch_after_sync_delete(self):
        from forward_netbox.utilities.job_liveness import _rq_job_is_active
        from forward_netbox.signals import acquire_job_schedule_transaction_lock

        job_persisted = threading.Event()
        release_creator_commit = threading.Event()
        deletion_guard_acquired = threading.Event()
        deletion_completed = threading.Event()
        errors = []
        result = {}

        def create_job_inside_outer_transaction():
            close_old_connections()
            try:
                with transaction.atomic():
                    sync = ForwardSync.objects.get(pk=self.sync.pk)
                    ForwardSync.objects.filter(pk=sync.pk).update(
                        status=ForwardSyncStatusChoices.QUEUED
                    )
                    result["job"] = ValidationJob.enqueue(
                        instance=sync,
                        user=self.user,
                        schedule_at=timezone.now() + timedelta(days=30),
                        interval=60,
                    )
                    job_persisted.set()
                    if not release_creator_commit.wait(timeout=10):
                        raise TimeoutError("sync deletion did not finish")
            except Exception as exc:  # pragma: no cover - assertion reports detail
                errors.append(exc)
            finally:
                connections.close_all()

        def delete_sync_before_creator_commit():
            close_old_connections()
            try:
                if not job_persisted.wait(timeout=10):
                    raise TimeoutError("job was not persisted")
                ForwardSync.objects.get(pk=self.sync.pk).delete()
                deletion_completed.set()
            except Exception as exc:  # pragma: no cover - assertion reports detail
                errors.append(exc)
            finally:
                connections.close_all()

        def observed_delete_guard():
            acquire_job_schedule_transaction_lock()
            deletion_guard_acquired.set()

        creator = threading.Thread(target=create_job_inside_outer_transaction)
        deleter = threading.Thread(target=delete_sync_before_creator_commit)
        creator.start()
        self.assertTrue(job_persisted.wait(timeout=10))
        with patch(
            "forward_netbox.signals.acquire_job_schedule_transaction_lock",
            side_effect=observed_delete_guard,
        ):
            deleter.start()
            self.assertTrue(deletion_guard_acquired.wait(timeout=10))
            self.assertFalse(deletion_completed.is_set())
            release_creator_commit.set()
            creator.join(timeout=10)
            deleter.join(timeout=10)

        self.assertFalse(creator.is_alive())
        self.assertFalse(deleter.is_alive())
        self.assertTrue(deletion_completed.is_set())
        self.assertEqual(errors, [])
        job = result["job"]
        self.assertFalse(ForwardSync.objects.filter(pk=self.sync.pk).exists())
        self.assertFalse(Job.objects.filter(pk=job.pk).exists())
        self.assertIs(_rq_job_is_active(job), False)

    def test_recovery_terminal_state_wins_before_worker_termination_lock(self):
        job = self._running_job(user=None, name="recovery-first")
        recovery_locked = threading.Event()
        worker_started = threading.Event()
        release_recovery = threading.Event()
        errors = []
        results = {}

        def recover():
            close_old_connections()
            try:
                with transaction.atomic():
                    Job.objects.select_for_update().get(pk=job.pk)
                    _terminal_mark([job.pk])
                    recovery_locked.set()
                    if not release_recovery.wait(timeout=10):
                        raise TimeoutError("worker did not attempt termination")
            except Exception as exc:  # pragma: no cover - assertion reports detail
                errors.append(exc)
            finally:
                close_old_connections()

        def terminate():
            close_old_connections()
            try:
                if not recovery_locked.wait(timeout=10):
                    raise TimeoutError("recovery did not acquire the job lock")
                worker_started.set()
                results["terminated"] = terminate_job_once(
                    Job.objects.get(pk=job.pk),
                    status=JobStatusChoices.STATUS_COMPLETED,
                )
            except Exception as exc:  # pragma: no cover - assertion reports detail
                errors.append(exc)
            finally:
                close_old_connections()

        recovery_thread = threading.Thread(target=recover)
        worker_thread = threading.Thread(target=terminate)
        recovery_thread.start()
        self.assertTrue(recovery_locked.wait(timeout=10))
        worker_thread.start()
        self.assertTrue(worker_started.wait(timeout=10))
        release_recovery.set()
        for thread in (recovery_thread, worker_thread):
            thread.join(timeout=10)

        self.assertFalse(
            any(thread.is_alive() for thread in (recovery_thread, worker_thread))
        )
        self.assertEqual(errors, [])
        self.assertFalse(results["terminated"])
        job.refresh_from_db()
        self.assertEqual(job.status, JobStatusChoices.STATUS_FAILED)

    def test_worker_terminal_state_wins_before_recovery_update(self):
        job = self._running_job(user=self.user, name="worker-first")
        worker_locked = threading.Event()
        recovery_started = threading.Event()
        release_worker = threading.Event()
        errors = []

        def terminate():
            close_old_connections()
            try:
                with transaction.atomic():
                    locked_job = Job.objects.select_for_update().get(pk=job.pk)
                    self.assertTrue(
                        terminate_job_once(
                            locked_job,
                            status=JobStatusChoices.STATUS_COMPLETED,
                        )
                    )
                    worker_locked.set()
                    if not release_worker.wait(timeout=10):
                        raise TimeoutError("recovery did not attempt its update")
            except Exception as exc:  # pragma: no cover - assertion reports detail
                errors.append(exc)
            finally:
                close_old_connections()

        def recover():
            close_old_connections()
            try:
                if not worker_locked.wait(timeout=10):
                    raise TimeoutError("worker did not acquire the job lock")
                recovery_started.set()
                _terminal_mark([job.pk])
            except Exception as exc:  # pragma: no cover - assertion reports detail
                errors.append(exc)
            finally:
                close_old_connections()

        worker_thread = threading.Thread(target=terminate)
        recovery_thread = threading.Thread(target=recover)
        worker_thread.start()
        self.assertTrue(worker_locked.wait(timeout=10))
        recovery_thread.start()
        self.assertTrue(recovery_started.wait(timeout=10))
        release_worker.set()
        for thread in (worker_thread, recovery_thread):
            thread.join(timeout=10)

        self.assertFalse(
            any(thread.is_alive() for thread in (worker_thread, recovery_thread))
        )
        self.assertEqual(errors, [])
        job.refresh_from_db()
        self.assertEqual(job.status, JobStatusChoices.STATUS_COMPLETED)

    def test_runner_handler_preserves_recovery_terminal_state(self):
        job = self._pending_job(
            user=None,
            name="runner-recovery-first",
            interval=30,
        )
        job_count = Job.objects.count()
        run_started = threading.Event()
        release_run = threading.Event()
        errors = []

        class BlockingRunner(ForwardJobRunner):
            def run(self, *args, **kwargs):
                run_started.set()
                if not release_run.wait(timeout=10):
                    raise TimeoutError("recovery did not complete")
                raise RuntimeError("runner exploded after recovery")

        def handle():
            close_old_connections()
            try:
                BlockingRunner.handle(Job.objects.get(pk=job.pk))
            except Exception as exc:  # pragma: no cover - assertion reports detail
                errors.append(exc)
            finally:
                close_old_connections()

        worker_thread = threading.Thread(target=handle)
        worker_thread.start()
        self.assertTrue(run_started.wait(timeout=10))
        _terminal_mark([job.pk])
        job.refresh_from_db()
        recovery_completed = job.completed
        recovery_error = job.error
        release_run.set()
        worker_thread.join(timeout=10)

        self.assertFalse(worker_thread.is_alive())
        self.assertEqual(errors, [])
        job.refresh_from_db()
        self.assertEqual(job.status, JobStatusChoices.STATUS_FAILED)
        self.assertEqual(job.completed, recovery_completed)
        self.assertEqual(job.error, recovery_error)
        self.assertIn(
            "runner exploded after recovery", job.data["worker_terminal_error"]
        )
        self.assertTrue(job.log_entries)
        self.assertEqual(Job.objects.count(), job_count)

    def test_runner_handler_rethrows_timeout_after_persisting_terminal_state(self):
        job = self._pending_job(user=None, name="runner-timeout")

        class TimeoutRunner(ForwardJobRunner):
            def run(self, *args, **kwargs):
                raise JobTimeoutException("runner timed out")

        with self.assertRaisesRegex(JobTimeoutException, "runner timed out"):
            TimeoutRunner.handle(Job.objects.get(pk=job.pk))

        job.refresh_from_db()
        self.assertEqual(job.status, JobStatusChoices.STATUS_ERRORED)
        self.assertIsNotNone(job.completed)
        self.assertIn("runner timed out", job.error)
        self.assertTrue(job.log_entries)

    def test_runner_handler_terminal_state_wins_before_recovery_update(self):
        job = self._pending_job(user=None, name="runner-worker-first")
        worker_terminated = threading.Event()
        release_handler = threading.Event()
        errors = []
        original_terminate = terminate_job_once

        class ImmediateRunner(ForwardJobRunner):
            def run(self, *args, **kwargs):
                return None

        def observed_terminate(*args, **kwargs):
            result = original_terminate(*args, **kwargs)
            worker_terminated.set()
            if not release_handler.wait(timeout=10):
                raise TimeoutError("recovery did not complete")
            return result

        def handle():
            close_old_connections()
            try:
                with patch(
                    "forward_netbox.jobs.terminate_job_once",
                    side_effect=observed_terminate,
                ):
                    ImmediateRunner.handle(Job.objects.get(pk=job.pk))
            except Exception as exc:  # pragma: no cover - assertion reports detail
                errors.append(exc)
            finally:
                close_old_connections()

        worker_thread = threading.Thread(target=handle)
        worker_thread.start()
        self.assertTrue(worker_terminated.wait(timeout=10))
        _terminal_mark([job.pk])
        release_handler.set()
        worker_thread.join(timeout=10)

        self.assertFalse(worker_thread.is_alive())
        self.assertEqual(errors, [])
        job.refresh_from_db()
        self.assertEqual(job.status, JobStatusChoices.STATUS_COMPLETED)

    def test_runner_handler_executes_body_once_for_duplicate_delivery(self):
        job = self._pending_job(user=None, name="runner-duplicate-delivery")
        body_started = threading.Event()
        release_body = threading.Event()
        body_calls = []
        errors = []

        class CountingRunner(ForwardJobRunner):
            def run(self, *args, **kwargs):
                body_calls.append(self.job.pk)
                body_started.set()
                if not release_body.wait(timeout=10):
                    raise TimeoutError("duplicate handler did not return")

        def handle():
            close_old_connections()
            try:
                CountingRunner.handle(Job.objects.get(pk=job.pk))
            except Exception as exc:  # pragma: no cover - assertion reports detail
                errors.append(exc)
            finally:
                close_old_connections()

        first = threading.Thread(target=handle)
        second = threading.Thread(target=handle)
        first.start()
        self.assertTrue(body_started.wait(timeout=10))
        second.start()
        second.join(timeout=10)
        self.assertFalse(second.is_alive())
        release_body.set()
        first.join(timeout=10)

        self.assertFalse(first.is_alive())
        self.assertEqual(errors, [])
        self.assertEqual(body_calls, [job.pk])
        job.refresh_from_db()
        self.assertEqual(job.status, JobStatusChoices.STATUS_COMPLETED)

    def test_sync_handler_does_not_revive_recovery_terminal_job_or_recur(self):
        ForwardSync.objects.filter(pk=self.sync.pk).update(
            user=None,
            interval=30,
            scheduled=timezone.now(),
        )
        self.sync.refresh_from_db()
        original_scheduled = self.sync.scheduled
        job = self._pending_job(user=None, name="sync-recovery-before-start")
        stale_job = Job.objects.get(pk=job.pk)
        _terminal_mark([job.pk])
        job.refresh_from_db()
        recovery_state = (job.status, job.completed, job.error)
        job_count = Job.objects.count()

        with (
            patch.object(ForwardSync, "sync") as sync_work,
            patch(
                "forward_netbox.utilities.sync_facade.reconcile_standing_schedules"
            ) as reconcile,
        ):
            sync_forwardsync(stale_job, adhoc=False)

        sync_work.assert_not_called()
        reconcile.assert_not_called()
        job.refresh_from_db()
        self.sync.refresh_from_db()
        self.assertEqual((job.status, job.completed, job.error), recovery_state)
        self.assertEqual(self.sync.scheduled, original_scheduled)
        self.assertEqual(Job.objects.count(), job_count)

    def test_running_validation_duplicate_preserves_row_and_new_interval(self):
        parameters = {
            **self.sync.parameters,
            "validation_schedule_interval": 30,
        }
        ForwardSync.objects.filter(pk=self.sync.pk).update(parameters=parameters)
        job = self._pending_job(
            user=self.user,
            name="validation",
            interval=30,
        )
        body_started = threading.Event()
        release_body = threading.Event()
        errors = []

        def work(_job):
            body_started.set()
            if not release_body.wait(timeout=10):
                raise TimeoutError("duplicate delivery did not return")

        def handle():
            close_old_connections()
            try:
                ValidationJob.handle(Job.objects.get(pk=job.pk))
            except Exception as exc:  # pragma: no cover - assertion reports detail
                errors.append(exc)
            finally:
                close_old_connections()

        with (
            patch("forward_netbox.jobs._validate_forwardsync_work", side_effect=work),
            patch(
                "forward_netbox.utilities.job_liveness.job_has_live_execution",
                return_value=True,
            ),
        ):
            first = threading.Thread(target=handle)
            first.start()
            self.assertTrue(body_started.wait(timeout=10))
            kept = enqueue_validation_job(
                self.sync,
                user=self.user,
                interval=60,
            )
            self.assertEqual(kept.pk, job.pk)

            second = threading.Thread(target=handle)
            second.start()
            second.join(timeout=10)
            self.assertFalse(second.is_alive())
            self.assertTrue(Job.objects.filter(pk=job.pk, status="running").exists())

            release_body.set()
            first.join(timeout=10)

        self.assertFalse(first.is_alive())
        self.assertEqual(errors, [])
        successors = self.sync.jobs.filter(
            name="validation",
            status__in=JobStatusChoices.ENQUEUED_STATE_CHOICES,
        ).exclude(pk=job.pk)
        self.assertEqual(successors.count(), 1)
        successor = successors.get()
        self.assertEqual(successor.interval, 60)
        self.assertEqual(successor.status, JobStatusChoices.STATUS_SCHEDULED)
        self.assertGreater(successor.scheduled, timezone.now())

    def test_running_preview_schedule_update_preserves_row_and_new_interval(self):
        parameters = {
            **self.sync.parameters,
            "preview_schedule_interval": 30,
        }
        ForwardSync.objects.filter(pk=self.sync.pk).update(parameters=parameters)
        job = self._pending_job(
            user=self.user,
            name="dependency preview",
            interval=30,
        )
        body_started = threading.Event()
        release_body = threading.Event()
        errors = []

        def work(_job):
            body_started.set()
            if not release_body.wait(timeout=10):
                raise TimeoutError("schedule update did not return")

        def handle():
            close_old_connections()
            try:
                DependencyPreviewJob.handle(Job.objects.get(pk=job.pk))
            except Exception as exc:  # pragma: no cover - assertion reports detail
                errors.append(exc)
            finally:
                close_old_connections()

        with (
            patch("forward_netbox.jobs._dependency_preview_work", side_effect=work),
            patch(
                "forward_netbox.utilities.job_liveness.job_has_live_execution",
                return_value=True,
            ),
        ):
            worker = threading.Thread(target=handle)
            worker.start()
            self.assertTrue(body_started.wait(timeout=10))
            kept = enqueue_preview_schedule(
                self.sync,
                user=self.user,
                interval=60,
            )
            self.assertEqual(kept.pk, job.pk)
            self.assertTrue(
                Job.objects.filter(
                    pk=job.pk,
                    status=JobStatusChoices.STATUS_RUNNING,
                ).exists()
            )
            release_body.set()
            worker.join(timeout=10)

        self.assertFalse(worker.is_alive())
        self.assertEqual(errors, [])
        successors = self.sync.jobs.filter(
            name="dependency preview",
            status__in=JobStatusChoices.ENQUEUED_STATE_CHOICES,
        ).exclude(pk=job.pk)
        self.assertEqual(successors.count(), 1)
        successor = successors.get()
        self.assertEqual(successor.interval, 60)
        self.assertEqual(successor.status, JobStatusChoices.STATUS_SCHEDULED)
        self.assertGreater(successor.scheduled, timezone.now())

    def test_validation_recovery_during_body_reconciles_one_successor(self):
        parameters = {
            **self.sync.parameters,
            "validation_schedule_interval": 30,
        }
        ForwardSync.objects.filter(pk=self.sync.pk).update(parameters=parameters)
        job = self._pending_job(
            user=self.user,
            name="validation",
            interval=30,
        )
        body_started = threading.Event()
        release_body = threading.Event()
        errors = []

        def work(_job):
            body_started.set()
            if not release_body.wait(timeout=10):
                raise TimeoutError("recovery did not release the worker")

        def handle():
            close_old_connections()
            try:
                ValidationJob.handle(Job.objects.get(pk=job.pk))
            except Exception as exc:  # pragma: no cover - assertion reports detail
                errors.append(exc)
            finally:
                close_old_connections()

        with patch("forward_netbox.jobs._validate_forwardsync_work", side_effect=work):
            worker = threading.Thread(target=handle)
            worker.start()
            self.assertTrue(body_started.wait(timeout=10))
            _terminal_mark([job.pk])
            terminal_job = Job.objects.get(pk=job.pk)
            recovery_state = (
                terminal_job.status,
                terminal_job.completed,
                terminal_job.error,
            )
            ValidationJob.handle(Job.objects.get(pk=job.pk))
            ValidationJob.handle(Job.objects.get(pk=job.pk))
            release_body.set()
            worker.join(timeout=10)

        self.assertFalse(worker.is_alive())
        self.assertEqual(errors, [])
        terminal_job.refresh_from_db()
        self.assertEqual(
            (terminal_job.status, terminal_job.completed, terminal_job.error),
            recovery_state,
        )
        successors = self.sync.jobs.filter(
            name="validation",
            status__in=JobStatusChoices.ENQUEUED_STATE_CHOICES,
        ).exclude(pk=job.pk)
        self.assertEqual(successors.count(), 1)
        self.assertEqual(successors.get().interval, 30)

    def test_merge_handler_does_not_revive_recovery_terminal_job(self):
        self.sync.user = None
        self.sync.save(update_fields=["user"])
        job = self._pending_job(
            user=None,
            name="merge-recovery-before-start",
            func=merge_forwardingestion,
            instance=self.ingestion,
        )
        stale_job = Job.objects.get(pk=job.pk)
        _terminal_mark([job.pk])
        job.refresh_from_db()
        recovery_state = (job.status, job.completed, job.error)

        with (
            patch.object(ForwardIngestion, "sync_merge") as merge_work,
            patch("forward_netbox.jobs._claim_ingestion_merge_job") as claim,
        ):
            merge_forwardingestion(stale_job)

        merge_work.assert_not_called()
        claim.assert_not_called()
        job.refresh_from_db()
        self.assertEqual((job.status, job.completed, job.error), recovery_state)

    def test_recovery_markers_leave_terminal_jobs_unchanged(self):
        job = self._running_job(user=None, name="terminal-marker-noop")
        job.terminate(status=JobStatusChoices.STATUS_ERRORED)

        _terminal_mark([job.pk])
        _completed_mark([job.pk])

        job.refresh_from_db()
        self.assertEqual(job.status, JobStatusChoices.STATUS_ERRORED)
