from datetime import datetime
from types import SimpleNamespace
from unittest.mock import patch
from uuid import uuid4

from core.choices import JobStatusChoices
from core.models import Job
from dcim.models import Site
from django.contrib.auth import get_user_model
from django.contrib.contenttypes.models import ContentType
from django.test import TestCase
from django.utils import timezone
from netbox.context import current_request
from netbox_branching.choices import BranchStatusChoices
from netbox_branching.models import Branch
from rq.timeouts import JobTimeoutException

from forward_netbox.choices import ForwardIngestionPhaseChoices
from forward_netbox.choices import ForwardSyncStatusChoices
from forward_netbox.jobs import merge_forwardingestion
from forward_netbox.jobs import record_timeout_issue
from forward_netbox.jobs import safe_save_job_data
from forward_netbox.models import ForwardIngestion
from forward_netbox.models import ForwardIngestionIssue
from forward_netbox.models import ForwardSource
from forward_netbox.models import ForwardSync


class ForwardJobsTest(TestCase):
    def setUp(self):
        self.addCleanup(current_request.set, None)
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
            auto_merge=False,
            parameters={"snapshot_id": "latestProcessed"},
        )
        self.ingestion = ForwardIngestion.objects.create(sync=self.sync)

    def test_auto_tag_backfilled_enqueues_only_when_enabled(self):
        from forward_netbox.jobs import _maybe_enqueue_backfilled_tag_refresh

        # Disabled by default: no enqueue.
        with patch("forward_netbox.jobs.Job.enqueue") as enqueue:
            _maybe_enqueue_backfilled_tag_refresh(self.sync)
            enqueue.assert_not_called()

        # Opt-in via the auto_tag_backfilled parameter: enqueues the tag job.
        self.sync.parameters = {**self.sync.parameters, "auto_tag_backfilled": True}
        with patch("forward_netbox.jobs.Job.enqueue") as enqueue:
            _maybe_enqueue_backfilled_tag_refresh(self.sync)
            enqueue.assert_called_once()
            self.assertEqual(enqueue.call_args.kwargs["instance"], self.sync)

    def test_vsys_parent_link_enqueues_by_default_and_respects_opt_out(self):
        from forward_netbox.jobs import _maybe_enqueue_vsys_parent_link

        # DEFAULT-ON: no parameter -> the parent-link overlay enqueues (a blank
        # Parent Device on every vsys/vdom is a confusing default).
        with patch("forward_netbox.jobs.Job.enqueue") as enqueue:
            _maybe_enqueue_vsys_parent_link(self.sync)
            enqueue.assert_called_once()
            self.assertEqual(enqueue.call_args.kwargs["instance"], self.sync)

        # Explicit opt-out with auto_link_vsys_parents=False -> no enqueue.
        self.sync.parameters = {
            **self.sync.parameters,
            "auto_link_vsys_parents": False,
        }
        with patch("forward_netbox.jobs.Job.enqueue") as enqueue:
            _maybe_enqueue_vsys_parent_link(self.sync)
            enqueue.assert_not_called()

    def test_auto_prune_orphans_enqueues_only_when_enabled(self):
        from forward_netbox.jobs import _maybe_enqueue_auto_prune

        # OFF by default (it deletes NetBox data): no enqueue.
        with patch("forward_netbox.jobs.Job.enqueue") as enqueue:
            _maybe_enqueue_auto_prune(self.sync)
            enqueue.assert_not_called()

        # Opt-in via auto_prune_orphans: enqueues the prune job.
        self.sync.parameters = {**self.sync.parameters, "auto_prune_orphans": True}
        with patch("forward_netbox.jobs.Job.enqueue") as enqueue:
            _maybe_enqueue_auto_prune(self.sync)
            enqueue.assert_called_once()
            self.assertIn("prune orphans", enqueue.call_args.kwargs["name"])

    def test_overlay_enqueue_skips_when_active_job_exists(self):
        # Regression (hung-pending pile-up): the default-on vsys overlay enqueues
        # after every sync; if one is still pending/running it must NOT enqueue a
        # duplicate that stacks up behind it.
        from django.contrib.contenttypes.models import ContentType

        from forward_netbox.jobs import _maybe_enqueue_vsys_parent_link

        name = f"{self.sync.name} - link vsys/vdom parents (auto)"
        Job.objects.create(
            object_type=ContentType.objects.get_for_model(ForwardSync),
            object_id=self.sync.pk,
            name=name,
            status=JobStatusChoices.STATUS_PENDING,
            job_id=uuid4(),
        )
        with patch("forward_netbox.jobs.Job.enqueue") as enqueue:
            _maybe_enqueue_vsys_parent_link(self.sync)
            enqueue.assert_not_called()

        # A completed job of the same name does NOT block a fresh enqueue.
        Job.objects.filter(object_id=self.sync.pk, name=name).update(
            status=JobStatusChoices.STATUS_COMPLETED
        )
        with patch("forward_netbox.jobs.Job.enqueue") as enqueue:
            _maybe_enqueue_vsys_parent_link(self.sync)
            enqueue.assert_called_once()

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
        obj_with_logger = SimpleNamespace(
            logger=SimpleNamespace(
                log_data={
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
            )
        )

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
        obj_with_logger = SimpleNamespace(
            logger=SimpleNamespace(
                log_data={
                    "logs": [
                        [
                            datetime.fromisoformat(
                                "2026-05-04T14:00:00+00:00"
                            ).isoformat(),
                            "success",
                            site,
                            "/plugins/forward/sync/2/",
                            "Synthetic UI harness ingestion completed.",
                        ]
                    ],
                    "statistics": {"dcim.site": {"last_object": site}},
                }
            )
        )

        safe_save_job_data(job, obj_with_logger)

        self.assertEqual(job.data["logs"][0][2]["model"], "dcim.site")
        self.assertEqual(
            job.data["statistics"]["dcim.site"]["last_object"]["pk"], site.pk
        )
        self.assertEqual(job.saved_update_fields, ["data", "log_entries"])

    def test_merge_forwardingestion_skips_duplicate_merge_without_branch(self):
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
        job = DummyJob()

        with patch("forward_netbox.utilities.merge.merge_branch") as mock_merge:
            merge_forwardingestion(job)

        mock_merge.assert_not_called()
        self.ingestion.refresh_from_db()
        self.assertIsNone(self.ingestion.merge_job)
        self.assertIsNone(job.terminated_status)
        self.assertEqual(
            job.data["logs"][0][4],
            "Forward ingestion branch is already merged or no longer present; skipping duplicate merge job.",
        )

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
        self.assertEqual(branch.status, BranchStatusChoices.FAILED)
        self.assertEqual(self.sync.status, ForwardSyncStatusChoices.FAILED)
        self.assertTrue(
            any("post merge bookkeeping failed" in message for message in messages)
        )

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
