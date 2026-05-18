import json

from core.choices import JobStatusChoices
from core.models import Job
from django.contrib.auth import get_user_model
from django.contrib.contenttypes.models import ContentType
from django.test import TestCase
from django.urls import reverse
from django.utils import timezone
from netbox_branching.models import Branch

from forward_netbox.models import ForwardExecutionRun
from forward_netbox.models import ForwardExecutionStep
from forward_netbox.models import ForwardIngestion
from forward_netbox.models import ForwardSource
from forward_netbox.models import ForwardSync
from forward_netbox.utilities.execution_ledger import active_execution_run


class ForwardIngestionLogExportViewTest(TestCase):
    @classmethod
    def setUpTestData(cls):
        User = get_user_model()
        cls.user = User.objects.create_superuser(
            username="log-export-admin",
            password="TestPassword123!",
            email="admin@example.com",
        )
        cls.source = ForwardSource.objects.create(
            name="source-log-export",
            type="saas",
            url="https://fwd.app",
            parameters={
                "username": "user@example.com",
                "password": "secret",
                "verify": True,
                "network_id": "test-network",
            },
        )
        cls.sync = ForwardSync.objects.create(
            name="sync-log-export",
            source=cls.source,
            parameters={"snapshot_id": "latestProcessed"},
        )
        cls.ingestion = ForwardIngestion.objects.create(
            sync=cls.sync,
            snapshot_selector="latestProcessed",
            snapshot_id="snapshot-1",
        )

        now = timezone.now()
        cls.job = Job.objects.create(
            object_type=ContentType.objects.get_for_model(ForwardIngestion),
            object_id=cls.ingestion.pk,
            name="ingestion-log-export-job",
            user=None,
            status=JobStatusChoices.STATUS_COMPLETED,
            job_id="123e4567-e89b-12d3-a456-426614174000",
            created=now,
            started=now,
            completed=now,
            data={
                "logs": [
                    [
                        now.isoformat(),
                        "success",
                        "sync-log-export",
                        "/plugins/forward/ingestion/1/",
                        "Synthetic sync stage completed.",
                    ]
                ],
                "statistics": {"dcim.site": {"current": 1, "total": 1}},
            },
        )
        cls.merge_job = Job.objects.create(
            object_type=ContentType.objects.get_for_model(ForwardIngestion),
            object_id=cls.ingestion.pk,
            name="ingestion-log-export-merge-job",
            user=None,
            status=JobStatusChoices.STATUS_COMPLETED,
            job_id="223e4567-e89b-12d3-a456-426614174001",
            created=now,
            started=now,
            completed=now,
            data={
                "logs": [
                    [
                        now.isoformat(),
                        "failure",
                        "sync-log-export",
                        "/plugins/forward/ingestion/1/",
                        "Synthetic merge stage failed.",
                    ]
                ],
                "statistics": {"dcim.site": {"current": 0, "total": 1}},
            },
        )
        cls.job.log_entries = [
            {
                "timestamp": now,
                "level": "info",
                "message": "Synthetic sync stage completed.",
            }
        ]
        cls.job.save(update_fields=["log_entries"])
        cls.merge_job.log_entries = [
            {
                "timestamp": now,
                "level": "error",
                "message": "Synthetic merge stage failed.",
            }
        ]
        cls.merge_job.save(update_fields=["log_entries"])
        cls.ingestion.job = cls.job
        cls.ingestion.merge_job = cls.merge_job
        cls.ingestion.save(update_fields=["job", "merge_job"])
        cls.execution_run = ForwardExecutionRun.objects.create(
            sync=cls.sync,
            source=cls.source,
            job=cls.job,
            backend="branching",
            status="running",
            snapshot_selector="latestProcessed",
            snapshot_id="snapshot-1",
            total_steps=1,
            next_step_index=1,
        )
        cls.execution_step = ForwardExecutionStep.objects.create(
            run=cls.execution_run,
            index=1,
            status="merge_queued",
            model_string="dcim.site",
            query_name="Forward Sites",
            execution_mode="query_id",
            execution_value="query-1",
            query_parameters={"forward_netbox_shard_keys": ["device-1"]},
            ingestion=cls.ingestion,
            job=cls.job,
            merge_job=cls.merge_job,
            estimated_changes=1,
        )
        cls.sync.set_branch_run_state(
            {
                "execution_run_id": cls.execution_run.pk,
                "next_plan_index": 1,
                "total_plan_items": 1,
            }
        )

    def test_export_logs_downloads_json_bundle(self):
        self.client.force_login(self.user)

        response = self.client.get(
            reverse(
                "plugins:forward_netbox:forwardingestion_export_logs",
                kwargs={"pk": self.ingestion.pk},
            )
            + "?stage=merge"
        )

        self.assertEqual(response.status_code, 200)
        self.assertIn("attachment;", response["Content-Disposition"])
        self.assertIn("forward-ingestion-", response["Content-Disposition"])

        data = json.loads(response.content)
        self.assertEqual(data["active_stage"], "merge")
        self.assertEqual(data["ingestion"]["pk"], self.ingestion.pk)
        self.assertEqual(data["ingestion"]["job"]["pk"], self.job.pk)
        self.assertEqual(data["ingestion"]["merge_job"]["pk"], self.merge_job.pk)
        self.assertEqual(data["sync"]["execution_state_source"], "execution_ledger")
        self.assertIn("execution_plan", data)
        self.assertEqual(data["execution_run"]["run"]["id"], self.execution_run.pk)
        self.assertEqual(data["execution_run"]["steps"][0]["job"], self.job.pk)
        self.assertEqual(
            data["execution_run"]["steps"][0]["merge_job"], self.merge_job.pk
        )
        self.assertEqual(
            data["execution_run"]["steps"][0]["job_detail"]["pk"], self.job.pk
        )
        self.assertEqual(
            data["job_results"]["logs"][0][4], "Synthetic sync stage completed."
        )
        self.assertEqual(
            data["merge_job_results"]["logs"][0][4],
            "Synthetic merge stage failed.",
        )
        self.assertEqual(data["job_results"]["statistics"]["dcim.site"]["total"], 1)
        self.assertEqual(
            data["merge_job_results"]["statistics"]["dcim.site"]["total"], 1
        )

    def test_export_logs_uses_ledger_branch_state_when_cache_is_absent(self):
        self.sync.clear_branch_run_state()
        self.client.force_login(self.user)

        response = self.client.get(
            reverse(
                "plugins:forward_netbox:forwardingestion_export_logs",
                kwargs={"pk": self.ingestion.pk},
            )
        )

        self.assertEqual(response.status_code, 200)
        data = json.loads(response.content)
        self.assertEqual(
            data["sync"]["execution_state"]["execution_run_id"],
            self.execution_run.pk,
        )
        self.assertEqual(data["sync"]["execution_state_source"], "execution_ledger")
        self.assertTrue(data["sync"]["execution_state"]["state_synthesized"])
        self.assertEqual(data["execution_plan"]["total_plan_items"], 1)
        self.assertEqual(data["execution_run"]["run"]["id"], self.execution_run.pk)

    def test_execution_run_support_bundle_downloads_json_bundle(self):
        self.client.force_login(self.user)

        response = self.client.get(
            reverse(
                "plugins:forward_netbox:forwardexecutionrun_export_bundle",
                kwargs={"pk": self.execution_run.pk},
            )
        )

        self.assertEqual(response.status_code, 200)
        self.assertIn("attachment;", response["Content-Disposition"])
        self.assertIn(
            f"forward-execution-run-{self.execution_run.pk}-support-bundle.json",
            response["Content-Disposition"],
        )

        data = json.loads(response.content)
        self.assertEqual(data["run"]["id"], self.execution_run.pk)
        self.assertEqual(data["steps"][0]["id"], self.execution_step.pk)
        self.assertEqual(
            data["steps"][0]["query_parameters"],
            {"forward_netbox_shard_keys": ["device-1"]},
        )
        self.assertEqual(data["steps"][0]["job_detail"]["pk"], self.job.pk)
        self.assertEqual(
            data["steps"][0]["ingestion_detail"]["id"],
            self.ingestion.pk,
        )

    def test_sync_support_bundle_downloads_json_bundle(self):
        self.client.force_login(self.user)

        response = self.client.get(
            reverse(
                "plugins:forward_netbox:forwardsync_support_bundle",
                kwargs={"pk": self.sync.pk},
            )
        )

        self.assertEqual(response.status_code, 200)
        self.assertIn("attachment;", response["Content-Disposition"])
        self.assertIn(
            f"forward-sync-{self.sync.pk}-support-bundle.json",
            response["Content-Disposition"],
        )

        data = json.loads(response.content)
        self.assertEqual(data["sync"]["pk"], self.sync.pk)
        self.assertEqual(data["sync"]["execution_state_source"], "execution_ledger")
        self.assertEqual(data["latest_ingestion"]["pk"], self.ingestion.pk)
        self.assertEqual(data["execution_run"]["run"]["id"], self.execution_run.pk)
        self.assertEqual(data["health"]["source"]["name"], self.source.name)
        self.assertEqual(
            data["execution_run"]["steps"][0]["merge_job_detail"]["pk"],
            self.merge_job.pk,
        )

    def test_sync_support_bundle_uses_ledger_branch_state_when_cache_is_absent(self):
        self.sync.clear_branch_run_state()
        self.client.force_login(self.user)

        response = self.client.get(
            reverse(
                "plugins:forward_netbox:forwardsync_support_bundle",
                kwargs={"pk": self.sync.pk},
            )
        )

        self.assertEqual(response.status_code, 200)
        data = json.loads(response.content)
        self.assertEqual(
            data["sync"]["execution_state"]["execution_run_id"],
            self.execution_run.pk,
        )
        self.assertEqual(data["sync"]["execution_state_source"], "execution_ledger")
        self.assertTrue(data["sync"]["execution_state"]["state_synthesized"])
        self.assertEqual(data["execution_run"]["run"]["id"], self.execution_run.pk)

    def test_sync_support_bundle_survives_cleanup_and_later_run(self):
        self.sync.clear_branch_run_state()
        later_run = ForwardExecutionRun.objects.create(
            sync=self.sync,
            source=self.source,
            backend="branching",
            status="running",
            snapshot_selector="latestProcessed",
            snapshot_id="snapshot-2",
            total_steps=1,
            next_step_index=1,
        )
        self.sync.set_branch_run_state(
            {
                "execution_run_id": later_run.pk,
                "next_plan_index": 1,
                "total_plan_items": 1,
            }
        )
        self.client.force_login(self.user)

        response = self.client.get(
            reverse(
                "plugins:forward_netbox:forwardsync_support_bundle",
                kwargs={"pk": self.sync.pk},
            )
        )

        self.assertEqual(response.status_code, 200)
        data = json.loads(response.content)
        self.assertEqual(data["sync"]["execution_state_source"], "execution_ledger")
        self.assertEqual(
            data["sync"]["execution_state"]["execution_run_id"],
            later_run.pk,
        )
        self.assertEqual(data["execution_run"]["run"]["id"], later_run.pk)

    def test_sync_support_bundle_survives_old_branch_upgrade_cleanup_and_later_run(
        self,
    ):
        branch = Branch.objects.create(
            name="legacy-upgrade-branch",
            schema_id=f"legacy_upgrade_{self.ingestion.pk}",
        )
        self.sync.set_branch_run_state(
            {
                "snapshot_selector": "latestProcessed",
                "snapshot_id": "snapshot-legacy",
                "phase": "executing",
                "phase_message": "Applying planned shard changes.",
                "next_plan_index": 1,
                "total_plan_items": 1,
                "plan_items": [
                    {
                        "index": 1,
                        "model": "dcim.site",
                        "label": "dcim.site legacy shard",
                        "estimated_changes": 1,
                        "status": "merged",
                        "sync_mode": "diff",
                        "query_name": "Forward Sites",
                        "execution_mode": "query_id",
                        "execution_value": "query-site",
                        "baseline_snapshot_id": "snapshot-before",
                        "apply_engine": "adapter",
                    }
                ],
            }
        )
        upgraded_run = active_execution_run(self.sync)
        ingestion = ForwardIngestion.objects.create(
            sync=self.sync,
            snapshot_selector="latestProcessed",
            snapshot_id="snapshot-legacy",
            branch=branch,
        )
        step = upgraded_run.steps.first()
        step.ingestion = ingestion
        step.branch_name = branch.name
        step.save(update_fields=["ingestion", "branch_name"])
        self.sync.clear_branch_run_state()
        branch.delete()
        later_run = ForwardExecutionRun.objects.create(
            sync=self.sync,
            source=self.source,
            backend="branching",
            status="running",
            snapshot_selector="latestProcessed",
            snapshot_id="snapshot-later",
            total_steps=1,
            next_step_index=1,
        )
        self.sync.set_branch_run_state(
            {
                "execution_run_id": later_run.pk,
                "next_plan_index": 1,
                "total_plan_items": 1,
            }
        )
        self.client.force_login(self.user)

        response = self.client.get(
            reverse(
                "plugins:forward_netbox:forwardsync_support_bundle",
                kwargs={"pk": self.sync.pk},
            )
        )

        self.assertEqual(response.status_code, 200)
        data = json.loads(response.content)
        self.assertEqual(data["sync"]["execution_state_source"], "execution_ledger")
        self.assertEqual(
            data["sync"]["execution_state"]["execution_run_id"],
            later_run.pk,
        )
        self.assertEqual(data["execution_run"]["run"]["id"], later_run.pk)
