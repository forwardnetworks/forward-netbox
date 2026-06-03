import os
import uuid
from datetime import timedelta

from core.choices import JobStatusChoices
from core.models import Job
from django.contrib.auth import get_user_model
from django.contrib.contenttypes.models import ContentType
from django.core.management.base import BaseCommand
from django.utils import timezone

from forward_netbox.choices import forward_configured_models
from forward_netbox.choices import ForwardSourceDeploymentChoices
from forward_netbox.choices import ForwardSourceStatusChoices
from forward_netbox.choices import ForwardSyncStatusChoices
from forward_netbox.choices import ForwardValidationStatusChoices
from forward_netbox.models import ForwardDriftPolicy
from forward_netbox.models import ForwardExecutionRun
from forward_netbox.models import ForwardExecutionStep
from forward_netbox.models import ForwardIngestion
from forward_netbox.models import ForwardIngestionIssue
from forward_netbox.models import ForwardNQEMap
from forward_netbox.models import ForwardSource
from forward_netbox.models import ForwardSync
from forward_netbox.models import ForwardValidationRun
from forward_netbox.utilities.branch_budget import DEFAULT_MAX_CHANGES_PER_BRANCH
from forward_netbox.utilities.forward_api import LATEST_PROCESSED_SNAPSHOT
from forward_netbox.utilities.job_compat import ensure_core_job_compat_defaults


class Command(BaseCommand):
    help = "Seed synthetic Forward NetBox records for the Playwright UI harness."

    def add_arguments(self, parser):
        parser.add_argument(
            "--username",
            default=os.getenv("NETBOX_UI_TEST_USERNAME", "admin"),
        )
        parser.add_argument(
            "--password",
            default=os.getenv("NETBOX_UI_TEST_PASSWORD", "admin"),
        )
        parser.add_argument(
            "--source-name",
            default=os.getenv("FORWARD_UI_HARNESS_SOURCE", "ui-harness-source"),
        )
        parser.add_argument(
            "--sync-name",
            default=os.getenv("FORWARD_UI_HARNESS_SYNC", "ui-harness-sync"),
        )
        parser.add_argument(
            "--snapshot-id",
            default=os.getenv("FORWARD_UI_HARNESS_SNAPSHOT", "ui-harness-snapshot"),
        )
        parser.add_argument(
            "--network-id",
            default=os.getenv("FORWARD_UI_HARNESS_NETWORK", "ui-harness-network"),
        )

    def handle(self, *args, **options):
        user = self._ensure_superuser(
            username=options["username"],
            password=options["password"],
        )
        source = self._ensure_source(
            source_name=options["source_name"],
            network_id=options["network_id"],
        )
        policy = self._ensure_drift_policy()
        sync = self._ensure_sync(
            sync_name=options["sync_name"],
            source=source,
            user=user,
            policy=policy,
        )
        self._ensure_data_file_map_visible()
        validation_run = self._ensure_validation_run(
            sync=sync,
            policy=policy,
            snapshot_id=options["snapshot_id"],
        )
        ingestion = self._ensure_ingestion(
            sync=sync,
            user=user,
            snapshot_id=options["snapshot_id"],
            validation_run=validation_run,
        )
        execution_run = self._ensure_execution_run(
            sync=sync,
            ingestion=ingestion,
            snapshot_id=options["snapshot_id"],
            validation_run=validation_run,
        )

        self.stdout.write(self.style.SUCCESS("Seeded Forward UI harness fixture."))
        self.stdout.write(f"username={user.username}")
        self.stdout.write(f"source_url={source.get_absolute_url()}")
        self.stdout.write(f"sync_url={sync.get_absolute_url()}")
        self.stdout.write(f"ingestion_url={ingestion.get_absolute_url()}")
        self.stdout.write(f"execution_run_url={execution_run.get_absolute_url()}")

    def _ensure_superuser(self, *, username, password):
        user_model = get_user_model()
        user, _ = user_model.objects.get_or_create(username=username)
        user.email = f"{username}@example.com"
        user.is_active = True
        if hasattr(user, "is_staff"):
            user.is_staff = True
        if hasattr(user, "is_superuser"):
            user.is_superuser = True
        user.set_password(password)
        user.save()
        return user

    def _ensure_source(self, *, source_name, network_id):
        source, _ = ForwardSource.objects.update_or_create(
            name=source_name,
            defaults={
                "type": ForwardSourceDeploymentChoices.CUSTOM,
                "url": "https://forward-ui-harness.example.test",
                "status": ForwardSourceStatusChoices.READY,
                "parameters": {
                    "username": "ui-harness@example.test",
                    "password": "synthetic-secret",
                    "verify": True,
                    "timeout": 1200,
                    "network_id": network_id,
                },
            },
        )
        source.full_clean()
        source.save()
        return source

    def _ensure_drift_policy(self):
        policy, _ = ForwardDriftPolicy.objects.update_or_create(
            name="ui-harness-drift-policy",
            defaults={
                "enabled": True,
                "require_processed_snapshot": True,
                "block_on_query_errors": True,
                "block_on_zero_rows": False,
                "max_deleted_objects": 100,
                "max_deleted_percent": 50,
            },
        )
        policy.full_clean()
        policy.save()
        return policy

    def _ensure_sync(self, *, sync_name, source, user, policy):
        parameters = {
            "auto_merge": True,
            "max_changes_per_branch": DEFAULT_MAX_CHANGES_PER_BRANCH,
            "snapshot_id": LATEST_PROCESSED_SNAPSHOT,
        }
        enabled_models = {"dcim.site", "dcim.device", "dcim.interface"}
        for model_string in forward_configured_models():
            parameters[model_string] = model_string in enabled_models

        sync, _ = ForwardSync.objects.update_or_create(
            name=sync_name,
            defaults={
                "source": source,
                "status": ForwardSyncStatusChoices.COMPLETED,
                "parameters": parameters,
                "auto_merge": True,
                "user": user,
                "drift_policy": policy,
                "last_synced": timezone.now(),
            },
        )
        sync.full_clean()
        sync.save()
        return sync

    def _ensure_data_file_map_visible(self):
        ForwardNQEMap.objects.filter(
            name="Forward Devices with NetBox Device Type Aliases"
        ).update(enabled=True)

    def _model_results(self, snapshot_id):
        return [
            {
                "model": "dcim.site",
                "query_name": "Forward Locations",
                "execution_mode": "query",
                "execution_value": "Forward Locations",
                "sync_mode": "full",
                "row_count": 1,
                "delete_count": 0,
                "failure_count": 0,
                "runtime_ms": 5.0,
                "snapshot_id": snapshot_id,
                "baseline_snapshot_id": "",
                "branch_plan_index": 1,
                "branch_plan_total": 1,
                "estimated_changes": 7,
                "shard_key_count": 0,
            },
            {
                "model": "dcim.device",
                "query_name": "Forward Devices",
                "execution_mode": "query",
                "execution_value": "Forward Devices",
                "sync_mode": "full",
                "row_count": 2,
                "delete_count": 0,
                "failure_count": 0,
                "runtime_ms": 7.0,
                "snapshot_id": snapshot_id,
                "baseline_snapshot_id": "",
            },
            {
                "model": "dcim.interface",
                "query_name": "Forward Interfaces",
                "execution_mode": "query",
                "execution_value": "Forward Interfaces",
                "sync_mode": "full",
                "row_count": 4,
                "delete_count": 0,
                "failure_count": 0,
                "runtime_ms": 9.0,
                "snapshot_id": snapshot_id,
                "baseline_snapshot_id": "",
            },
        ]

    def _ensure_validation_run(self, *, sync, policy, snapshot_id):
        ForwardValidationRun.objects.filter(sync=sync).delete()
        return ForwardValidationRun.objects.create(
            sync=sync,
            policy=policy,
            status=ForwardValidationStatusChoices.PASSED,
            allowed=True,
            snapshot_selector=LATEST_PROCESSED_SNAPSHOT,
            snapshot_id=snapshot_id,
            baseline_snapshot_id="",
            snapshot_info={
                "state": "PROCESSED",
                "createdAt": "2026-04-27T00:00:00Z",
                "processedAt": "2026-04-27T00:05:00Z",
            },
            snapshot_metrics={"snapshotState": "PROCESSED"},
            model_results=self._model_results(snapshot_id),
            drift_summary={
                "model_count": 3,
                "branch_count": 1,
                "total_rows": 7,
                "total_deletes": 0,
                "total_failures": 0,
            },
            blocking_reasons=[],
            started=timezone.now(),
            completed=timezone.now(),
        )

    def _ensure_ingestion(self, *, sync, user, snapshot_id, validation_run):
        old_job_ids = {
            job_id
            for job_ids in ForwardIngestion.objects.filter(sync=sync).values_list(
                "job_id",
                "merge_job_id",
            )
            for job_id in job_ids
            if job_id
        }
        ForwardIngestion.objects.filter(sync=sync).delete()
        if old_job_ids:
            Job.objects.filter(pk__in=old_job_ids).delete()
        ingestion = ForwardIngestion.objects.create(
            sync=sync,
            snapshot_selector=LATEST_PROCESSED_SNAPSHOT,
            snapshot_id=snapshot_id,
            sync_mode="full",
            baseline_ready=True,
            validation_run=validation_run,
            snapshot_info={
                "state": "PROCESSED",
                "createdAt": "2026-04-27T00:00:00Z",
                "processedAt": "2026-04-27T00:05:00Z",
            },
            snapshot_metrics={
                "snapshotState": "PROCESSED",
                "numSuccessfulDevices": 2,
                "numCollectionFailureDevices": 0,
                "numProcessingFailureDevices": 0,
                "numSuccessfulEndpoints": 4,
                "numCollectionFailureEndpoints": 0,
                "numProcessingFailureEndpoints": 0,
                "collectionDuration": 12,
                "processingDuration": 34,
            },
            model_results=self._model_results(snapshot_id),
        )
        job = self._ensure_job(ingestion=ingestion, user=user)
        ingestion.job = job
        ingestion.save(update_fields=["job"])
        ForwardIngestionIssue.objects.create(
            ingestion=ingestion,
            phase="sync",
            model="dcim.interface",
            message="Synthetic validation warning for UI harness rendering.",
            exception="SyntheticWarning",
            raw_data={"fixture": True},
        )
        return ingestion

    def _ensure_execution_run(self, *, sync, ingestion, snapshot_id, validation_run):
        ForwardExecutionRun.objects.filter(sync=sync).delete()
        run = ForwardExecutionRun.objects.create(
            sync=sync,
            source=sync.source,
            job=ingestion.job,
            validation_run=validation_run,
            backend="branching",
            status="completed",
            phase="completed",
            phase_message="Synthetic UI harness execution completed.",
            snapshot_selector=LATEST_PROCESSED_SNAPSHOT,
            snapshot_id=snapshot_id,
            max_changes_per_branch=DEFAULT_MAX_CHANGES_PER_BRANCH,
            auto_merge=True,
            total_steps=2,
            next_step_index=3,
            plan_preview={
                "total_estimated_changes": 7,
                "branch_count": 2,
                "models": {
                    "dcim.site": {"branches": 1, "estimated_changes": 1},
                    "dcim.interface": {"branches": 1, "estimated_changes": 4},
                },
            },
            model_change_density={"dcim.interface": 1.0},
            latest_heartbeat=timezone.now(),
            baseline_ready=True,
            completed=timezone.now(),
        )
        ForwardExecutionStep.objects.create(
            run=run,
            index=1,
            kind="stage",
            status="merged",
            model_string="dcim.site",
            label="dcim.site | Forward Locations",
            query_name="Forward Locations",
            execution_mode="query_path",
            execution_value="/forward_netbox_validation/forward_locations",
            sync_mode="full",
            estimated_changes=1,
            actual_changes=1,
            query_parameters={
                "forward_netbox_shard_keys": ["site:ui-harness-site-1"],
            },
            fetch_mode="model",
            ingestion=ingestion,
            job=ingestion.job,
            heartbeat=timezone.now(),
            started=timezone.now() - timedelta(minutes=2),
            completed=timezone.now() - timedelta(minutes=1),
        )
        ForwardExecutionStep.objects.create(
            run=run,
            index=2,
            kind="stage",
            status="merged",
            model_string="dcim.interface",
            label="dcim.interface | Forward Interfaces shard 1",
            query_name="Forward Interfaces",
            execution_mode="query_path",
            execution_value="/forward_netbox_validation/forward_interfaces",
            sync_mode="full",
            estimated_changes=4,
            actual_changes=4,
            query_parameters={
                "forward_netbox_shard_keys": ["device:ui-harness-device-1"],
            },
            shard_keys=["device:ui-harness-device-1"],
            fetch_mode="nqe_column_filter",
            fetch_key_family="device",
            fetch_parameters={
                "forward_netbox_shard_mode": "shard_keys",
                "forward_netbox_shard_keys": ["device:ui-harness-device-1"],
                "forward_netbox_shard_bucket": 0,
                "forward_netbox_shard_bucket_count": 1,
            },
            fetch_column_filters=[
                {
                    "operator": "DEFAULT",
                    "columnName": "device",
                    "value": "ui-harness-device-1",
                }
            ],
            ingestion=ingestion,
            job=ingestion.job,
            heartbeat=timezone.now(),
            started=timezone.now() - timedelta(minutes=1),
            completed=timezone.now(),
        )
        return run

    def _ensure_job(self, *, ingestion, user):
        ensure_core_job_compat_defaults()
        content_type = ContentType.objects.get_for_model(ForwardIngestion)
        now = timezone.now()
        values = {
            "object_type": content_type,
            "object_id": ingestion.pk,
            "name": f"{ingestion.sync.name} - ui harness",
            "user": user,
            "status": JobStatusChoices.STATUS_COMPLETED,
            "job_id": uuid.uuid4(),
            "created": now,
            "started": now,
            "completed": now,
            "data": {
                "statistics": {
                    "dcim.site": {"current": 1, "total": 1},
                    "dcim.device": {"current": 2, "total": 2},
                    "dcim.interface": {"current": 4, "total": 4},
                },
                "logs": [
                    [
                        "2026-04-27T00:00:00Z",
                        "success",
                        str(ingestion.sync),
                        ingestion.sync.get_absolute_url(),
                        "Synthetic UI harness ingestion completed.",
                    ]
                ],
            },
        }
        if any(field.name == "notifications" for field in Job._meta.fields):
            values["notifications"] = []
        return Job.objects.create(**values)
