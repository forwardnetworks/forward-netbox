# The two halves of config backup that shipped in 2.9.0 without tests: the
# job layer (when the overlay enqueues, what the job records, what it never
# records) and the delivery health check (the five ways a backup can succeed
# while delivering nothing to Validity).
from unittest.mock import patch
from uuid import uuid4

from core.choices import JobStatusChoices
from core.models import DataSource
from core.models import Job
from django.contrib.contenttypes.models import ContentType
from django.test import TestCase
from django.utils import timezone

from forward_netbox.choices import ForwardSyncStatusChoices
from forward_netbox.jobs import _maybe_enqueue_config_backup
from forward_netbox.jobs import _run_forward_config_backup_work
from forward_netbox.models import ForwardIngestion
from forward_netbox.models import ForwardSource
from forward_netbox.models import ForwardSync
from forward_netbox.utilities.config_backup import ConfigBackupResult
from forward_netbox.utilities.health import _config_backup_delivery_check

SECRET_CONFIG = "enable secret 9 $9$abcdefghijklmnop\n"


class _Fixture(TestCase):
    def setUp(self):
        self.data_source = DataSource.objects.create(
            name="cb-health-repo",
            type="git",
            source_url="https://git.example.com/x.git",
        )
        self.source = ForwardSource.objects.create(
            name="cb-health-source",
            type="saas",
            url="https://fwd.app",
            parameters={
                "network_id": "net-1",
                "config_backup_data_source": self.data_source.pk,
            },
        )
        self.sync = ForwardSync.objects.create(
            name="cb-health-sync",
            source=self.source,
            status=ForwardSyncStatusChoices.COMPLETED,
            parameters={"snapshot_id": "latestProcessed"},
        )
        self.ingestion = ForwardIngestion.objects.create(
            sync=self.sync, snapshot_id="snap-1", baseline_ready=True
        )

    def _job(self, name="cb-health-sync - config backup (auto)", status=None):
        return Job.objects.create(
            object_type=ContentType.objects.get_for_model(ForwardSync),
            object_id=self.sync.pk,
            name=name,
            status=status or JobStatusChoices.STATUS_RUNNING,
            job_id=uuid4(),
        )


class ConfigBackupEnqueueGateTest(_Fixture):
    def test_a_completed_sync_with_the_parameter_enqueues_once(self):
        with patch("forward_netbox.jobs.ConfigBackupJob.enqueue") as enqueue:
            _maybe_enqueue_config_backup(
                self.sync, snapshot_id="snap-1", ingestion_id=self.ingestion.pk
            )
        enqueue.assert_called_once()
        self.assertEqual(enqueue.call_args.kwargs["snapshot_id"], "snap-1")
        self.assertEqual(enqueue.call_args.kwargs["ingestion_id"], self.ingestion.pk)

    def test_a_sync_that_did_not_complete_does_not_enqueue(self):
        ForwardSync.objects.filter(pk=self.sync.pk).update(
            status=ForwardSyncStatusChoices.FAILED
        )
        self.sync.refresh_from_db()
        with patch("forward_netbox.jobs.ConfigBackupJob.enqueue") as enqueue:
            _maybe_enqueue_config_backup(self.sync, snapshot_id="snap-1")
        enqueue.assert_not_called()

    def test_no_snapshot_id_does_not_enqueue(self):
        with patch("forward_netbox.jobs.ConfigBackupJob.enqueue") as enqueue:
            _maybe_enqueue_config_backup(self.sync, snapshot_id="  ")
        enqueue.assert_not_called()

    def test_without_the_parameter_nothing_is_enqueued(self):
        self.source.parameters = {"network_id": "net-1"}
        self.source.save()
        self.sync.refresh_from_db()
        with patch("forward_netbox.jobs.ConfigBackupJob.enqueue") as enqueue:
            _maybe_enqueue_config_backup(self.sync, snapshot_id="snap-1")
        enqueue.assert_not_called()

    def test_an_active_backup_of_the_same_name_blocks_a_second(self):
        self._job(status=JobStatusChoices.STATUS_PENDING)
        with patch("forward_netbox.jobs.ConfigBackupJob.enqueue") as enqueue:
            _maybe_enqueue_config_backup(self.sync, snapshot_id="snap-1")
        enqueue.assert_not_called()


class ConfigBackupJobWorkTest(_Fixture):
    def test_the_job_records_counts_only_never_configuration_text(self):
        job = self._job()
        result = ConfigBackupResult()
        result.written = 2
        result.pushed = True
        result.commit = "abc123"
        result.snapshot_id = "snap-1"

        with patch(
            "forward_netbox.utilities.config_backup.run_config_backup",
            return_value=result,
        ) as run:
            _run_forward_config_backup_work(
                job, snapshot_id="snap-1", ingestion_id=self.ingestion.pk
            )

        run.assert_called_once()
        job.refresh_from_db()
        self.assertEqual(job.data["written"], 2)
        self.assertTrue(job.data["pushed"])
        self.assertEqual(job.data["snapshot_id"], "snap-1")
        self.assertNotIn("configs", job.data)
        self.assertNotIn(SECRET_CONFIG, str(job.data))

    def test_a_stale_snapshot_exits_without_running_the_backup(self):
        ForwardIngestion.objects.create(
            sync=self.sync, snapshot_id="snap-2", baseline_ready=True
        )
        job = self._job()

        with patch(
            "forward_netbox.utilities.config_backup.run_config_backup"
        ) as run, patch("forward_netbox.jobs._enqueue_post_sync_overlays"):
            _run_forward_config_backup_work(
                job, snapshot_id="snap-1", ingestion_id=self.ingestion.pk
            )

        run.assert_not_called()
        job.refresh_from_db()
        self.assertEqual(job.data["skipped"], "stale_post_sync_snapshot")

    def test_a_failure_is_recorded_by_type_and_the_url_never_reaches_job_data(self):
        job = self._job()
        leak = "https://svc:s3cret@git.example.com/x.git"

        with patch(
            "forward_netbox.utilities.config_backup.run_config_backup",
            side_effect=RuntimeError(f"push to {leak} failed"),
        ):
            with self.assertRaises(RuntimeError):
                _run_forward_config_backup_work(
                    job, snapshot_id="snap-1", ingestion_id=self.ingestion.pk
                )

        job.refresh_from_db()
        self.assertEqual(job.data["error_type"], "RuntimeError")
        self.assertNotIn("s3cret", str(job.data))
        self.assertNotIn(leak, str(job.data))

    def test_the_operator_button_resolves_its_own_snapshot(self):
        job = self._job(name="cb-health-sync - config backup")
        result = ConfigBackupResult()
        result.pushed = True

        with patch.object(
            ForwardSync, "resolve_snapshot_id", return_value="snap-1"
        ), patch.object(ForwardSource, "get_client", return_value=object()), patch(
            "forward_netbox.utilities.config_backup.run_config_backup",
            return_value=result,
        ) as run:
            _run_forward_config_backup_work(job)

        self.assertEqual(run.call_args.kwargs["snapshot_id"], "snap-1")


class ConfigBackupDeliveryCheckTest(_Fixture):
    def test_disabled_backup_yields_no_check(self):
        self.source.parameters = {"network_id": "net-1"}
        self.source.save()
        self.sync.refresh_from_db()
        self.assertIsNone(_config_backup_delivery_check(self.sync))

    def test_a_missing_data_source_warns(self):
        self.source.parameters["config_backup_data_source"] = 999999
        self.source.save()
        self.sync.refresh_from_db()
        check = _config_backup_delivery_check(self.sync)
        self.assertEqual(check["status"], "warn")
        self.assertIn("no longer exists", check["message"])

    def test_a_never_synced_data_source_warns(self):
        check = _config_backup_delivery_check(self.sync)
        self.assertEqual(check["status"], "warn")
        self.assertIn("has never synced", check["message"])

    def test_a_synced_data_source_passes_without_validity(self):
        DataSource.objects.filter(pk=self.data_source.pk).update(
            last_synced=timezone.now()
        )
        with patch("django.apps.apps.is_installed", return_value=False):
            check = _config_backup_delivery_check(self.sync)
        self.assertEqual(check["status"], "pass")

    def _with_validity(self, template=None, bound_value=None, default=False):
        DataSource.objects.filter(pk=self.data_source.pk).update(
            last_synced=timezone.now()
        )
        if template is not None:
            self.data_source.custom_field_data = {"device_config_path": template}
            if default:
                self.data_source.custom_field_data["default"] = True
            DataSource.objects.filter(pk=self.data_source.pk).update(
                custom_field_data=self.data_source.custom_field_data
            )
        if bound_value is not None:
            from tenancy.models import Tenant

            tenant = Tenant.objects.create(name="CB Tenant", slug="cb-tenant")
            Tenant.objects.filter(pk=tenant.pk).update(
                custom_field_data={"data_source": bound_value}
            )
        with patch(
            "django.apps.apps.is_installed", side_effect=lambda app: app == "validity"
        ):
            return _config_backup_delivery_check(self.sync)

    def test_validity_without_a_config_path_warns(self):
        check = self._with_validity(template=None)
        self.assertEqual(check["status"], "warn")
        self.assertIn("no `device_config_path`", check["message"])

    def test_a_config_path_outside_our_prefix_warns(self):
        check = self._with_validity(template="backups/{{device.name}}.cfg")
        self.assertEqual(check["status"], "warn")
        self.assertIn("does not point at `configs/`", check["message"])

    def test_no_tenant_binding_and_no_default_warns(self):
        check = self._with_validity(template="configs/{{device.name}}.cfg")
        self.assertEqual(check["status"], "warn")
        self.assertIn("no tenant binds", check["message"])

    def test_a_tenant_binding_stored_as_an_int_passes(self):
        check = self._with_validity(
            template="configs/{{device.name}}.cfg", bound_value=self.data_source.pk
        )
        self.assertEqual(check["status"], "pass")

    def test_a_tenant_binding_stored_as_a_string_passes_too(self):
        # Validity casts this out of JSON; NetBox may have stored either form.
        check = self._with_validity(
            template="configs/{{device.name}}.cfg", bound_value=str(self.data_source.pk)
        )
        self.assertEqual(check["status"], "pass")

    def test_a_default_data_source_passes_without_a_tenant(self):
        check = self._with_validity(
            template="configs/{{device.name}}.cfg", default=True
        )
        self.assertEqual(check["status"], "pass")
