# The config backup reads Forward and writes git for most of an hour on a large
# estate. It ran inside the post-sync generation guard, which holds the sync and
# source rows and the global ownership lock, so every sync, merge, prune and
# sync-form save waited behind it; a customer's sync edit timed out at the
# gateway and was lost. Synthetic data.
from contextlib import contextmanager
from types import SimpleNamespace
from unittest.mock import Mock
from unittest.mock import patch
from uuid import uuid4

from core.choices import JobStatusChoices
from core.models import Job
from django.contrib.contenttypes.models import ContentType
from django.db import OperationalError
from django.test import TestCase

from forward_netbox.forms import _sync_row_busy
from forward_netbox.models import ForwardSource
from forward_netbox.models import ForwardSync


class _Base(TestCase):
    def setUp(self):
        self.source = ForwardSource.objects.create(
            name="lj-src",
            type="saas",
            url="https://fwd.app",
            status="ready",
            parameters={"network_id": "net-1"},
        )
        self.sync = ForwardSync.objects.create(
            name="lj-sync", source=self.source, parameters={}
        )


class ConfigBackupReleasesTheLocksTest(_Base):
    def test_the_backup_runs_after_the_generation_check_releases_the_locks(self):
        from forward_netbox.jobs import _run_forward_config_backup_work

        state = {"inside": False, "checked": False, "inside_during_backup": None}

        @contextmanager
        def guard(*args, **kwargs):
            state["inside"] = True
            state["checked"] = True
            try:
                yield {}
            finally:
                state["inside"] = False

        def backup(*args, **kwargs):
            state["inside_during_backup"] = state["inside"]
            return SimpleNamespace(as_dict=lambda: {"written": 1})

        job = Job.objects.create(
            object_type=ContentType.objects.get_for_model(ForwardSync),
            object_id=self.sync.pk,
            name="lj config backup",
            status=JobStatusChoices.STATUS_RUNNING,
            job_id=uuid4(),
        )
        with (
            patch(
                "forward_netbox.utilities.post_sync.current_post_sync_snapshot", guard
            ),
            patch("forward_netbox.utilities.config_backup.run_config_backup", backup),
            patch("forward_netbox.jobs.SyncLogging", return_value=Mock()),
        ):
            _run_forward_config_backup_work(job, snapshot_id="s1", ingestion_id=1)

        self.assertTrue(state["checked"], "the generation is still checked")
        self.assertIs(state["inside_during_backup"], False)


class SyncFormRefusesWhenTheRowIsHeldTest(_Base):
    def test_a_held_row_refuses_at_once_with_a_readable_reason(self):
        with patch.object(
            ForwardSync.objects.__class__,
            "select_for_update",
            side_effect=OperationalError("could not obtain lock"),
        ):
            message = _sync_row_busy(self.sync)

        self.assertIn("This sync is busy", message)
        self.assertIn("Nothing was changed", message)

    def test_a_free_row_says_nothing(self):
        self.assertEqual(_sync_row_busy(self.sync), "")

    def test_a_new_sync_is_never_busy(self):
        self.assertEqual(_sync_row_busy(ForwardSync()), "")


class OneConfigBackupPerSyncTest(_Base):
    def test_a_second_backup_for_the_same_sync_is_refused_while_one_runs(self):
        from django.db import connections

        from forward_netbox.jobs import _CONFIG_BACKUP_LOCK_NAMESPACE
        from forward_netbox.jobs import _one_config_backup_per_sync

        other = connections.create_connection("default")
        try:
            with other.cursor() as cursor:
                cursor.execute(
                    "SELECT pg_advisory_lock(%s, %s)",
                    [_CONFIG_BACKUP_LOCK_NAMESPACE, self.sync.pk],
                )
            with _one_config_backup_per_sync(self.sync.pk) as acquired:
                self.assertFalse(acquired)
        finally:
            other.close()

        with _one_config_backup_per_sync(self.sync.pk) as acquired:
            self.assertTrue(acquired)

    def test_a_skipped_backup_records_why_and_does_not_run(self):
        from forward_netbox.jobs import _run_forward_config_backup_work

        @contextmanager
        def held(_pk):
            yield False

        @contextmanager
        def guard(*args, **kwargs):
            yield {}

        job = Job.objects.create(
            object_type=ContentType.objects.get_for_model(ForwardSync),
            object_id=self.sync.pk,
            name="lj config backup",
            status=JobStatusChoices.STATUS_RUNNING,
            job_id=uuid4(),
        )
        with (
            patch(
                "forward_netbox.utilities.post_sync.current_post_sync_snapshot", guard
            ),
            patch("forward_netbox.jobs._one_config_backup_per_sync", held),
            patch("forward_netbox.utilities.config_backup.run_config_backup") as run,
            patch("forward_netbox.jobs.SyncLogging", return_value=Mock()),
        ):
            _run_forward_config_backup_work(job, snapshot_id="s1", ingestion_id=1)

        run.assert_not_called()
        job.refresh_from_db()
        self.assertTrue(job.data["skipped"])
