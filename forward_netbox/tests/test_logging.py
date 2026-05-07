from datetime import datetime
from types import SimpleNamespace
from unittest.mock import patch

from django.test import TestCase

from forward_netbox.utilities.logging import SyncLogging


class SyncLoggingTest(TestCase):
    @patch("forward_netbox.utilities.logging.cache.set")
    @patch("forward_netbox.utilities.logging.Job.objects.get")
    @patch("forward_netbox.utilities.logging.timezone.now")
    def test_log_success_persists_core_job_log_entry(
        self, mock_now, mock_job_get, mock_cache_set
    ):
        mock_now.return_value = datetime.fromisoformat("2026-05-04T14:00:00+00:00")
        job = SimpleNamespace(log_entries=[], save=lambda update_fields=None: None)
        mock_job_get.return_value = job

        logger = SyncLogging(job=52)
        logger.log_success("Synthetic UI harness ingestion completed.")

        self.assertEqual(len(job.log_entries), 1)
        self.assertEqual(job.log_entries[0]["level"], "info")
        self.assertEqual(
            job.log_entries[0]["message"], "Synthetic UI harness ingestion completed."
        )
        self.assertEqual(job.log_entries[0]["timestamp"], mock_now.return_value)
        mock_cache_set.assert_called_once()

    @patch("forward_netbox.utilities.logging.cache.set")
    @patch("forward_netbox.utilities.logging.Job.objects.get")
    @patch("forward_netbox.utilities.logging.timezone.now")
    def test_log_failure_persists_core_job_log_entry(
        self, mock_now, mock_job_get, mock_cache_set
    ):
        mock_now.return_value = datetime.fromisoformat("2026-05-04T14:00:00+00:00")
        job = SimpleNamespace(log_entries=[], save=lambda update_fields=None: None)
        mock_job_get.return_value = job

        logger = SyncLogging(job=52)
        logger.log_failure("Forward ingestion failed.")

        self.assertEqual(len(job.log_entries), 1)
        self.assertEqual(job.log_entries[0]["level"], "error")
        self.assertEqual(job.log_entries[0]["message"], "Forward ingestion failed.")
        mock_cache_set.assert_called_once()
