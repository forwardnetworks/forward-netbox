from datetime import datetime
from types import SimpleNamespace
from unittest.mock import patch

from django.test import TestCase

from forward_netbox.utilities.logging import SyncLogging


class SyncLoggingTest(TestCase):
    @patch("forward_netbox.utilities.logging.Job.objects.filter")
    @patch("forward_netbox.utilities.logging.Job.objects.get")
    @patch("forward_netbox.utilities.logging.ContentType.objects.filter")
    @patch("forward_netbox.utilities.logging.timezone.now")
    def test_log_success_persists_core_job_log_entry(
        self, mock_now, mock_content_type_filter, mock_job_get, mock_job_filter
    ):
        mock_now.return_value = datetime.fromisoformat("2026-05-04T14:00:00+00:00")
        mock_content_type_filter.return_value.exists.return_value = True
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
        mock_job_filter.return_value.update.assert_called_once_with(
            data=logger.log_data
        )

    @patch("forward_netbox.utilities.logging.Job.objects.filter")
    @patch("forward_netbox.utilities.logging.Job.objects.get")
    @patch("forward_netbox.utilities.logging.ContentType.objects.filter")
    @patch("forward_netbox.utilities.logging.timezone.now")
    def test_log_failure_persists_core_job_log_entry(
        self, mock_now, mock_content_type_filter, mock_job_get, mock_job_filter
    ):
        mock_now.return_value = datetime.fromisoformat("2026-05-04T14:00:00+00:00")
        mock_content_type_filter.return_value.exists.return_value = True
        job = SimpleNamespace(log_entries=[], save=lambda update_fields=None: None)
        mock_job_get.return_value = job

        logger = SyncLogging(job=52)
        logger.log_failure("Forward ingestion failed.")

        self.assertEqual(len(job.log_entries), 1)
        self.assertEqual(job.log_entries[0]["level"], "error")
        self.assertEqual(job.log_entries[0]["message"], "Forward ingestion failed.")
        mock_job_filter.return_value.update.assert_called_once_with(
            data=logger.log_data
        )

    @patch("forward_netbox.utilities.logging.Job.objects.filter")
    @patch("forward_netbox.utilities.logging.Job.objects.get")
    @patch("forward_netbox.utilities.logging.ContentType.objects.filter")
    @patch("forward_netbox.utilities.logging.timezone.now")
    def test_log_skips_core_job_persistence_when_object_type_missing(
        self, mock_now, mock_content_type_filter, mock_job_get, mock_job_filter
    ):
        mock_now.return_value = datetime.fromisoformat("2026-05-04T14:00:00+00:00")
        mock_content_type_filter.return_value.exists.return_value = False
        job = SimpleNamespace(
            object_type_id=123,
            log_entries=[],
            save=lambda update_fields=None: None,
        )
        mock_job_get.return_value = job

        logger = SyncLogging(job=52)
        logger.log_info("Forward ingestion is still running.")

        self.assertEqual(len(job.log_entries), 0)
        mock_job_filter.return_value.update.assert_called_once_with(
            data=logger.log_data
        )

    @patch("forward_netbox.utilities.logging.Job.objects.filter")
    def test_set_api_usage_summary_persists_counter_payload(self, mock_job_filter):
        logger = SyncLogging(job=52)

        logger.set_api_usage_summary(
            {
                "http_attempts": 5,
                "http_429_failures": 1,
                "nqe_pages": 3,
            }
        )

        self.assertEqual(
            logger.log_data["forward_api_usage"],
            {
                "http_attempts": 5,
                "http_429_failures": 1,
                "nqe_pages": 3,
            },
        )
        mock_job_filter.return_value.update.assert_called_once_with(
            data=logger.log_data
        )

    @patch("forward_netbox.utilities.logging.Job.objects.filter")
    def test_add_dependency_lookup_summary_persists_model_payload(
        self, mock_job_filter
    ):
        logger = SyncLogging(job=52)

        logger.add_dependency_lookup_summary(
            {
                "model": "dcim.device",
                "row_count": 4,
                "primed_target_count": 7,
                "device_name_count": 4,
                "tag_row_count": 0,
                "interface_pair_count": 2,
                "module_bay_pair_count": 0,
                "fhrp_group_count": 1,
                "ipam_identity_row_count": 0,
                "ipam_global_host_row_count": 0,
            }
        )

        self.assertTrue(logger.log_data["dependency_lookup_cache"]["available"])
        self.assertEqual(logger.log_data["dependency_lookup_cache"]["row_count"], 4)
        self.assertEqual(
            logger.log_data["dependency_lookup_cache"]["primed_target_count"], 7
        )
        self.assertEqual(
            logger.log_data["dependency_lookup_cache"]["models"][0]["fhrp_group_count"],
            1,
        )
        self.assertEqual(
            logger.log_data["dependency_lookup_cache"]["models"][0]["model"],
            "dcim.device",
        )
        mock_job_filter.return_value.update.assert_called_once_with(
            data=logger.log_data
        )

    @patch("forward_netbox.utilities.logging.Job.objects.filter")
    def test_increment_statistics_supports_bulk_amounts(self, mock_job_filter):
        logger = SyncLogging(job=52)

        logger.increment_statistics("dcim.interface", outcome="skipped", amount=3)

        self.assertEqual(
            logger.log_data["statistics"]["dcim.interface"],
            {
                "current": 3,
                "total": 0,
                "applied": 0,
                "failed": 0,
                "skipped": 3,
                "unchanged": 0,
            },
        )
        mock_job_filter.return_value.update.assert_called_once_with(
            data=logger.log_data
        )

    @patch("forward_netbox.utilities.logging.monotonic", return_value=10.0)
    @patch("forward_netbox.utilities.logging.Job.objects.filter")
    def test_single_row_statistics_are_debounced_until_durable_flush(
        self, mock_job_filter, _mock_monotonic
    ):
        logger = SyncLogging(job=52)

        for _ in range(1000):
            logger.increment_statistics("dcim.interface", outcome="applied")

        self.assertEqual(
            logger.log_data["statistics"]["dcim.interface"]["applied"],
            1000,
        )
        mock_job_filter.assert_not_called()

        logger.flush()

        mock_job_filter.return_value.update.assert_called_once_with(
            data=logger.log_data
        )

    @patch("forward_netbox.utilities.logging.Job.objects.filter")
    def test_add_dependency_parent_coverage_summary_persists_model_payload(
        self, mock_job_filter
    ):
        logger = SyncLogging(job=52)

        logger.add_dependency_parent_coverage_summary(
            {
                "available": True,
                "model": "dcim.interface",
                "row_count": 8,
                "blocked_row_count": 3,
                "missing_parent_count": 1,
                "missing_parent_names": ["device-1"],
                "groups": [
                    {
                        "parent_model": "dcim.device",
                        "parent_field": "device",
                        "parent_name": "device-1",
                        "row_count": 3,
                        "sample_rows": ["eth1/1", "eth1/2"],
                    }
                ],
            }
        )

        self.assertTrue(logger.log_data["dependency_parent_coverage"]["available"])
        self.assertEqual(
            logger.log_data["dependency_parent_coverage"]["row_count"],
            8,
        )
        self.assertEqual(
            logger.log_data["dependency_parent_coverage"]["blocked_row_count"],
            3,
        )
        self.assertEqual(
            logger.log_data["dependency_parent_coverage"]["models"][0]["model"],
            "dcim.interface",
        )
        mock_job_filter.return_value.update.assert_called_once_with(
            data=logger.log_data
        )
