"""Why there are fewer files than devices, answered without a shell.

"I see 2,744 configs but there are 4,286 devices" needed a person to work out
which devices Forward returned nothing for. The backup now records, in counts
only, how many managed devices it fetched for and how many had no collected
configuration, and Health and the bundle say so in plain words next to the
exact Validity path for the folder in use.
"""

from datetime import timedelta
from uuid import uuid4

from core.choices import JobStatusChoices
from core.models import Job
from dcim.models import Device
from django.contrib.auth import get_user_model
from django.contrib.contenttypes.models import ContentType
from django.urls import reverse
from django.utils import timezone

from forward_netbox.models import ForwardDeviceIdentity
from forward_netbox.models import ForwardSync
from forward_netbox.tests.test_config_backup_folder import FolderTestBase
from forward_netbox.tests.test_config_backup_folder import ROWS
from forward_netbox.utilities.health import config_backup_card
from forward_netbox.utilities.health import config_backup_delivery_bundle_payload
from forward_netbox.utilities.health import config_backup_delivery_state
from forward_netbox.utilities.health import config_backup_shortfall_sentence
from forward_netbox.utilities.health import sync_health_summary


class EvidenceBase(FolderTestBase):
    def _third_device_with_no_config(self):
        device = Device.objects.first()
        third = Device.objects.create(
            name="router-3",
            site=device.site,
            device_type=device.device_type,
            role=device.role,
        )
        ForwardDeviceIdentity.objects.create(
            sync=self.sync,
            ingestion=self.sync.forwardingestion_set.first(),
            source_device_key="fwd-router-3",
            device=third,
        )

    def _job(self, data, *, minutes_ago=0, status=JobStatusChoices.STATUS_COMPLETED):
        when = timezone.now() - timedelta(minutes=minutes_ago)
        return Job.objects.create(
            object_type=ContentType.objects.get_for_model(ForwardSync),
            object_id=self.sync.pk,
            name="evidence sync - config backup (auto)",
            status=status,
            job_id=uuid4(),
            created=when,
            started=when,
            completed=when,
            data=data,
        )


class RunRecordsTheShortfallTest(EvidenceBase):
    def test_devices_forward_returned_nothing_for_are_counted(self):
        self._third_device_with_no_config()

        result = self._run(rows=ROWS)

        self.assertEqual(result.scoped_devices, 3)
        self.assertEqual(result.scoped_without_config, 1)
        self.assertEqual(result.written, 2)
        self.assertEqual(result.files_in_folder, 2)
        recorded = result.as_dict()
        self.assertEqual(recorded["scoped_devices"], 3)
        self.assertEqual(recorded["scoped_without_config"], 1)
        self.assertEqual(recorded["files_in_folder"], 2)

    def test_every_device_returned_a_config_means_none_are_missing(self):
        result = self._run(rows=ROWS)

        self.assertEqual(result.scoped_devices, 2)
        self.assertEqual(result.scoped_without_config, 0)

    def test_an_unmapped_row_does_not_count_as_a_managed_device_with_a_config(self):
        self._third_device_with_no_config()

        result = self._run(
            rows=ROWS + [{"name": "fwd-unknown", "config": "hostname x\n"}]
        )

        self.assertEqual(result.unmapped, 1)
        self.assertEqual(result.scoped_without_config, 1)

    def test_files_in_the_folder_include_files_from_earlier_runs(self):
        self._third_device_with_no_config()
        self._run(rows=ROWS)

        second = self._run(
            rows=[{"name": "fwd-router-1", "config": "hostname router-1 changed\n"}],
            snapshot_id="snap-2",
        )

        self.assertEqual(second.files_in_folder, 2)
        self.assertEqual(second.scoped_without_config, 2)


class HealthReadsItTest(EvidenceBase):
    def _record(self, result, **overrides):
        data = {**result.as_dict(), **overrides}
        return self._job(data)

    def test_the_sentence_names_the_counts_and_the_cause(self):
        self._third_device_with_no_config()
        self._record(self._run(rows=ROWS))

        state = config_backup_delivery_state(self.sync)

        sentence = config_backup_shortfall_sentence(state)
        self.assertIn(
            "Forward returned a configuration for 2 of 3 managed devices", sentence
        )
        self.assertIn("1 have no collected configuration in that snapshot", sentence)
        self.assertIn("no file exists for them", sentence)

    def test_all_devices_returned_says_so_without_a_gap(self):
        self._record(self._run(rows=ROWS))

        state = config_backup_delivery_state(self.sync)

        self.assertEqual(
            config_backup_shortfall_sentence(state),
            "Forward returned a configuration for all 2 managed devices in the "
            "last backup.",
        )

    def test_a_later_run_that_skipped_does_not_hide_the_run_that_did_the_work(self):
        self._third_device_with_no_config()
        self._job(self._run(rows=ROWS).as_dict(), minutes_ago=30)
        self._job(
            {
                "snapshot_id": "snap-1",
                "rows": 0,
                "skipped_reason": "snapshot already backed up",
            },
            minutes_ago=1,
        )

        state = config_backup_delivery_state(self.sync)

        self.assertEqual(state["last_skipped_reason"], "snapshot already backed up")
        self.assertEqual(state["last_result"]["scoped_devices"], 3)
        self.assertEqual(state["last_result"]["written"], 2)

    def test_a_run_recorded_before_the_scope_was_kept_falls_back_on_what_it_wrote(
        self,
    ):
        self._third_device_with_no_config()
        self._job({"snapshot_id": "snap-1", "rows": 2, "written": 2, "unchanged": 0})

        state = config_backup_delivery_state(self.sync)

        self.assertIn(
            "Forward returned a configuration for 2 of 3 managed devices",
            config_backup_shortfall_sentence(state),
        )

    def test_no_job_at_all_is_reported_as_none_found_not_as_an_empty_backup(self):
        state = config_backup_delivery_state(self.sync)

        self.assertIsNone(state["last_result"])
        self.assertEqual(state["config_backup_jobs_found"], 0)
        self.assertIsNone(config_backup_shortfall_sentence(state))

    def test_only_counts_ids_and_timestamps_are_carried(self):
        self._record(self._run(rows=ROWS), warnings=["hostname router-1 secret"])

        state = config_backup_delivery_state(self.sync)

        self.assertNotIn("warnings", state["last_result"])
        self.assertNotIn("secret", repr(state["last_result"]))

    def test_the_delivery_check_carries_the_sentence(self):
        from forward_netbox.utilities.health import _config_backup_delivery_check

        self._third_device_with_no_config()
        self._record(self._run(rows=ROWS))

        check = _config_backup_delivery_check(self.sync)

        self.assertIn("2 of 3 managed devices", check["message"])

    def test_the_bundle_carries_the_counts_and_the_sentence(self):
        self._third_device_with_no_config()
        self._record(self._run(rows=ROWS))

        payload = config_backup_delivery_bundle_payload(self.sync)

        self.assertEqual(payload["last_result"]["scoped_without_config"], 1)
        self.assertEqual(payload["last_result"]["files_in_folder"], 2)
        self.assertEqual(payload["in_scope_devices_now"], 3)
        self.assertEqual(payload["config_backup_jobs_found"], 1)
        self.assertNotIn("{{device.name}}", repr(payload))
        self.assertIn("2 of 3 managed devices", payload["shortfall"])

    def test_the_card_gives_the_exact_path_for_the_configured_folder(self):
        self._folder("net/configs")

        card = config_backup_card(config_backup_delivery_state(self.sync))

        self.assertTrue(card["enabled"])
        self.assertEqual(
            card["expected_device_config_path"], "net/configs/{{device.name}}.cfg"
        )
        self.assertTrue(card["path_prefix_customized"])

    def test_a_sync_without_config_backup_has_no_card(self):
        self.source.parameters = {"network_id": "net-1"}
        self.source.save()
        self.sync.refresh_from_db()

        self.assertEqual(
            config_backup_card(config_backup_delivery_state(self.sync)),
            {"enabled": False},
        )
        self.assertFalse(sync_health_summary(self.sync)["config_backup"]["enabled"])


class HealthTabRendersItTest(EvidenceBase):
    def setUp(self):
        super().setUp()
        self.admin = get_user_model().objects.create_superuser(
            username="cb-evidence-admin",
            password="TestPassword123!",
            email="cb-evidence@example.com",
        )

    def _health_page(self):
        self.client.force_login(self.admin)
        response = self.client.get(
            reverse(
                "plugins:forward_netbox:forwardsync_health",
                kwargs={"pk": self.sync.pk},
            )
        )
        self.assertEqual(response.status_code, 200)
        return response

    def test_the_tab_shows_the_card_the_counts_and_the_path_to_set(self):
        self._third_device_with_no_config()
        self._job(self._run(rows=ROWS).as_dict())

        response = self._health_page()

        self.assertContains(response, "Config Backup")
        self.assertContains(response, "Managed devices with no config from Forward")
        self.assertContains(
            response, "Forward returned a configuration for 2 of 3 managed devices"
        )
        self.assertContains(response, "configs/{{device.name}}.cfg")

    def test_the_tab_says_when_no_backup_job_exists(self):
        response = self._health_page()

        self.assertContains(response, "No config backup job exists for this sync")


class BundleCarriesItTest(EvidenceBase):
    def test_the_bundle_carries_the_card_and_a_value_free_delivery_payload(self):
        import json

        from forward_netbox.views import _sync_support_bundle_payload

        self._folder("net/configs")
        self._third_device_with_no_config()
        self._job(self._run(rows=ROWS).as_dict())

        payload = _sync_support_bundle_payload(self.sync)

        card = payload["health"]["config_backup"]
        self.assertTrue(card["enabled"])
        self.assertIn("2 of 3 managed devices", card["shortfall"])
        self.assertEqual(card["last_result"]["scoped_without_config"], 1)
        delivery = payload["config_backup_delivery"]
        self.assertEqual(delivery["last_result"]["scoped_without_config"], 1)
        self.assertEqual(delivery["last_result"]["files_in_folder"], 2)
        # The delivery payload stays value-free: the folder is only a boolean.
        self.assertTrue(delivery["path_prefix_customized"])
        self.assertNotIn("net/configs", json.dumps(delivery, default=str))
