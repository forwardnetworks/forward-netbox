# A run deleted hundreds of prefixes and nothing in the bundle said why. A prefix
# is derived from the addresses on in-scope device interfaces, so when a device
# fails collection its prefixes vanish. The bundle counts how many deleted
# prefixes an uncollected device still has an address in. Synthetic data.
from datetime import timedelta
from uuid import uuid4

from core.choices import JobStatusChoices
from core.models import Job
from core.models import ObjectChange
from dcim.models import Device
from dcim.models import DeviceRole
from dcim.models import DeviceType
from dcim.models import Interface
from dcim.models import Manufacturer
from dcim.models import Site
from django.contrib.contenttypes.models import ContentType
from django.utils import timezone
from ipam.models import IPAddress
from ipam.models import Prefix
from ipam.models import VRF

from forward_netbox.tests.test_health_evidence import EvidenceTestBase
from forward_netbox.utilities.health_evidence import recent_ingestions_evidence


class PrefixDeleteCauseTest(EvidenceTestBase):
    def setUp(self):
        super().setUp()
        site = Site.objects.create(name="pc-site", slug="pc-site")
        maker = Manufacturer.objects.create(name="pc-mfr", slug="pc-mfr")
        kind = DeviceType.objects.create(manufacturer=maker, model="pc", slug="pc")
        role = DeviceRole.objects.create(name="pc-role", slug="pc-role")
        self.device = Device.objects.create(
            name="pc-dev", site=site, device_type=kind, role=role
        )
        interface = Interface.objects.create(
            device=self.device, name="eth0", type="1000base-t"
        )
        IPAddress.objects.create(address="10.1.1.1/24", assigned_object=interface)

    def _scope_report(self, device_ids, *, after=True, data=None):
        now = timezone.now()
        shift = timedelta(minutes=10) if after else -timedelta(hours=2)
        Job.objects.create(
            object_type=self.content_type,
            object_id=self.sync.pk,
            name="scope reconciliation",
            status=JobStatusChoices.STATUS_COMPLETED,
            job_id=uuid4(),
            created=now,
            started=now,
            completed=now + shift,
            data=(
                data
                if data is not None
                else {"present_backfilled_device_ids": device_ids}
            ),
        )

    def _merged(self):
        return self._ingestion(started=timezone.now(), merge_applied_at=timezone.now())

    def _deleted_prefix(self, ingestion, prefix, vrf=None):
        # The change-log row as NetBox itself serialises a prefix.
        row = Prefix.objects.create(prefix=prefix, vrf=vrf)
        ObjectChange.objects.create(
            request_id=ingestion.change_request_id,
            user_name="forward",
            changed_object_type=ContentType.objects.get_for_model(Prefix),
            changed_object_id=row.pk,
            object_repr=prefix,
            action="delete",
            prechange_data=row.serialize_object(),
        )
        row.delete()

    def _cause(self):
        (entry,) = recent_ingestions_evidence(self.sync)["ingestions"]
        return entry["prefix_delete_cause"]

    def test_prefixes_an_uncollected_device_still_has_an_address_in_are_counted(self):
        ingestion = self._merged()
        self._scope_report([self.device.pk])
        self._deleted_prefix(ingestion, "10.1.1.0/24")
        self._deleted_prefix(ingestion, "10.9.9.0/24")
        self._deleted_prefix(ingestion, "10.8.8.0/24")

        cause = self._cause()

        self.assertEqual(cause["deleted_prefixes"], 3)
        self.assertEqual(cause["hosted_by_uncollected_device"], 1)
        self.assertEqual(cause["not_explained_by_uncollected_devices"], 2)
        self.assertEqual(cause["uncollected_devices_in_scope"], 1)
        self.assertIsNotNone(cause["scope_report_generated_at"])

    def test_the_vrf_must_match(self):
        vrf = VRF.objects.create(name="pc-vrf")
        other = VRF.objects.create(name="pc-other")
        interface = Interface.objects.get(device=self.device)
        IPAddress.objects.create(
            address="10.2.2.1/24", vrf=vrf, assigned_object=interface
        )
        ingestion = self._merged()
        self._scope_report([self.device.pk])
        self._deleted_prefix(ingestion, "10.2.2.0/24", vrf)
        self._deleted_prefix(ingestion, "10.2.2.0/24", other)
        self._deleted_prefix(ingestion, "10.2.2.0/24", None)

        self.assertEqual(self._cause()["hosted_by_uncollected_device"], 1)

    def test_an_ipv6_prefix_matches(self):
        interface = Interface.objects.get(device=self.device)
        IPAddress.objects.create(address="2001:db8:1::1/64", assigned_object=interface)
        ingestion = self._merged()
        self._scope_report([self.device.pk])
        self._deleted_prefix(ingestion, "2001:db8:1::/64")

        self.assertEqual(self._cause()["hosted_by_uncollected_device"], 1)

    def test_no_uncollected_devices_explains_nothing(self):
        ingestion = self._merged()
        self._scope_report([])
        self._deleted_prefix(ingestion, "10.1.1.0/24")

        self.assertEqual(self._cause()["hosted_by_uncollected_device"], 0)

    def test_a_report_computed_before_the_run_is_not_used(self):
        ingestion = self._merged()
        self._scope_report([self.device.pk], after=False)
        self._deleted_prefix(ingestion, "10.1.1.0/24")

        cause = self._cause()

        self.assertNotIn("hosted_by_uncollected_device", cause)
        self.assertIn("No scope report computed after this run", cause["note"])

    def test_a_report_without_the_device_list_says_so_instead_of_zero(self):
        ingestion = self._merged()
        self._scope_report([], data={"forward_in_scope_completed": 5})
        self._deleted_prefix(ingestion, "10.1.1.0/24")

        self.assertNotIn("hosted_by_uncollected_device", self._cause())

    def test_an_unmerged_run_reports_no_counts(self):
        ingestion = self._ingestion(started=timezone.now())
        self._scope_report([self.device.pk])
        self._deleted_prefix(ingestion, "10.1.1.0/24")

        cause = self._cause()

        self.assertNotIn("deleted_prefixes", cause)
        self.assertIn("not merged", cause["note"])

    def test_a_delete_with_no_recorded_prefix_is_counted_apart(self):
        ingestion = self._merged()
        self._scope_report([self.device.pk])
        ObjectChange.objects.create(
            request_id=ingestion.change_request_id,
            user_name="forward",
            changed_object_type=ContentType.objects.get_for_model(Prefix),
            changed_object_id=1,
            object_repr="x",
            action="delete",
        )

        cause = self._cause()

        self.assertEqual(cause["deletes_without_recorded_prefix"], 1)
        self.assertEqual(cause["not_explained_by_uncollected_devices"], 0)

    def test_no_prefix_or_device_name_is_carried(self):
        ingestion = self._merged()
        self._scope_report([self.device.pk])
        self._deleted_prefix(ingestion, "10.1.1.0/24")

        rendered = repr(self._cause())

        self.assertNotIn("10.1.1", rendered)
        self.assertNotIn("pc-dev", rendered)
