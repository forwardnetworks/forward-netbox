# The customer's live blocker on 2.7.6, named by the conflict catalogue as
# `tag-mutation-identity-unresolved`.
#
# Forward reported eleven devices carrying the include tags that did not exist
# in their NetBox, out of roughly 3400. `reconcile_source_device_tag_claims`
# refused the ENTIRE mutation if any name failed to resolve, so both the
# scope-tag and status-tag domains failed on every run: ownership never
# completed, convergence stayed blocked, and every drift figure read "Not
# measured" with no remedy available to them.
#
# The two resolution failures are deliberately treated differently, and these
# tests pin both halves:
#
#   missing   - no device of that name exists, so there is nothing to tag and
#               nothing to release. Skipping changes no NetBox row.
#   ambiguous - several devices share the name. `desired_ids` drives both the
#               add and the remove, so dropping the key could release a claim
#               from a device that holds one, or tag the wrong device.
from dcim.models import Device
from dcim.models import DeviceRole
from dcim.models import DeviceType
from dcim.models import Manufacturer
from dcim.models import Site
from django.test import TestCase

from forward_netbox.models import ForwardDeviceTagClaim
from forward_netbox.models import ForwardIngestion
from forward_netbox.models import ForwardOwnershipReconciliation
from forward_netbox.models import ForwardSource
from forward_netbox.models import ForwardSync
from forward_netbox.utilities.ownership import OwnershipConflictError
from forward_netbox.utilities.ownership import reconcile_source_device_tag_claims

SLUG = "b-chalasani"


class AbsentDeviceDoesNotBlockTagDomainTest(TestCase):
    def setUp(self):
        source = ForwardSource.objects.create(
            name="absent-src",
            type="saas",
            url="https://fwd.app",
            parameters={"network_id": "net-1"},
        )
        self.sync = ForwardSync.objects.create(name="absent-sync", source=source)
        self.ingestion = ForwardIngestion.objects.create(
            sync=self.sync, snapshot_id="snap-absent"
        )
        self.site = Site.objects.create(name="absent-site", slug="absent-site")
        manufacturer = Manufacturer.objects.create(name="absent-mfr", slug="absent-mfr")
        self.device_type = DeviceType.objects.create(
            manufacturer=manufacturer, model="am", slug="am"
        )
        self.role = DeviceRole.objects.create(name="absent-role", slug="absent-role")
        self.present = self._device("present-device")

    def _device(self, name, site=None):
        return Device.objects.create(
            name=name,
            site=site or self.site,
            device_type=self.device_type,
            role=self.role,
        )

    def _twin_named(self, name):
        """Two devices sharing a name. NetBox scopes name uniqueness to the
        site, so genuine ambiguity means the same name in two sites - which is
        precisely how it arises in a real estate."""
        second_site = Site.objects.create(
            name=f"{name}-other-site", slug=f"{name}-other-site"
        )
        return self._device(name), self._device(name, site=second_site)

    def _reconcile(self, names):
        return reconcile_source_device_tag_claims(
            self.sync,
            names,
            slug=SLUG,
            name="B.Chalasani",
            color="9e9e9e",
            description="",
            claim_type="scope",
            generation=self.ingestion.pk,
            snapshot_id=self.ingestion.snapshot_id,
        )

    def test_a_device_absent_from_netbox_is_skipped_not_fatal(self):
        # The customer's shape: most names resolve, a few do not exist at all.
        result = self._reconcile({"present-device", "never-existed"})

        self.assertEqual(result["skipped_absent_devices"], 1)
        self.assertTrue(
            ForwardDeviceTagClaim.objects.filter(device=self.present).exists(),
            "the devices that DO exist were not claimed",
        )

    def test_the_domain_completes_despite_an_absent_device(self):
        self._reconcile({"present-device", "never-existed"})

        domain = ForwardOwnershipReconciliation.objects.filter(
            sync=self.sync,
            domain=ForwardOwnershipReconciliation.Domain.SCOPE_TAGS,
        ).first()
        self.assertIsNotNone(domain)
        self.assertEqual(
            domain.status,
            ForwardOwnershipReconciliation.Status.COMPLETED,
            "ownership still did not complete, so convergence stays blocked",
        )

    def test_only_absent_names_still_completes(self):
        # Nothing resolves. There is still nothing unsafe about proceeding.
        result = self._reconcile({"never-existed", "also-never-existed"})
        self.assertEqual(result["skipped_absent_devices"], 2)
        self.assertEqual(result["total"], 0)

    def test_an_ambiguous_name_still_refuses(self):
        # Two devices share a name. Resolving it could tag the wrong one, and
        # dropping it could release a claim from one that holds it.
        self._twin_named("twin")

        with self.assertRaises(OwnershipConflictError) as caught:
            self._reconcile({"present-device", "twin"})

        message = str(caught.exception)
        self.assertIn("ambiguous", message)
        # Device names are customer data; the counts are what gets reported.
        self.assertNotIn("twin", message)

    def test_the_refusal_reports_both_counts(self):
        self._twin_named("twin")

        with self.assertRaises(OwnershipConflictError) as caught:
            self._reconcile({"twin", "never-existed", "also-never-existed"})

        message = str(caught.exception)
        self.assertIn("1", message)  # one ambiguous
        self.assertIn("2", message)  # two absent

    def test_a_fully_resolvable_run_is_unchanged(self):
        other = self._device("second-device")

        result = self._reconcile({"present-device", "second-device"})

        self.assertEqual(result["skipped_absent_devices"], 0)
        self.assertEqual(result["total"], 2)
        self.assertTrue(ForwardDeviceTagClaim.objects.filter(device=other).exists())
