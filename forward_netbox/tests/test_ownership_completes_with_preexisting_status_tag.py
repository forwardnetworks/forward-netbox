# The customer's live failure, end to end.
#
# `forward-backfilled` existed in their NetBox with no ForwardManagedDeviceTag
# row. Status-tag reconciliation refused to adopt it on EVERY run, so the
# ownership domain never completed, convergence stayed blocked, and every drift
# figure read "Not measured" with no remedy available to them.
#
# The unit test for the fix covers `_ensure_managed_tag` in isolation. That is
# not the thing that was broken for them: the job is what failed, and a helper
# passing says nothing about whether the DOMAIN now reaches COMPLETED. These
# tests drive the reconciliation the job calls and assert on the recorded
# domain state, which is what the Drift Report reads.
from dcim.models import Device
from dcim.models import DeviceRole
from dcim.models import DeviceType
from dcim.models import Manufacturer
from dcim.models import Site
from django.test import TestCase
from extras.models import Tag

from forward_netbox.models import ForwardDeviceTagClaim
from forward_netbox.models import ForwardIngestion
from forward_netbox.models import ForwardManagedDeviceTag
from forward_netbox.models import ForwardOwnershipReconciliation
from forward_netbox.models import ForwardPreservedDeviceTagAssignment
from forward_netbox.models import ForwardSource
from forward_netbox.models import ForwardSync
from forward_netbox.utilities.scope_reconciliation import (
    _apply_maintained_device_tag,
)

BACKFILLED_SLUG = "forward-backfilled"


class OwnershipCompletesWithPreexistingStatusTagTest(TestCase):
    def setUp(self):
        source = ForwardSource.objects.create(
            name="e2e-src",
            type="saas",
            url="https://fwd.app",
            parameters={"network_id": "net-1"},
        )
        self.sync = ForwardSync.objects.create(name="e2e-sync", source=source)
        self.ingestion = ForwardIngestion.objects.create(
            sync=self.sync, snapshot_id="snap-e2e"
        )
        site = Site.objects.create(name="e2e-site", slug="e2e-site")
        manufacturer = Manufacturer.objects.create(name="e2e-mfr", slug="e2e-mfr")
        device_type = DeviceType.objects.create(
            manufacturer=manufacturer, model="e2e", slug="e2e"
        )
        role = DeviceRole.objects.create(name="e2e-role", slug="e2e-role")
        self.device = Device.objects.create(
            name="e2e-backfilled-device",
            site=site,
            device_type=device_type,
            role=role,
        )
        self.operator_device = Device.objects.create(
            name="e2e-operator-device",
            site=site,
            device_type=device_type,
            role=role,
        )

    def _reconcile(self, device_names):
        return _apply_maintained_device_tag(
            self.sync,
            device_names,
            slug=BACKFILLED_SLUG,
            name="Forward Backfilled",
            color="f44336",
            description="",
            claim_type="backfilled",
            generation=self.ingestion.pk,
            snapshot_id=self.ingestion.snapshot_id,
        )

    def _status_domain(self):
        return ForwardOwnershipReconciliation.objects.filter(
            sync=self.sync,
            domain=ForwardOwnershipReconciliation.Domain.STATUS_TAGS,
        ).first()

    def test_the_domain_completes_when_the_tag_already_exists_unowned(self):
        # Exactly the customer's state: the tag is present, nothing owns it.
        Tag.objects.create(name="Forward Backfilled", slug=BACKFILLED_SLUG)

        self._reconcile({self.device.name})

        domain = self._status_domain()
        self.assertIsNotNone(domain, "status-tag domain was never recorded")
        self.assertEqual(
            domain.status,
            ForwardOwnershipReconciliation.Status.COMPLETED,
            "the ownership domain did not complete, so convergence stays "
            "blocked and drift reads 'Not measured'",
        )
        self.assertTrue(
            self.device.tags.filter(slug=BACKFILLED_SLUG).exists(),
            "the backfilled device was never tagged",
        )

    def test_it_completes_again_on_the_next_run(self):
        # The failure repeated every run, so once is not evidence.
        Tag.objects.create(name="Forward Backfilled", slug=BACKFILLED_SLUG)
        for _ in range(3):
            self._reconcile({self.device.name})
        self.assertEqual(
            self._status_domain().status,
            ForwardOwnershipReconciliation.Status.COMPLETED,
        )
        self.assertEqual(
            ForwardManagedDeviceTag.objects.filter(tag__slug=BACKFILLED_SLUG).count(),
            1,
        )

    def test_an_operators_own_assignment_survives_adoption(self):
        # Adoption takes over a tag the operator already used. What they put on
        # their own devices must not be swept away by that.
        tag = Tag.objects.create(name="Forward Backfilled", slug=BACKFILLED_SLUG)
        self.operator_device.tags.add(tag)

        self._reconcile({self.device.name})

        self.assertTrue(
            self.operator_device.tags.filter(slug=BACKFILLED_SLUG).exists(),
            "adoption removed a tag assignment the operator made themselves",
        )
        self.assertTrue(
            ForwardPreservedDeviceTagAssignment.objects.filter(
                device=self.operator_device, tag=tag
            ).exists(),
            "the operator's assignment was not recorded as preserved, so "
            "releasing ownership would not restore it",
        )

    def test_a_device_leaving_backfilled_state_releases_the_plugins_claim(self):
        Tag.objects.create(name="Forward Backfilled", slug=BACKFILLED_SLUG)
        self._reconcile({self.device.name})
        self.assertTrue(self.device.tags.filter(slug=BACKFILLED_SLUG).exists())

        # Collection succeeds next run, so the device is no longer backfilled.
        self._reconcile(set())

        # The CLAIM is what this sync owns, and it is released.
        self.assertFalse(
            ForwardDeviceTagClaim.objects.filter(device=self.device).exists()
        )
        self.assertEqual(
            self._status_domain().status,
            ForwardOwnershipReconciliation.Status.COMPLETED,
        )
        # The tag ASSIGNMENT is deliberately not asserted here. Removing it is
        # gated on `_domain_is_current` (ownership.py:637), which compares
        # against the sync's promoted baseline - this fixture never promotes
        # one, so the removal correctly does not run. That gate is what stops
        # one sync stripping a tag another sync still claims, and it deserves
        # its own test rather than being incidentally exercised here.
