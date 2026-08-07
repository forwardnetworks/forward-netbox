# `forward-backfilled` exists in any deployment that has ever had a collection
# failure. If its `ForwardManagedDeviceTag` row is absent - an operator made the
# tag by hand, it predates the managed-tag registry, or the row was cleaned up
# while the tag survived - status-tag reconciliation used to raise
# OwnershipConflictError on EVERY run.
#
# Nothing cleared that. The ownership domain never completed, so convergence
# stayed blocked and every drift figure read "Not measured", run after run. A
# customer sat in exactly that state.
#
# The override that was supposed to permit adoption was passed `tag_created`,
# true only when the plugin had just created the tag - precisely when there is
# nothing to adopt. So it could never fire when it was needed.
from dcim.models import Device
from dcim.models import DeviceRole
from dcim.models import DeviceType
from dcim.models import Manufacturer
from dcim.models import Site
from django.test import TestCase
from extras.models import Tag

from forward_netbox.models import ForwardManagedDeviceTag
from forward_netbox.models import ForwardPreservedDeviceTagAssignment
from forward_netbox.utilities.ownership import _ensure_managed_tag


class ReservedStatusTagAdoptionTest(TestCase):
    def setUp(self):
        site = Site.objects.create(name="adopt-site", slug="adopt-site")
        manufacturer = Manufacturer.objects.create(name="adopt-mfr", slug="adopt-mfr")
        device_type = DeviceType.objects.create(
            manufacturer=manufacturer, model="adopt-model", slug="adopt-model"
        )
        role = DeviceRole.objects.create(name="adopt-role", slug="adopt-role")
        self.plugin_device = Device.objects.create(
            name="adopt-plugin", site=site, device_type=device_type, role=role
        )
        self.operator_device = Device.objects.create(
            name="adopt-operator", site=site, device_type=device_type, role=role
        )

    def test_a_preexisting_reserved_tag_is_adopted_not_refused(self):
        tag = Tag.objects.create(name="Forward Backfilled", slug="forward-backfilled")

        managed = _ensure_managed_tag(
            tag,
            "backfilled",
            plugin_assignment_ids={self.plugin_device.pk},
        )

        self.assertEqual(managed.tag_id, tag.pk)
        self.assertEqual(managed.claim_type, "backfilled")

    def test_adoption_preserves_what_the_operator_had_tagged(self):
        # Refusing was never protecting these; the preservation path is what
        # protects them, and the refusal ran before it could.
        tag = Tag.objects.create(name="Forward Backfilled", slug="forward-backfilled")
        self.operator_device.tags.add(tag)

        _ensure_managed_tag(
            tag,
            "backfilled",
            plugin_assignment_ids={self.plugin_device.pk},
        )

        self.assertTrue(
            ForwardPreservedDeviceTagAssignment.objects.filter(
                device=self.operator_device, tag=tag
            ).exists()
        )

    def test_the_plugins_own_assignments_are_not_recorded_as_preserved(self):
        tag = Tag.objects.create(name="Forward Backfilled", slug="forward-backfilled")
        self.plugin_device.tags.add(tag)

        _ensure_managed_tag(
            tag,
            "backfilled",
            plugin_assignment_ids={self.plugin_device.pk},
        )

        self.assertFalse(
            ForwardPreservedDeviceTagAssignment.objects.filter(
                device=self.plugin_device
            ).exists()
        )

    def test_adoption_is_idempotent_across_runs(self):
        # The failure repeated every run, so the fix has to hold every run.
        tag = Tag.objects.create(name="Forward Backfilled", slug="forward-backfilled")
        for _ in range(3):
            _ensure_managed_tag(
                tag, "backfilled", plugin_assignment_ids={self.plugin_device.pk}
            )
        self.assertEqual(ForwardManagedDeviceTag.objects.filter(tag=tag).count(), 1)

    def test_a_tag_claimed_by_another_claim_type_still_refuses(self):
        # The genuine conflict is unchanged: one tag cannot be two claim types.
        tag = Tag.objects.create(name="Forward Backfilled", slug="forward-backfilled")
        ForwardManagedDeviceTag.objects.create(tag=tag, claim_type="scope")

        from forward_netbox.utilities.ownership import OwnershipConflictError

        with self.assertRaises(OwnershipConflictError):
            _ensure_managed_tag(tag, "backfilled")
