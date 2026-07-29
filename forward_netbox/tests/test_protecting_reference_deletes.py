"""A delete the database will refuse must be identified before it is attempted.

Delete protection was plugin-ownership based only, so a row still referenced by
a PROTECT foreign key was scheduled anyway and failed at apply time with
`ProtectedError`. That is how a sync left a device undeleted with two surviving
BGP peers and never reached convergence.

A reference only blocks when the referencing row survives the run. When it is
being deleted too, ordering resolves it and the parent delete must proceed.
"""

from types import SimpleNamespace

from django.test import TestCase
from netbox_branching.merge_strategies.squash import ActionType

from dcim.models import Device
from dcim.models import Interface
from forward_netbox.utilities.bulk_merge import protecting_reference_blocked_deletes


def _protected_pair():
    """Build a real parent/child pair joined by a PROTECT foreign key.

    Asserted rather than assumed, so the test fails loudly if the relation ever
    stops being PROTECT instead of silently proving nothing.
    """
    from django.db import models as django_models
    from dcim.models import DeviceRole, DeviceType, Manufacturer, Site

    field = Device._meta.get_field("device_type")
    assert field.remote_field.on_delete is django_models.PROTECT, (
        "dcim.Device.device_type is no longer PROTECT; pick another relation"
    )

    manufacturer = Manufacturer.objects.create(name="Acme", slug="acme")
    device_type = DeviceType.objects.create(
        manufacturer=manufacturer, model="Model-1", slug="model-1"
    )
    site = Site.objects.create(name="Site-1", slug="site-1")
    role = DeviceRole.objects.create(name="Role-1", slug="role-1")
    device = Device.objects.create(
        name="device-1", device_type=device_type, role=role, site=site
    )
    return device_type, device, "device_type"


def _delete(model_class, pk):
    return SimpleNamespace(
        final_action=ActionType.DELETE,
        model_class=model_class,
        key=(model_class._meta.label_lower, pk),
    )


def _update(model_class, pk):
    return SimpleNamespace(
        final_action=ActionType.UPDATE,
        model_class=model_class,
        key=(model_class._meta.label_lower, pk),
    )


class ProtectingReferenceDeleteTest(TestCase):
    def _collapsed(self, *changes):
        return {change.key: change for change in changes}

    def test_no_deletes_is_a_no_op(self):
        self.assertEqual(
            protecting_reference_blocked_deletes(
                self._collapsed(_update(Device, 1))
            ),
            {},
        )

    def test_empty_change_set_is_a_no_op(self):
        self.assertEqual(protecting_reference_blocked_deletes({}), {})

    def test_a_delete_with_no_references_is_not_blocked(self):
        # Nothing exists in the database, so nothing can protect it.
        self.assertEqual(
            protecting_reference_blocked_deletes(
                self._collapsed(_delete(Device, 424242))
            ),
            {},
        )

    def test_only_protect_and_restrict_relations_are_considered(self):
        # Interface.device is CASCADE, so an interface never blocks its device.
        blocked = protecting_reference_blocked_deletes(
            self._collapsed(_delete(Device, 424242), _delete(Interface, 999999))
        )
        self.assertEqual(blocked, {})

    def test_a_surviving_protect_reference_blocks_the_delete(self):
        # The reported failure in miniature: a parent scheduled for deletion
        # while a PROTECT child survives the run.
        parent, child, field_name = _protected_pair()
        blocked = protecting_reference_blocked_deletes(
            self._collapsed(_delete(type(parent), parent.pk))
        )
        key = (type(parent)._meta.label_lower, parent.pk)
        self.assertIn(key, blocked, "a surviving PROTECT reference must block")
        labels = {label for label, _count in blocked[key]}
        self.assertIn(type(child)._meta.label, labels)

    def test_deleting_the_referencing_row_too_unblocks_the_parent(self):
        # Ordering resolves this case, so the parent delete must proceed.
        parent, child, field_name = _protected_pair()
        blocked = protecting_reference_blocked_deletes(
            self._collapsed(
                _delete(type(parent), parent.pk),
                _delete(type(child), child.pk),
            )
        )
        self.assertEqual(blocked, {})

    def test_the_scan_batches_large_delete_sets(self):
        # Exercises the chunking path rather than asserting a query count.
        changes = self._collapsed(
            *[_delete(Device, pk) for pk in range(500000, 506001)]
        )
        self.assertEqual(protecting_reference_blocked_deletes(changes), {})
