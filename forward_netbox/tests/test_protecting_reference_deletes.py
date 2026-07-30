"""A delete the database will refuse must be identified before it is attempted.

Delete protection was plugin-ownership based only, so a row still referenced by
a PROTECT foreign key was scheduled anyway and failed at apply time with
`ProtectedError`. That is how a sync left a device undeleted with two surviving
BGP peers and never reached convergence.

A reference only blocks when the referencing row survives the run. When it is
being deleted too, ordering resolves it and the parent delete must proceed.
"""

import logging
from types import SimpleNamespace
from unittest.mock import patch

from dcim.models import Device
from dcim.models import Interface
from django.test import TestCase
from netbox_branching.merge_strategies.squash import ActionType
from netbox_branching.merge_strategies.squash import CollapsedChange
from netbox_branching.merge_strategies.squash import SquashMergeStrategy

from forward_netbox.utilities import bulk_merge
from forward_netbox.utilities.bulk_merge import _acyclic_delete_edges
from forward_netbox.utilities.bulk_merge import _add_protected_child_delete_dependencies
from forward_netbox.utilities.bulk_merge import _protected_child_delete_edges
from forward_netbox.utilities.bulk_merge import describe_protecting_references
from forward_netbox.utilities.bulk_merge import protecting_reference_blocked_deletes


def _protected_pair():
    """Build a real parent/child pair joined by a PROTECT foreign key.

    Asserted rather than assumed, so the test fails loudly if the relation ever
    stops being PROTECT instead of silently proving nothing.
    """
    from django.db import models as django_models
    from dcim.models import DeviceRole, DeviceType, Manufacturer, Site

    field = Device._meta.get_field("device_type")
    assert (
        field.remote_field.on_delete is django_models.PROTECT
    ), "dcim.Device.device_type is no longer PROTECT; pick another relation"

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
            protecting_reference_blocked_deletes(self._collapsed(_update(Device, 1))),
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

    def test_the_diagnostic_names_the_protecting_model(self):
        parent, child, _field = _protected_pair()
        references = describe_protecting_references(type(parent), parent.pk)
        labels = {label for label, _count in references}
        self.assertIn(type(child)._meta.label, labels)

    def test_the_diagnostic_is_empty_for_an_unreferenced_row(self):
        self.assertEqual(describe_protecting_references(Device, 424242), [])

    def test_the_diagnostic_never_raises(self):
        # A diagnostic must not convert a recorded failure into an unhandled one.
        class _Broken:
            class _meta:  # noqa: N801 - mimics Django's Options attribute
                related_objects = property(
                    lambda self: (_ for _ in ()).throw(RuntimeError("boom"))
                )

        self.assertEqual(describe_protecting_references(_Broken, 1), [])

    def test_the_scan_batches_large_delete_sets(self):
        # Exercises the chunking path rather than asserting a query count.
        changes = self._collapsed(
            *[_delete(Device, pk) for pk in range(500000, 506001)]
        )
        self.assertEqual(protecting_reference_blocked_deletes(changes), {})


def _collapsed_delete(model_class, pk, *, time=0):
    """A branch-native bulk DELETE: no ``prechange_data`` preimage.

    That absence is the defect's precondition — the framework builds its
    child-before-parent DELETE edges out of exactly this field.
    """
    change = CollapsedChange((model_class._meta.label_lower, pk), model_class)
    change.final_action = ActionType.DELETE
    change.prechange_data = None
    change.last_change = SimpleNamespace(time=time)
    return change


class ProtectedChildDeleteOrderingTest(TestCase):
    """The parent DELETE must follow the child DELETE that protects it.

    Without the edge the parent is attempted first and the database refuses it,
    so the row survives every sync and never converges.
    """

    def _order(self, changes):
        with (
            patch.object(SquashMergeStrategy, "_build_fk_dependency_graph"),
            patch.object(bulk_merge.squash_dependency_graph_built, "send"),
        ):
            return bulk_merge._order_collapsed_changes_fast(
                changes,
                logging.getLogger("forward_netbox.tests.protected-delete-ordering"),
                "merge",
            )

    def test_child_delete_is_ordered_before_the_parent_it_protects(self):
        parent, child, _field = _protected_pair()
        parent_change = _collapsed_delete(type(parent), parent.pk, time=0)
        child_change = _collapsed_delete(type(child), child.pk, time=1)
        changes = {
            parent_change.key: parent_change,
            child_change.key: child_change,
        }

        ordered = [change.key for change in self._order(changes)]

        self.assertLess(
            ordered.index(child_change.key),
            ordered.index(parent_change.key),
            "the protecting child must be deleted before its parent",
        )

    def test_the_edge_is_restored_without_any_prechange_preimage(self):
        # Directly asserts the gap being closed: the framework's own step 3
        # reads prechange_data, which is None on both changes here.
        parent, child, _field = _protected_pair()
        parent_change = _collapsed_delete(type(parent), parent.pk)
        child_change = _collapsed_delete(type(child), child.pk)
        changes = {
            parent_change.key: parent_change,
            child_change.key: child_change,
        }

        added = _add_protected_child_delete_dependencies(
            changes, logging.getLogger("forward_netbox.tests.edge-restore")
        )

        self.assertEqual(added, 1)
        self.assertIn(child_change.key, parent_change.depends_on)
        self.assertIn(parent_change.key, child_change.depended_by)

    def test_a_surviving_child_gets_no_edge(self):
        # The reference outlives the run, so the delete must stay strictly
        # failing rather than be reordered into a false success.
        parent, _child, _field = _protected_pair()
        parent_change = _collapsed_delete(type(parent), parent.pk)
        changes = {parent_change.key: parent_change}

        added = _add_protected_child_delete_dependencies(
            changes, logging.getLogger("forward_netbox.tests.surviving-child")
        )

        self.assertEqual(added, 0)
        self.assertEqual(parent_change.depends_on, set())

    def test_cascade_relations_produce_no_edges(self):
        # Interface.device is CASCADE; the database handles it, so ordering
        # must not add work.
        self.assertEqual(
            _protected_child_delete_edges(
                {
                    change.key: change
                    for change in (
                        _collapsed_delete(Device, 424242),
                        _collapsed_delete(Interface, 999999),
                    )
                }
            ),
            [],
        )

    def test_a_cycle_is_dropped_rather_than_failing_the_whole_merge(self):
        # Mutually protecting rows are undeletable by the database anyway.
        # Raising here would abandon every other change in the merge.
        first = _collapsed_delete(Device, 1)
        second = _collapsed_delete(Device, 2)
        changes = {first.key: first, second.key: second}

        accepted = _acyclic_delete_edges(
            changes,
            [(first.key, second.key), (second.key, first.key)],
            logging.getLogger("forward_netbox.tests.cycle-drop"),
        )

        self.assertEqual(accepted, [])

    def test_an_acyclic_edge_set_is_kept_intact(self):
        first = _collapsed_delete(Device, 1)
        second = _collapsed_delete(Device, 2)
        third = _collapsed_delete(Device, 3)
        changes = {c.key: c for c in (first, second, third)}
        edges = [(first.key, second.key), (second.key, third.key)]

        accepted = _acyclic_delete_edges(
            changes,
            edges,
            logging.getLogger("forward_netbox.tests.cycle-keep"),
        )

        self.assertEqual(accepted, edges)

    def test_ordering_still_succeeds_when_the_graph_would_cycle(self):
        # End-to-end proof of the guard: a cyclic candidate set must not turn
        # into the "Cycle detected in dependency graph" merge abort.
        first = _collapsed_delete(Device, 1, time=0)
        second = _collapsed_delete(Device, 2, time=1)
        changes = {first.key: first, second.key: second}

        with patch.object(
            bulk_merge,
            "_protected_child_delete_edges",
            return_value=[(first.key, second.key), (second.key, first.key)],
        ):
            ordered = self._order(changes)

        self.assertEqual(len(ordered), 2)


class SkipProtectedBlockedDeletesTest(TestCase):
    """The identifier must actually be wired into the merge.

    `protecting_reference_blocked_deletes` shipped fully implemented and tested
    with **zero production call sites**, so a delete held by a surviving PROTECT
    reference was still scheduled and still failed with `ProtectedError` at apply
    time. A failed row permanently blocks baseline promotion, which meant one
    operator-owned object could wedge a sync's convergence bookkeeping.
    """

    def _collapsed(self, *changes):
        return {change.key: change for change in changes}

    def test_marks_a_blocked_delete_as_skip(self):
        device_type, device, _field = _protected_pair()
        # Only the parent is deleted; the device referencing it survives.
        parent = _delete(type(device_type), device_type.pk)
        changes = self._collapsed(parent)

        blocked = bulk_merge._skip_protecting_reference_blocked_deletes(
            changes, logging.getLogger("forward_netbox.tests.skip-blocked")
        )

        self.assertIn(parent.key, blocked)
        self.assertEqual(changes[parent.key].final_action, ActionType.SKIP)

    def test_leaves_a_delete_alone_when_the_reference_is_also_deleted(self):
        # Ordering resolves this pair; skipping it would lose a real delete.
        device_type, device, _field = _protected_pair()
        parent = _delete(type(device_type), device_type.pk)
        child = _delete(Device, device.pk)
        changes = self._collapsed(parent, child)

        blocked = bulk_merge._skip_protecting_reference_blocked_deletes(
            changes, logging.getLogger("forward_netbox.tests.skip-blocked")
        )

        self.assertEqual(blocked, {})
        self.assertEqual(changes[parent.key].final_action, ActionType.DELETE)
        self.assertEqual(changes[child.key].final_action, ActionType.DELETE)

    def test_no_deletes_is_a_no_op(self):
        changes = self._collapsed(_update(Device, 1))

        self.assertEqual(
            bulk_merge._skip_protecting_reference_blocked_deletes(
                changes, logging.getLogger("forward_netbox.tests.skip-blocked")
            ),
            {},
        )

    def test_the_merge_calls_it(self):
        # The defect being fixed was an unwired function, so pin the call site
        # itself: asserting only the helper's behaviour would pass again if a
        # refactor dropped the call.
        import inspect

        source = inspect.getsource(bulk_merge._bulk_merge_changes_main)

        self.assertIn("_skip_protecting_reference_blocked_deletes", source)


class UpdatedReferrerDoesNotBlockTest(TestCase):
    """An updated referrer may release the FK, so it must not block.

    The check reads destination state before ordering has applied anything, so
    a referrer that is about to release its FK still looks like it protects.
    Treating it as blocking skipped a delete that in fact succeeds — the
    regression that `test_dlm_protected_version_delete_follows_destination_fk_
    reassignment` caught.
    """

    def _collapsed(self, *changes):
        return {change.key: change for change in changes}

    def test_an_updated_referrer_does_not_block_the_delete(self):
        parent, child, _field = _protected_pair()

        blocked = protecting_reference_blocked_deletes(
            self._collapsed(
                _delete(type(parent), parent.pk),
                _update(type(child), child.pk),
            )
        )

        self.assertEqual(blocked, {})

    def test_an_unchanged_referrer_still_blocks(self):
        # The tightening must not swallow the case the skip exists for.
        parent, child, _field = _protected_pair()

        blocked = protecting_reference_blocked_deletes(
            self._collapsed(_delete(type(parent), parent.pk))
        )

        key = (type(parent)._meta.label_lower, parent.pk)
        self.assertIn(key, blocked)
