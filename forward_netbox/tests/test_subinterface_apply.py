from unittest.mock import Mock
from unittest.mock import patch

from dcim.models import Device
from dcim.models import DeviceRole
from dcim.models import DeviceType
from dcim.models import Interface
from dcim.models import Manufacturer
from dcim.models import Site
from django.test import TestCase

from forward_netbox.models import ForwardSource
from forward_netbox.models import ForwardSync
from forward_netbox.utilities.apply_engine_bulk import bulk_orm_apply_interface
from forward_netbox.utilities.sync import ForwardSyncRunner


class SubinterfaceFixture(TestCase):
    """A subinterface row carries `parent`, the name of an interface on its device.

    The requirement: the NetBox interface for the subinterface is linked to its
    parent, whichever order the rows arrive in; a subinterface never rewrites a
    top-level interface that happens to share its name; a subinterface whose
    parent does not exist is skipped, not created parentless.
    """

    def setUp(self):
        source = ForwardSource.objects.create(
            name="sub-source",
            type="saas",
            url="https://fwd.app",
            status="ready",
            parameters={
                "username": "u@example.com",
                "password": "p",
                "verify": True,
                "network_id": "net-1",
            },
        )
        self.sync = ForwardSync.objects.create(
            name="sub-sync",
            source=source,
            parameters={"snapshot_id": "latestProcessed"},
        )
        mfr = Manufacturer.objects.create(name="MfrS", slug="mfr-s")
        dt = DeviceType.objects.create(manufacturer=mfr, model="dt-s", slug="dt-s")
        role = DeviceRole.objects.create(name="RoleS", slug="role-s")
        site = Site.objects.create(name="SiteS", slug="site-s")
        self.device = Device.objects.create(
            name="dev-s", device_type=dt, role=role, site=site
        )

    def _runner(self):
        return ForwardSyncRunner(
            sync=self.sync, ingestion=None, client=None, logger_=Mock()
        )

    def _apply(self, rows):
        bulk_orm_apply_interface(self._runner(), rows)

    def _parent_row(self, name="Ethernet1/52"):
        return {
            "device": "dev-s",
            "name": name,
            "type": "100gbase-x-qsfp28",
            "enabled": True,
        }

    def _child_row(self, name="Ethernet1/52.100", parent="Ethernet1/52"):
        return {
            "device": "dev-s",
            "name": name,
            "type": "virtual",
            "enabled": True,
            "parent": parent,
        }


class SubinterfaceApplyTest(SubinterfaceFixture):
    def test_parent_row_before_child_links_the_child(self):
        self._apply([self._parent_row(), self._child_row()])

        child = Interface.objects.get(device=self.device, name="Ethernet1/52.100")
        self.assertEqual(child.parent.name, "Ethernet1/52")
        self.assertEqual(child.type, "virtual")

    def test_child_row_before_parent_links_the_child(self):
        # Row order is query order; nothing sorts parents first.
        self._apply([self._child_row(), self._parent_row()])

        child = Interface.objects.get(device=self.device, name="Ethernet1/52.100")
        self.assertEqual(child.parent.name, "Ethernet1/52")

    def test_parent_already_in_netbox_links_the_child(self):
        Interface.objects.create(
            device=self.device, name="Ethernet1/52", type="100gbase-x-qsfp28"
        )

        self._apply([self._child_row()])

        child = Interface.objects.get(device=self.device, name="Ethernet1/52.100")
        self.assertEqual(child.parent.name, "Ethernet1/52")

    def test_many_children_on_one_parent_all_link(self):
        rows = [self._parent_row()] + [
            self._child_row(name=f"Ethernet1/52.{vlan}") for vlan in range(100, 110)
        ]

        self._apply(rows)

        children = Interface.objects.filter(
            device=self.device, parent__name="Ethernet1/52"
        )
        self.assertEqual(children.count(), 10)

    def test_a_second_apply_changes_nothing(self):
        rows = [self._parent_row(), self._child_row()]
        self._apply(rows)
        before = {
            row["name"]: (row["pk"], row["parent_id"], row["type"])
            for row in Interface.objects.filter(device=self.device).values(
                "name", "pk", "parent_id", "type"
            )
        }

        self._apply(rows)

        after = {
            row["name"]: (row["pk"], row["parent_id"], row["type"])
            for row in Interface.objects.filter(device=self.device).values(
                "name", "pk", "parent_id", "type"
            )
        }
        self.assertEqual(before, after)

    def test_missing_parent_skips_the_child(self):
        self._apply([self._child_row()])

        self.assertFalse(
            Interface.objects.filter(
                device=self.device, name="Ethernet1/52.100"
            ).exists()
        )

    def test_same_name_as_a_top_level_interface_does_not_rewrite_it(self):
        # `(device, name)` is the identity. A subinterface row named like an
        # existing top-level interface must not turn that interface virtual or
        # give it a parent.
        existing = Interface.objects.create(
            device=self.device, name="Ethernet1/52.100", type="1000base-t"
        )
        Interface.objects.create(
            device=self.device, name="Ethernet1/52", type="100gbase-x-qsfp28"
        )

        self._apply([self._child_row()])

        existing.refresh_from_db()
        self.assertEqual(existing.type, "1000base-t")
        self.assertIsNone(existing.parent)

    def test_rows_without_parent_behave_as_before(self):
        self._apply([self._parent_row()])

        interface = Interface.objects.get(device=self.device, name="Ethernet1/52")
        self.assertIsNone(interface.parent)


class SubinterfaceAdapterTest(SubinterfaceApplyTest):
    """The row-by-row adapter must agree with the bulk path."""

    def _apply(self, rows):
        from forward_netbox.exceptions import ForwardDependencySkipError
        from forward_netbox.utilities.sync_interface import apply_dcim_interface

        runner = self._runner()
        for row in rows:
            try:
                apply_dcim_interface(runner, row)
            except ForwardDependencySkipError:
                pass

    def test_child_row_before_parent_links_the_child(self):
        # The adapter applies rows in order and cannot defer a link, so a child
        # that arrives first is skipped; the bulk path resolves order itself.
        self._apply([self._child_row(), self._parent_row()])

        self.assertFalse(
            Interface.objects.filter(
                device=self.device, name="Ethernet1/52.100"
            ).exists()
        )


class SubinterfaceApplyEdgeTest(SubinterfaceFixture):
    """Cases a code review of the first version found unprotected."""

    def _existing(self):
        parent = Interface.objects.create(
            device=self.device, name="Ethernet1/52", type="100gbase-x-qsfp28"
        )
        other = Interface.objects.create(
            device=self.device, name="Ethernet1/53", type="100gbase-x-qsfp28"
        )
        child = Interface.objects.create(
            device=self.device,
            name="Ethernet1/52.100",
            type="virtual",
            parent=parent,
            enabled=True,
        )
        return parent, other, child

    def test_an_existing_child_whose_parent_changes_is_relinked(self):
        _parent, other, child = self._existing()

        bulk_orm_apply_interface(
            self._runner(), [self._child_row(parent="Ethernet1/53")]
        )

        child.refresh_from_db()
        self.assertEqual(child.parent_id, other.pk)

    def test_an_existing_child_whose_parent_is_missing_is_not_written(self):
        _parent, _other, child = self._existing()
        row = self._child_row(parent="Ethernet9/9")
        row["enabled"] = False

        bulk_orm_apply_interface(self._runner(), [row])

        child.refresh_from_db()
        self.assertTrue(child.enabled)
        self.assertIsNotNone(child.parent_id)

    def test_a_skipped_child_marks_its_dependents_failed(self):
        runner = self._runner()

        bulk_orm_apply_interface(runner, [self._child_row()])

        self.assertTrue(
            runner._dependency_failed("dcim.interface", ("dev-s", "Ethernet1/52.100"))
        )

    def test_a_top_level_row_in_the_same_batch_is_never_rewritten(self):
        # Neither order may let the subinterface row take the name or attach the
        # top-level interface to a parent.
        for rows in (
            [self._child_row(name="Ethernet1/54"), self._top_level("Ethernet1/54")],
            [self._top_level("Ethernet1/54"), self._child_row(name="Ethernet1/54")],
        ):
            Interface.objects.filter(device=self.device).delete()
            with self.subTest(order=[row.get("parent") for row in rows]):
                bulk_orm_apply_interface(self._runner(), [self._parent_row(), *rows])

                clash = Interface.objects.get(device=self.device, name="Ethernet1/54")
                self.assertEqual(clash.type, "1000base-t")
                self.assertIsNone(clash.parent_id)

    def _top_level(self, name):
        return {"device": "dev-s", "name": name, "type": "1000base-t", "enabled": True}

    def test_a_constraint_error_still_links_the_children(self):
        # The isolate fallback saves rows one at a time; the parent link must
        # still be written, or the next sync sees a parentless child with a
        # top-level name and skips it for good.
        from django.db import IntegrityError

        real = Interface.objects.bulk_create
        calls = {"n": 0}

        def flaky(objs, *args, **kwargs):
            calls["n"] += 1
            if calls["n"] == 1:
                raise IntegrityError("simulated")
            return real(objs, *args, **kwargs)

        with patch.object(Interface.objects, "bulk_create", side_effect=flaky):
            bulk_orm_apply_interface(
                self._runner(), [self._parent_row(), self._child_row()]
            )

        child = Interface.objects.get(device=self.device, name="Ethernet1/52.100")
        self.assertEqual(child.parent.name, "Ethernet1/52")

    def test_a_preview_writes_nothing_and_counts_a_relink_as_an_update(self):
        _parent, _other, child = self._existing()

        counts = bulk_orm_apply_interface(
            self._runner(),
            [self._child_row(parent="Ethernet1/53")],
            preview=True,
        )

        child.refresh_from_db()
        self.assertEqual(child.parent.name, "Ethernet1/52")
        self.assertEqual(counts["updates"], 1)

    def test_a_relinked_child_is_reported_to_the_branch_as_an_update(self):
        from forward_netbox.utilities import apply_engine_bulk

        _parent, other, child = self._existing()

        with (
            patch.object(apply_engine_bulk, "_branch_is_active", return_value=True),
            patch.object(apply_engine_bulk, "emit_branch_object_changes") as emit,
        ):
            bulk_orm_apply_interface(
                self._runner(), [self._child_row(parent="Ethernet1/53")]
            )

        updates = emit.call_args.args[1]
        emitted = [obj for obj in updates if getattr(obj, "pk", None) == child.pk]
        self.assertEqual(len(emitted), 1)
        self.assertEqual(emitted[0].parent_id, other.pk)

    def test_a_missing_parent_beats_a_lag_on_the_same_row(self):
        # The LAG pass must not pick up a row the parent pass rejected: it would
        # write an existing child, or bulk_update an unsaved new one.
        _parent, _other, child = self._existing()
        existing_row = self._child_row(parent="Ethernet9/9")
        existing_row["lag"] = "Po1"
        existing_row["enabled"] = False
        new_row = self._child_row(name="Ethernet1/52.200", parent="Ethernet9/9")
        new_row["lag"] = "Po1"

        bulk_orm_apply_interface(self._runner(), [existing_row, new_row])

        child.refresh_from_db()
        self.assertTrue(child.enabled)
        self.assertIsNone(child.lag_id)
        self.assertFalse(
            Interface.objects.filter(
                device=self.device, name="Ethernet1/52.200"
            ).exists()
        )

    def test_a_child_is_relinked_to_a_parent_created_in_the_same_batch(self):
        _parent, _other, child = self._existing()

        bulk_orm_apply_interface(
            self._runner(),
            [self._child_row(parent="Ethernet1/60"), self._parent_row("Ethernet1/60")],
        )

        child.refresh_from_db()
        self.assertEqual(child.parent.name, "Ethernet1/60")

    def test_a_subinterface_cannot_take_a_top_level_name_in_another_spelling(self):
        bulk_orm_apply_interface(
            self._runner(),
            [
                self._parent_row(),
                self._top_level("Port-channel1"),
                {**self._child_row(name="po1"), "parent": "Ethernet1/52"},
            ],
        )

        clash = Interface.objects.filter(
            device=self.device, name__in=["Port-channel1", "po1"]
        )
        self.assertEqual(clash.count(), 1)
        self.assertIsNone(clash.get().parent_id)

    def test_a_child_whose_parent_could_not_be_saved_is_not_left_parentless(self):
        # Left in place with no parent, the next sync would take it for a
        # top-level interface and skip it for good.
        from django.db import IntegrityError

        real_create = Interface.objects.bulk_create
        real_save = Interface.save
        state = {"first": True}

        def flaky_create(objs, *args, **kwargs):
            if state["first"]:
                state["first"] = False
                raise IntegrityError("simulated")
            return real_create(objs, *args, **kwargs)

        def save(instance, *args, **kwargs):
            if instance.name == "Ethernet1/52":
                raise IntegrityError("parent cannot be saved")
            return real_save(instance, *args, **kwargs)

        with (
            patch.object(Interface.objects, "bulk_create", side_effect=flaky_create),
            patch.object(Interface, "save", autospec=True, side_effect=save),
        ):
            bulk_orm_apply_interface(
                self._runner(), [self._parent_row(), self._child_row()]
            )

        self.assertFalse(
            Interface.objects.filter(
                device=self.device, name="Ethernet1/52.100"
            ).exists()
        )
