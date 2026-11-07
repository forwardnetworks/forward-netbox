# A subinterface already in NetBox with its parent but a type that is not
# `virtual` (an earlier snapshot reported it as a physical or LAG type) must be
# brought to `virtual` by the sync, not rejected: NetBox only lets a virtual
# interface (or a channel subinterface) have a parent.
from dcim.models import Interface

from forward_netbox.tests.test_subinterface_apply import SubinterfaceFixture
from forward_netbox.utilities.sync_interface import apply_dcim_interface


class SubinterfaceRetypeTest(SubinterfaceFixture):
    def test_an_existing_child_with_a_physical_type_becomes_virtual(self):
        parent = Interface.objects.create(
            device=self.device, name="Ethernet1/52", type="100gbase-x-qsfp28"
        )
        child = Interface.objects.create(
            device=self.device, name="Ethernet1/52.100", type="1000base-t"
        )
        Interface.objects.filter(pk=child.pk).update(parent=parent)

        self._apply([self._parent_row(), self._child_row()])

        child.refresh_from_db()
        self.assertEqual(child.type, "virtual")
        self.assertEqual(child.parent_id, parent.pk)

    def test_a_subinterface_forward_now_lists_as_a_physical_top_level_one(self):
        # Forward reclassified it: a top-level, non-virtual row for an interface
        # NetBox still holds as a subinterface. Rejected before for a parent the
        # row does not carry; now the stale parent link is dropped.
        parent = Interface.objects.create(
            device=self.device, name="Ethernet1/52", type="100gbase-x-qsfp28"
        )
        child = Interface.objects.create(
            device=self.device, name="Ethernet1/52.100", type="virtual"
        )
        Interface.objects.filter(pk=child.pk).update(parent=parent)

        self._apply(
            [
                self._parent_row(),
                {
                    "device": "dev-s",
                    "name": "Ethernet1/52.100",
                    "type": "1000base-t",
                    "enabled": True,
                    "parent": None,
                },
            ]
        )

        child.refresh_from_db()
        self.assertEqual(child.type, "1000base-t")
        self.assertIsNone(child.parent_id)

    def test_a_virtual_interface_with_a_hand_set_parent_keeps_it(self):
        # Forward lists it top-level and virtual: nothing says the parent is
        # wrong, and the sync has never cleared one.
        parent = Interface.objects.create(
            device=self.device, name="Ethernet1/52", type="100gbase-x-qsfp28"
        )
        child = Interface.objects.create(
            device=self.device, name="vlan100", type="virtual"
        )
        Interface.objects.filter(pk=child.pk).update(parent=parent)

        self._apply(
            [
                self._parent_row(),
                {
                    "device": "dev-s",
                    "name": "vlan100",
                    "type": "virtual",
                    "enabled": False,
                    "parent": None,
                },
            ]
        )

        child.refresh_from_db()
        self.assertEqual(child.parent_id, parent.pk)
        self.assertFalse(child.enabled)

    def test_a_channel_subinterface_keeps_its_parent(self):
        # NetBox lets a non-virtual interface bound to a channel of its parent
        # keep that parent; the row says nothing about it, so it is not cleared.
        parent = Interface.objects.create(
            device=self.device, name="Ethernet1/52", type="100gbase-x-qsfp28"
        )
        child = Interface.objects.create(
            device=self.device, name="Ethernet1/52:1", type="25gbase-x-sfp28"
        )
        Interface.objects.filter(pk=parent.pk).update(channels=4)
        Interface.objects.filter(pk=child.pk).update(parent=parent, channel_id=1)

        self._apply(
            [
                self._parent_row(),
                {
                    "device": "dev-s",
                    "name": "Ethernet1/52:1",
                    "type": "25gbase-x-sfp28",
                    "enabled": False,
                    "parent": None,
                },
            ]
        )

        child.refresh_from_db()
        self.assertEqual(child.parent_id, parent.pk)
        self.assertFalse(child.enabled)

    def test_the_row_oriented_path_drops_the_stale_parent_too(self):
        parent = Interface.objects.create(
            device=self.device, name="Ethernet1/52", type="100gbase-x-qsfp28"
        )
        child = Interface.objects.create(
            device=self.device, name="Ethernet1/52.100", type="virtual"
        )
        Interface.objects.filter(pk=child.pk).update(parent=parent)

        apply_dcim_interface(
            self._runner(),
            {
                "device": "dev-s",
                "name": "Ethernet1/52.100",
                "type": "1000base-t",
                "enabled": True,
                "parent": None,
            },
        )

        child.refresh_from_db()
        self.assertEqual(child.type, "1000base-t")
        self.assertIsNone(child.parent_id)
