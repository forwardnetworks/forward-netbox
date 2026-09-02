# Slice four of the adapter-only drift comparison: `ipam.fhrpgroup`.
#
# One row means up to THREE persisted objects - the FHRPGroup, its virtual-IP
# IPAddress, and the FHRPGroupAssignment binding it to an interface - so the
# row's verdict is the strongest of the three. Unchanged only when all three
# already match.
#
# The writes come in both shapes seen so far: the group and the assignment go
# through `runner._coalesce_update_or_create` (the OTHER upsert primitive,
# newly overridden here), while the VIP saves directly inside
# `_ensure_fhrp_vip` and the canonical-name migration is a direct
# `group.save()`.
from dcim.models import Device
from dcim.models import DeviceRole
from dcim.models import DeviceType
from dcim.models import Interface
from dcim.models import Manufacturer
from dcim.models import Site
from django.test import TestCase
from ipam.models import FHRPGroup
from ipam.models import FHRPGroupAssignment
from ipam.models import IPAddress

from forward_netbox.utilities.drift_comparison import compare_model_rows


class FhrpGroupPreviewTest(TestCase):
    def setUp(self):
        site = Site.objects.create(name="F Site", slug="f-site")
        mfr = Manufacturer.objects.create(name="F Mfr", slug="f-mfr")
        dtype = DeviceType.objects.create(manufacturer=mfr, model="F DT", slug="f-dt")
        role = DeviceRole.objects.create(name="F Role", slug="f-role")
        self.device = Device.objects.create(
            name="fhrp-dev", site=site, device_type=dtype, role=role, status="active"
        )
        self.interface = Interface.objects.create(
            device=self.device, name="Vlan100", type="virtual"
        )

    def _row(self, **extra):
        row = {
            "device": "fhrp-dev",
            "interface": "Vlan100",
            "group_id": 100,
            "protocol": "hsrp",
            "address": "10.0.0.1/24",
            "status": "active",
            "priority": 100,
        }
        row.update(extra)
        return row

    def _existing(self, *, priority=100, status="active", name=None):
        group = FHRPGroup.objects.create(
            protocol="hsrp",
            group_id=100,
            name=name or "hsrp-100-10.0.0.1",
            description="Forward FHRP group",
            comments="",
        )
        from django.contrib.contenttypes.models import ContentType

        IPAddress.objects.create(
            address="10.0.0.1/24",
            status=status,
            role="hsrp",
            assigned_object_type=ContentType.objects.get_for_model(FHRPGroup),
            assigned_object_id=group.pk,
        )
        FHRPGroupAssignment.objects.create(
            interface_type=ContentType.objects.get_for_model(Interface),
            interface_id=self.interface.pk,
            group=group,
            priority=priority,
        )
        return group

    # --- the negative space -------------------------------------------------

    def test_a_preview_creates_no_group_vip_or_assignment(self):
        groups = FHRPGroup.objects.count()
        addresses = IPAddress.objects.count()
        assignments = FHRPGroupAssignment.objects.count()

        result = compare_model_rows(None, "ipam.fhrpgroup", [self._row()])

        self.assertEqual(FHRPGroup.objects.count(), groups)
        self.assertEqual(IPAddress.objects.count(), addresses)
        self.assertEqual(FHRPGroupAssignment.objects.count(), assignments)
        self.assertEqual(result["creates"], 1)

    def test_a_preview_does_not_rewrite_a_drifted_vip(self):
        self._existing(status="deprecated")

        result = compare_model_rows(None, "ipam.fhrpgroup", [self._row()])

        self.assertEqual(
            IPAddress.objects.get(address="10.0.0.1/24").status, "deprecated"
        )
        self.assertEqual(result["updates"], 1)

    def test_a_preview_does_not_migrate_a_group_name(self):
        # The apply rewrites a group's name to canonical form with a direct
        # `group.save(update_fields=["name"])`.
        group = self._existing(name="Legacy Name")

        compare_model_rows(None, "ipam.fhrpgroup", [self._row()])

        group.refresh_from_db()
        self.assertEqual(group.name, "Legacy Name")

    def test_a_preview_does_not_rewrite_an_assignment_priority(self):
        group = self._existing(priority=50)

        result = compare_model_rows(None, "ipam.fhrpgroup", [self._row()])

        assignment = FHRPGroupAssignment.objects.get(group=group)
        self.assertEqual(assignment.priority, 50)
        self.assertEqual(result["updates"], 1)

    # --- classification -----------------------------------------------------

    def test_an_absent_group_is_a_create(self):
        result = compare_model_rows(None, "ipam.fhrpgroup", [self._row()])

        self.assertEqual(
            result, {"creates": 1, "updates": 0, "unchanged": 0, "rejected": 0}
        )

    def test_a_fully_matching_row_is_unchanged(self):
        self._existing()

        result = compare_model_rows(None, "ipam.fhrpgroup", [self._row()])

        self.assertEqual(
            result, {"creates": 0, "updates": 0, "unchanged": 1, "rejected": 0}
        )

    def test_a_group_present_without_its_assignment_is_a_create(self):
        # The group and VIP exist but this interface is not bound to it, so the
        # assignment is genuinely new.
        from django.contrib.contenttypes.models import ContentType

        group = FHRPGroup.objects.create(
            protocol="hsrp",
            group_id=100,
            name="hsrp-100-10.0.0.1",
            description="Forward FHRP group",
            comments="",
        )
        IPAddress.objects.create(
            address="10.0.0.1/24",
            status="active",
            role="hsrp",
            assigned_object_type=ContentType.objects.get_for_model(FHRPGroup),
            assigned_object_id=group.pk,
        )

        result = compare_model_rows(None, "ipam.fhrpgroup", [self._row()])

        self.assertEqual(result["creates"], 1)

    def test_a_group_present_without_its_vip_creates_no_address(self):
        """The VIP-create guard is only reachable through this shape.

        When the group is absent the row short-circuits to a create before
        `_ensure_fhrp_vip` is ever called, so the guard inside it is dead code
        for every other test here. A negative control found that: removing the
        guard broke nothing until this case existed.
        """
        FHRPGroup.objects.create(
            protocol="hsrp",
            group_id=100,
            name="hsrp-100-10.0.0.1",
            description="Forward FHRP group",
            comments="",
        )
        addresses = IPAddress.objects.count()

        result = compare_model_rows(None, "ipam.fhrpgroup", [self._row()])

        self.assertEqual(IPAddress.objects.count(), addresses)
        self.assertEqual(result["creates"], 1)

    def test_an_unknown_device_is_rejected(self):
        result = compare_model_rows(
            None, "ipam.fhrpgroup", [self._row(device="no-such-device")]
        )

        self.assertEqual(result["rejected"], 1)
        self.assertEqual(result["creates"], 0)

    def test_an_unknown_interface_is_rejected(self):
        result = compare_model_rows(
            None, "ipam.fhrpgroup", [self._row(interface="Vlan999")]
        )

        self.assertEqual(result["rejected"], 1)
        self.assertEqual(result["creates"], 0)

    def test_a_vip_owned_by_another_object_is_rejected_not_a_create(self):
        # An address assigned to something that is not an FHRP group is a
        # conflict the apply refuses, so it must not read as drift.
        from django.contrib.contenttypes.models import ContentType

        IPAddress.objects.create(
            address="10.0.0.1/24",
            status="active",
            assigned_object_type=ContentType.objects.get_for_model(Interface),
            assigned_object_id=self.interface.pk,
        )
        FHRPGroup.objects.create(
            protocol="hsrp",
            group_id=100,
            name="hsrp-100-10.0.0.1",
            description="Forward FHRP group",
            comments="",
        )

        result = compare_model_rows(None, "ipam.fhrpgroup", [self._row()])

        self.assertEqual(result["rejected"], 1)
        self.assertEqual(result["creates"], 0)

    def test_a_shared_vip_is_not_drift(self):
        # Two groups legitimately share a VIP; the apply leaves it on the group
        # that owns it and writes nothing for it. With the group and assignment
        # already present the row is unchanged, not a perpetual update.
        other = FHRPGroup.objects.create(
            protocol="hsrp", group_id=200, name="hsrp-200-10.0.0.1"
        )
        from django.contrib.contenttypes.models import ContentType

        IPAddress.objects.create(
            address="10.0.0.1/24",
            status="active",
            role="hsrp",
            assigned_object_type=ContentType.objects.get_for_model(FHRPGroup),
            assigned_object_id=other.pk,
        )
        group = FHRPGroup.objects.create(
            protocol="hsrp",
            group_id=100,
            name="hsrp-100-10.0.0.1",
            description="Forward FHRP group",
            comments="",
        )
        FHRPGroupAssignment.objects.create(
            interface_type=ContentType.objects.get_for_model(Interface),
            interface_id=self.interface.pk,
            group=group,
            priority=100,
        )

        result = compare_model_rows(None, "ipam.fhrpgroup", [self._row()])

        self.assertEqual(result["rejected"], 0)
        self.assertEqual(result["creates"], 0)
        self.assertEqual(result["unchanged"], 1)
