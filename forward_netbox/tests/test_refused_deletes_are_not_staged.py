"""A delete the database will refuse must never be staged.

A deployment's sync reported sixteen protected-delete skips: ten `dcim.device`
held by `netbox_routing.bgppeer`, and six `netbox_dlm.softwareversion` held by
`netbox_dlm.inventoryitemsoftware`. Nothing was lost - PROTECT refused every one
- but staging them was wrong three times over:

  - the durable state then tombstones them as deleted, so nothing retries, the
    rows stay in NetBox, and the report goes quiet while the two systems
    disagree;
  - the skips are noise on every run that does re-derive them;
  - and they were the only visible edge of a delete path that would have removed
    a device with no protecting child silently.

The guards that should have caught them were hand-written lists.
`netbox_dlm.softwareversion` protected against image files, validated rules,
device software and vulnerabilities, and simply omitted `InventoryItemSoftware`.
`dcim.device` checked plugin ownership only and asked nothing about references.

The fix asks Django's deletion collector instead, and the `dcim.device` case is
why. `protecting_relations(Device)` does NOT name `BGPPeer`: a `BGPRouter`
attaches to a device through a GENERIC key with no database constraint, and the
protection appears only further down the cascade a delete would perform. A scan
of the model's own reverse relations calls that device deletable, and is wrong.
`Collector.collect` is what `.delete()` runs, so it sees cascades, generic
relations and hidden relations by construction.

These tests pin the negative space: the protected row is held back, the
unprotected row is still staged, and an unreadable verdict fails closed.
"""

from dcim.models import Device
from dcim.models import DeviceRole
from dcim.models import DeviceType
from dcim.models import Manufacturer
from dcim.models import Site
from django.apps import apps
from django.test import TestCase

from forward_netbox.utilities.workload_state import _reference_protected_pks


class TheCollectorSeesProtectionAScanCannotTest(TestCase):
    """The reason this uses Collector and not the relation list."""

    def setUp(self):
        site = Site.objects.create(name="Ref Site", slug="ref-site")
        mfr = Manufacturer.objects.create(name="Ref Mfr", slug="ref-mfr")
        dtype = DeviceType.objects.create(
            manufacturer=mfr, model="Ref DT", slug="ref-dt"
        )
        role = DeviceRole.objects.create(name="Ref Role", slug="ref-role")
        self.free = Device.objects.create(
            name="free-device", site=site, device_type=dtype, role=role
        )
        self.held = Device.objects.create(
            name="held-device", site=site, device_type=dtype, role=role
        )

    def test_a_device_with_nothing_pointing_at_it_is_not_held(self):
        self.assertEqual(_reference_protected_pks(Device, [self.free.pk]), set())

    def test_a_device_held_by_a_protected_child_is_held(self):
        # VirtualDeviceContext.device is PROTECT and is a direct relation, so
        # this case is visible to both approaches. It pins the ordinary path.
        VirtualDeviceContext = apps.get_model("dcim", "VirtualDeviceContext")
        VirtualDeviceContext.objects.create(
            name="vdc-1", device=self.held, identifier=1, status="active"
        )

        held = _reference_protected_pks(Device, [self.free.pk, self.held.pk])

        self.assertEqual(held, {self.held.pk})

    def test_the_relation_scan_alone_would_miss_the_routing_case(self):
        """Documents WHY the collector is used, without depending on a fixture.

        `netbox_routing.BGPPeer` is what the deployment's message named, and it
        is absent from Device's protecting relations because the link is
        generic. If this ever starts appearing, the naive scan would have been
        sufficient and this rationale should be revisited.
        """
        from forward_netbox.utilities.bulk_merge import protecting_relations

        labels = {
            relation.related_model._meta.label
            for relation in protecting_relations(Device)
        }
        self.assertNotIn(
            "netbox_routing.BGPPeer",
            labels,
            "a one-level relation scan now sees BGPPeer; the collector is still "
            "correct, but the justification in the docstring needs updating",
        )


class SoftwareVersionInventoryItemHoldTest(TestCase):
    """The six skips the hand-written list omitted."""

    def _dlm(self, name):
        return apps.get_model("netbox_dlm", name)

    def setUp(self):
        from dcim.models import Platform

        self.platform = Platform.objects.create(name="Ref Plat", slug="ref-plat")
        SoftwareVersion = self._dlm("SoftwareVersion")
        self.free = SoftwareVersion.objects.create(
            platform=self.platform, version="1.0-free"
        )
        self.held = SoftwareVersion.objects.create(
            platform=self.platform, version="1.0-held"
        )

    def test_a_version_held_only_by_inventory_item_software_is_held(self):
        from dcim.models import InventoryItem

        site = Site.objects.create(name="SV Site", slug="sv-site")
        mfr = Manufacturer.objects.create(name="SV Mfr", slug="sv-mfr")
        dtype = DeviceType.objects.create(manufacturer=mfr, model="SV DT", slug="sv-dt")
        role = DeviceRole.objects.create(name="SV Role", slug="sv-role")
        device = Device.objects.create(
            name="sv-device", site=site, device_type=dtype, role=role
        )
        item = InventoryItem.objects.create(device=device, name="sv-item")
        self._dlm("InventoryItemSoftware").objects.create(
            inventory_item=item, software_version=self.held
        )

        held = _reference_protected_pks(
            self._dlm("SoftwareVersion"), [self.free.pk, self.held.pk]
        )

        self.assertEqual(
            held,
            {self.held.pk},
            "InventoryItemSoftware.software_version is PROTECT; a version it "
            "holds must never be staged for deletion",
        )


class TheGuardFailsClosedTest(TestCase):
    """On a destructive path, 'cannot tell' must mean 'do not delete'."""

    def test_an_unreadable_verdict_holds_the_row_back(self):
        site = Site.objects.create(name="FC Site", slug="fc-site")
        mfr = Manufacturer.objects.create(name="FC Mfr", slug="fc-mfr")
        dtype = DeviceType.objects.create(manufacturer=mfr, model="FC DT", slug="fc-dt")
        role = DeviceRole.objects.create(name="FC Role", slug="fc-role")
        device = Device.objects.create(
            name="fc-device", site=site, device_type=dtype, role=role
        )

        from django.db.models import deletion

        original = deletion.Collector.collect

        def exploding(self, objs, *args, **kwargs):
            raise RuntimeError("collector unavailable")

        deletion.Collector.collect = exploding
        try:
            held = _reference_protected_pks(Device, [device.pk])
        finally:
            deletion.Collector.collect = original

        self.assertEqual(
            held,
            {device.pk},
            "an unreadable collector verdict must hold the row back, not stage it",
        )

    def test_an_empty_candidate_set_is_cheap_and_empty(self):
        self.assertEqual(_reference_protected_pks(Device, []), set())
        self.assertEqual(_reference_protected_pks(Device, [None]), set())
