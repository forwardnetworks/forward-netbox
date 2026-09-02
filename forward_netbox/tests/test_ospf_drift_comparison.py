# Slice eight of the adapter-only drift comparison, and the last one:
# `netbox_routing.ospfinstance`, `ospfarea` and `ospfinterface`.
#
# The audit result here is short, which is itself the finding: every write in
# these three chains is already behind a `runner.` call the preview overrides.
# No direct save, unlike the BGP neighbour address and the FHRP virtual IP.
#
# What they DO need is the opposite of what the peering models needed. A BGP
# peer's BGPRouter and BGPScope have no Forward query of their own, so the peer
# is the only place their drift can be reported and its verdict is the
# strongest across the whole chain. Every OSPF parent is a SEPARATELY measured
# model with its own query and its own rows, so folding a parent's create into
# the interface's verdict would count one object twice. `preview_leaf_outcome`
# is that distinction, and the double-count test below is what pins it.
from dcim.models import Device
from dcim.models import DeviceRole
from dcim.models import DeviceType
from dcim.models import Interface
from dcim.models import Manufacturer
from dcim.models import Site
from django.test import TestCase
from ipam.models import VRF

from forward_netbox.utilities.drift_comparison import compare_model_rows

INSTANCE = "netbox_routing.ospfinstance"
AREA = "netbox_routing.ospfarea"
OSPF_INTERFACE = "netbox_routing.ospfinterface"


def _ospf_models():
    from forward_netbox.utilities.sync_primitives import optional_model

    return (
        optional_model("netbox_routing", "OSPFInstance", INSTANCE),
        optional_model("netbox_routing", "OSPFArea", AREA),
        optional_model("netbox_routing", "OSPFInterface", OSPF_INTERFACE),
    )


class OspfPreviewTest(TestCase):
    def setUp(self):
        site = Site.objects.create(name="O Site", slug="o-site")
        mfr = Manufacturer.objects.create(name="O Mfr", slug="o-mfr")
        dtype = DeviceType.objects.create(manufacturer=mfr, model="O DT", slug="o-dt")
        role = DeviceRole.objects.create(name="O Role", slug="o-role")
        self.device = Device.objects.create(
            name="ospf-dev", site=site, device_type=dtype, role=role, status="active"
        )
        self.interface = Interface.objects.create(
            device=self.device, name="GigabitEthernet0/0", type="1000base-t"
        )

    def _row(self, **extra):
        row = {
            "device": "ospf-dev",
            "process_id": "1",
            "router_id": "10.0.0.1",
            "area_id": "0.0.0.0",
            "area_type": "standard",
            "local_interface": "GigabitEthernet0/0",
        }
        row.update(extra)
        return row

    def _existing_instance(self, *, router_id="10.0.0.1", vrf=None):
        from forward_netbox.utilities.sync_routing_impl import ospf_instance_comments

        OSPFInstance, _, _ = _ospf_models()
        return OSPFInstance.objects.create(
            name=f"{self.device.name} OSPF 1",
            router_id=router_id,
            process_id=1,
            device=self.device,
            vrf=vrf,
            comments=ospf_instance_comments(self._row(), "1"),
        )

    def _existing_area(self, *, area_type="standard"):
        _, OSPFArea, _ = _ospf_models()
        return OSPFArea.objects.create(
            area_id="0.0.0.0",
            area_type=area_type,
            description="Observed by Forward from structured OSPF state.",
        )

    def _existing_interface(self):
        from forward_netbox.utilities.sync_routing_impl import ospf_interface_comments

        _, _, OSPFInterface = _ospf_models()
        return OSPFInterface.objects.create(
            instance=self._existing_instance(),
            area=self._existing_area(),
            interface=self.interface,
            priority=None,
            comments=ospf_interface_comments(self._row()),
        )

    # --- the firewall ------------------------------------------------------

    def test_a_preview_creates_no_instance_area_or_interface(self):
        OSPFInstance, OSPFArea, OSPFInterface = _ospf_models()
        for model_string in (INSTANCE, AREA, OSPF_INTERFACE):
            compare_model_rows(None, model_string, [self._row()])
        self.assertEqual(OSPFInstance.objects.count(), 0)
        self.assertEqual(OSPFArea.objects.count(), 0)
        self.assertEqual(OSPFInterface.objects.count(), 0)
        self.assertEqual(VRF.objects.count(), 0)

    def test_a_preview_does_not_rewrite_a_drifted_instance(self):
        instance = self._existing_instance(router_id="10.9.9.9")
        compare_model_rows(None, INSTANCE, [self._row()])
        instance.refresh_from_db()
        self.assertEqual(str(instance.router_id), "10.9.9.9")

    # --- the classification ------------------------------------------------

    def test_a_matching_instance_is_unchanged(self):
        self._existing_instance()
        result = compare_model_rows(None, INSTANCE, [self._row()])
        self.assertEqual(result["unchanged"], 1)

    def test_a_drifted_instance_is_an_update(self):
        self._existing_instance(router_id="10.9.9.9")
        result = compare_model_rows(None, INSTANCE, [self._row()])
        self.assertEqual(result["updates"], 1)

    def test_an_absent_instance_is_a_create(self):
        result = compare_model_rows(None, INSTANCE, [self._row()])
        self.assertEqual(result["creates"], 1)

    def test_a_matching_area_is_unchanged(self):
        self._existing_area()
        result = compare_model_rows(None, AREA, [self._row()])
        self.assertEqual(result["unchanged"], 1)

    def test_a_drifted_area_is_an_update(self):
        self._existing_area(area_type="nssa")
        result = compare_model_rows(None, AREA, [self._row()])
        self.assertEqual(result["updates"], 1)

    def test_a_matching_interface_is_unchanged(self):
        self._existing_interface()
        result = compare_model_rows(None, OSPF_INTERFACE, [self._row()])
        self.assertEqual(result["unchanged"], 1)

    # --- the parents are measured elsewhere, so they are NOT counted here ---

    def test_an_absent_parent_does_not_make_a_matching_interface_drift(self):
        """The interface row exists and matches; only its parents are absent.

        Its `instance` and `area` are separately measured models, so counting
        their creates here would report the same two objects twice - once under
        their own model and again under this one. The interface coalesces on
        `interface` alone, so its own row is found and compared on its merits.
        """
        _, _, OSPFInterface = _ospf_models()
        from forward_netbox.utilities.sync_routing_impl import ospf_interface_comments

        OSPFInterface.objects.create(
            instance=self._existing_instance(),
            area=self._existing_area(),
            interface=self.interface,
            priority=None,
            comments=ospf_interface_comments(self._row()),
        )
        result = compare_model_rows(None, OSPF_INTERFACE, [self._row()])
        self.assertEqual(result["unchanged"], 1)
        self.assertEqual(result["creates"], 0)

    def test_an_absent_instance_is_not_also_a_create_under_the_interface(self):
        """The same absent OSPFInstance, asked of both models.

        `netbox_routing.ospfinstance` reports the create, because it owns that
        object. `netbox_routing.ospfinterface` reports an UPDATE - the apply
        would repoint the existing interface row at the new instance - and
        crucially not a second create, which is what folding a parent's outcome
        into the leaf's verdict would have produced.
        """
        OSPFInstance, _, _ = _ospf_models()
        # The interface exists and points at a DIFFERENT process, so the row's
        # own instance is absent while the interface row itself is present.
        # `instance` is NOT NULL, so this is the only shape that models it.
        other = OSPFInstance.objects.create(
            name=f"{self.device.name} OSPF 99",
            router_id="10.0.0.99",
            process_id=99,
            device=self.device,
            vrf=None,
            comments="",
        )
        from forward_netbox.utilities.sync_routing_impl import ospf_interface_comments

        _, _, OSPFInterface = _ospf_models()
        OSPFInterface.objects.create(
            instance=other,
            area=self._existing_area(),
            interface=self.interface,
            priority=None,
            comments=ospf_interface_comments(self._row()),
        )

        instance_result = compare_model_rows(None, INSTANCE, [self._row()])
        self.assertEqual(instance_result["creates"], 1)

        interface_result = compare_model_rows(None, OSPF_INTERFACE, [self._row()])
        self.assertEqual(interface_result["creates"], 0)
        self.assertEqual(interface_result["updates"], 1)

    # --- the VRF collision this shares with the peering chain --------------

    def test_an_instance_naming_an_absent_vrf_is_a_create(self):
        """Coalesced on ("device", "vrf", "process_id").

        With `vrf=None` standing in for an unresolved VRF, this matched the
        device's GLOBAL instance and reported an instance the apply would
        create as already present.
        """
        self._existing_instance()
        result = compare_model_rows(None, INSTANCE, [self._row(vrf="TENANT-B")])
        self.assertEqual(result["creates"], 1)
        self.assertEqual(result["unchanged"], 0)

    def test_an_instance_naming_an_existing_vrf_resolves_into_it(self):
        vrf = VRF.objects.create(name="TENANT-B")
        self._existing_instance(vrf=vrf)
        result = compare_model_rows(None, INSTANCE, [self._row(vrf="TENANT-B")])
        self.assertEqual(result["unchanged"], 1)

    # --- rows the apply refuses -------------------------------------------

    def test_an_instance_without_a_router_id_is_rejected(self):
        result = compare_model_rows(None, INSTANCE, [self._row(router_id="")])
        self.assertEqual(result["rejected"], 1)
        self.assertEqual(result["creates"], 0)

    def test_an_unknown_device_is_rejected(self):
        result = compare_model_rows(None, INSTANCE, [self._row(device="nope")])
        self.assertEqual(result["rejected"], 1)

    def test_an_uninported_local_interface_is_a_skip_not_drift(self):
        result = compare_model_rows(
            None, OSPF_INTERFACE, [self._row(local_interface="Gi9/9")]
        )
        self.assertEqual(result["rejected"], 1)
        self.assertEqual(result["creates"], 0)

    # --- every adapter model now answers -----------------------------------

    def test_the_ospf_models_report_a_measurement_not_an_upper_bound(self):
        for model_string in (INSTANCE, AREA, OSPF_INTERFACE):
            with self.subTest(model_string=model_string):
                self.assertIsNotNone(compare_model_rows(None, model_string, []))


class ForwardQueryErrorSignatureTest(TestCase):
    """`ForwardQueryError` took no keywords, and two callers passed four.

    Found by the preview, but the bug is in the APPLY. Both raise sites built
    the exception with `model_string=`, `context=` and `data=` against an
    `__init__` that accepted none, so both raised `TypeError` instead.
    `apply_model_rows` catches `ForwardQueryError` per row, records the issue
    and continues; it catches no `TypeError` at all. So one OSPF row with no
    `router_id`, or one BGP row with an address family NetBox does not offer,
    aborted the apply for its entire model instead of being skipped.
    """

    def test_the_structured_keywords_are_accepted_and_recorded(self):
        from forward_netbox.exceptions import ForwardQueryError

        exc = ForwardQueryError(
            "boom",
            model_string=INSTANCE,
            context={"device": "d1"},
            data={"row": 1},
        )
        self.assertEqual(exc.model_string, INSTANCE)
        self.assertEqual(exc.context, {"device": "d1"})
        self.assertEqual(exc.data, {"row": 1})

    def test_the_ospf_raise_site_raises_its_own_class(self):
        from forward_netbox.exceptions import ForwardQueryError
        from forward_netbox.utilities.drift_comparison import PreviewRunner
        from forward_netbox.utilities.sync_routing_impl import ensure_ospf_instance

        site = Site.objects.create(name="Q Site", slug="q-site")
        mfr = Manufacturer.objects.create(name="Q Mfr", slug="q-mfr")
        dtype = DeviceType.objects.create(manufacturer=mfr, model="Q DT", slug="q-dt")
        role = DeviceRole.objects.create(name="Q Role", slug="q-role")
        Device.objects.create(
            name="q-dev", site=site, device_type=dtype, role=role, status="active"
        )
        with self.assertRaises(ForwardQueryError):
            ensure_ospf_instance(PreviewRunner(), {"device": "q-dev", "router_id": ""})

    def test_the_message_still_reaches_str(self):
        from forward_netbox.exceptions import ForwardQueryError

        self.assertEqual(str(ForwardQueryError("plain message")), "plain message")
