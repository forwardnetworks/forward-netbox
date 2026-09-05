# Two OSPF neighbours on one interface must not drift forever.
#
# `forward_ospf_interfaces.nqe` selects `foreach neighbor in area.neighbors`,
# so a broadcast segment with three neighbours produces THREE Forward rows that
# all carry the same `local_interface`. `ensure_ospf_interface` upserts them
# with `coalesce_sets=[("interface",)]` - one OSPFInterface per NetBox
# interface - so all three collapse onto one object, and each writes a
# different `comments` (remote device, remote interface, remote router ID all
# differ per neighbour).
#
# Last writer wins. Every subsequent comparison then finds the other rows still
# wanting to write their own version, so they report as drift on every run
# forever, and no sync can resolve them. On a customer estate that is 180 of
# 2854 rows permanently drifted and `In sync: No` that can never become Yes.
from dcim.models import Device
from dcim.models import DeviceRole
from dcim.models import DeviceType
from dcim.models import Interface
from dcim.models import Manufacturer
from dcim.models import Site
from django.test import TestCase

from forward_netbox.utilities.drift_comparison import compare_model_rows

OSPF_INTERFACE = "netbox_routing.ospfinterface"


def _ospf_models():
    from forward_netbox.utilities.sync_primitives import optional_model

    return (
        optional_model("netbox_routing", "OSPFInstance", "netbox_routing.ospfinstance"),
        optional_model("netbox_routing", "OSPFArea", "netbox_routing.ospfarea"),
        optional_model("netbox_routing", "OSPFInterface", OSPF_INTERFACE),
    )


class BroadcastSegmentRowsConvergeTest(TestCase):
    def setUp(self):
        site = Site.objects.create(name="N Site", slug="n-site")
        mfr = Manufacturer.objects.create(name="N Mfr", slug="n-mfr")
        dtype = DeviceType.objects.create(manufacturer=mfr, model="N DT", slug="n-dt")
        role = DeviceRole.objects.create(name="N Role", slug="n-role")
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

    def _neighbours(self):
        """Three neighbours seen on ONE local interface, as Forward reports it."""
        return [
            self._row(remote_device="peer-a", remote_router_id="10.0.0.2"),
            self._row(remote_device="peer-b", remote_router_id="10.0.0.3"),
            self._row(remote_device="peer-c", remote_router_id="10.0.0.4"),
        ]

    def _apply_once(self, rows, *, legacy=False):
        """Persist what a sync would write for these rows.

        `legacy=True` reproduces what the pre-fix apply left behind: the LAST
        row's comments, because each row overwrote the one before it.
        """
        from forward_netbox.utilities.row_collapsing import collapse_rows
        from forward_netbox.utilities.sync_routing_impl import ospf_interface_comments

        collapsed = collapse_rows(OSPF_INTERFACE, rows)

        OSPFInstance, OSPFArea, OSPFInterface = _ospf_models()
        instance = OSPFInstance.objects.create(
            name=f"{self.device.name} OSPF 1",
            router_id="10.0.0.1",
            process_id=1,
            device=self.device,
            vrf=None,
        )
        area = OSPFArea.objects.create(
            area_id="0.0.0.0",
            area_type="standard",
            description="Observed by Forward from structured OSPF state.",
        )
        # One object for the whole group, carrying whatever the apply settled
        # on - which today is the LAST row's comments.
        return OSPFInterface.objects.create(
            instance=instance,
            area=area,
            interface=self.interface,
            priority=None,
            comments=ospf_interface_comments(rows[-1] if legacy else collapsed[0]),
        )

    def test_a_synced_broadcast_segment_reports_no_drift(self):
        rows = self._neighbours()
        self._apply_once(rows)

        result = compare_model_rows(None, OSPF_INTERFACE, rows)

        # Every row describes a state the estate is already in. None of them is
        # a difference between NetBox and Forward, so none of them is drift.
        self.assertEqual(result["updates"], 0)
        self.assertEqual(result["creates"], 0)

    def test_the_surplus_neighbours_are_not_counted_as_separate_work(self):
        rows = self._neighbours()
        self._apply_once(rows)

        result = compare_model_rows(None, OSPF_INTERFACE, rows)

        # Three rows, one object. Estimated apply work must describe objects
        # the sync would write, not rows it would read.
        self.assertEqual(result["updates"] + result["creates"], 0)

    def test_an_estate_synced_by_the_old_code_converges_in_one_run(self):
        # The upgrade cost, stated rather than discovered: an interface whose
        # comments were written last-writer-wins rewrites ONCE to the merged
        # form and is then stable. One update per affected interface, not the
        # N-1 that recurred on every run before.
        rows = self._neighbours()
        self._apply_once(rows, legacy=True)

        result = compare_model_rows(None, OSPF_INTERFACE, rows)

        self.assertEqual(result["updates"], 1)
        self.assertEqual(result["creates"], 0)

    def test_a_real_change_on_the_segment_is_still_reported(self):
        # The guard against fixing this by simply not looking: if the segment
        # genuinely changes, exactly one object's worth of drift must appear.
        rows = self._neighbours()
        self._apply_once(rows)
        changed = [self._row(remote_device="peer-a", remote_router_id="10.0.0.9")]

        result = compare_model_rows(None, OSPF_INTERFACE, changed + rows[1:])

        self.assertEqual(result["creates"], 0)
        self.assertEqual(result["updates"], 1)
