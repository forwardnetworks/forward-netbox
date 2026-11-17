# When a device moves to another site, the plugin clears its interfaces' untagged
# VLANs that belong to the old site (NetBox refuses a cross-site untagged VLAN).
# The interface rows name a VLAN only by its id, so they do not change when the
# site does, and an incremental run used to skip them: the new site's VLAN was
# never written back, and every moved device's access ports were left with no
# untagged VLAN. Grouping a customer's data-centre locations into one site would
# have moved some 1,500 devices that way.
from unittest.mock import Mock

from dcim.models import Device
from dcim.models import DeviceRole
from dcim.models import DeviceType
from dcim.models import Interface
from dcim.models import Manufacturer
from dcim.models import Site
from django.contrib.auth import get_user_model
from django.test import TestCase
from ipam.models import VLAN

from forward_netbox.models import ForwardDeviceIdentity
from forward_netbox.models import ForwardIngestion
from forward_netbox.models import ForwardSource
from forward_netbox.models import ForwardSync
from forward_netbox.tests.test_workload_state import _workload
from forward_netbox.utilities.apply_engine_bulk import bulk_orm_apply_device
from forward_netbox.utilities.apply_engine_bulk import bulk_orm_apply_interface
from forward_netbox.utilities.sync import ForwardSyncRunner
from forward_netbox.utilities.sync_interface import apply_dcim_interface
from forward_netbox.utilities.workload_state import apply_durable_workload_deltas
from forward_netbox.utilities.workload_state import promote_workload_states_locked
from forward_netbox.utilities.workload_state import stage_workload_states

INTERFACES = [
    {"device": "router-1", "name": "Ethernet1", "untagged_vlan": 10},
    {"device": "router-1", "name": "Ethernet2", "untagged_vlan": 20},
    {"device": "router-2", "name": "Ethernet1", "untagged_vlan": 10},
]
SCOPE = {"forward_netbox_shard_keys": ["router-1", "router-2"]}


class SiteMoveRestagesInterfacesTest(TestCase):
    def setUp(self):
        user = get_user_model().objects.create_user(username="site-move-owner")
        source = ForwardSource.objects.create(
            name="site-move-source",
            type="saas",
            url="https://fwd.app",
            parameters={"network_id": "network"},
        )
        self.sync = ForwardSync.objects.create(
            name="site-move-sync",
            source=source,
            user=user,
            parameters={"dcim.interface": True},
        )
        maker = Manufacturer.objects.create(name="sm-mfr", slug="sm-mfr")
        kind = DeviceType.objects.create(manufacturer=maker, model="sm", slug="sm")
        role = DeviceRole.objects.create(name="sm-role", slug="sm-role")
        old_site = Site.objects.create(name="old-site", slug="old-site")
        for name in ("router-1", "router-2"):
            Device.objects.create(name=name, device_type=kind, role=role, site=old_site)
        self.snapshots = 0

    def _run(self, device_sites):
        device_rows = [
            {"name": name, "site": slug, "site_slug": slug}
            for name, slug in device_sites.items()
        ]
        workloads = [
            _workload(device_rows, model_string="dcim.device", parameters=SCOPE),
            _workload(INTERFACES, parameters=SCOPE),
        ]
        return apply_durable_workload_deltas(self.sync, workloads)

    def _promote(self, pending):
        self.snapshots += 1
        ingestion = ForwardIngestion.objects.create(
            sync=self.sync, snapshot_id=f"snapshot-{self.snapshots}"
        )
        stage_workload_states(ingestion, pending)
        promote_workload_states_locked(ingestion)

    def _interface_rows(self, workloads):
        # An interface workload with nothing to stage is dropped from the run.
        workload = next(
            (w for w in workloads if w.model_string == "dcim.interface"), None
        )
        return [] if workload is None else workload.upsert_rows

    def _summary(self, summaries):
        return next(
            (s for s in summaries if s.get("model") == "dcim.interface"),
            {"site_move_restaged_rows": 0},
        )

    def _baseline(self):
        _, pending, _ = self._run({"router-1": "old-site", "router-2": "old-site"})
        self._promote(pending)

    def test_a_moving_devices_unchanged_interfaces_are_applied_again(self):
        self._baseline()

        workloads, _, summaries = self._run(
            {"router-1": "new-site", "router-2": "old-site"}
        )

        rows = self._interface_rows(workloads)
        self.assertEqual(
            sorted((r["device"], r["name"]) for r in rows),
            [("router-1", "Ethernet1"), ("router-1", "Ethernet2")],
        )
        self.assertEqual(self._summary(summaries)["site_move_restaged_rows"], 2)

    def test_nothing_is_reapplied_when_no_device_moves(self):
        self._baseline()

        workloads, _, summaries = self._run(
            {"router-1": "old-site", "router-2": "old-site"}
        )

        self.assertEqual(self._interface_rows(workloads), [])
        self.assertEqual(self._summary(summaries)["site_move_restaged_rows"], 0)

    def test_a_site_matched_by_name_is_not_a_move(self):
        # The device apply finds a site by slug, then by name, and a site matched
        # by name keeps its own slug. Comparing slugs alone would call every
        # device there "moving" on every run.
        Site.objects.filter(slug="old-site").update(slug="their-slug")
        self._baseline()

        workloads, _, _ = self._run({"router-1": "old-site", "router-2": "old-site"})

        self.assertEqual(self._interface_rows(workloads), [])

    def test_a_device_not_yet_in_netbox_is_not_a_move(self):
        self._baseline()
        Device.objects.filter(name="router-2").delete()

        workloads, _, _ = self._run({"router-1": "old-site", "router-2": "new-site"})

        self.assertEqual(self._interface_rows(workloads), [])


class MovedDeviceGetsTheNewSitesVlanTest(TestCase):
    """The whole chain, on real models: move, clear, re-apply, new VLAN."""

    def setUp(self):
        source = ForwardSource.objects.create(
            name="chain-src",
            type="saas",
            url="https://fwd.app",
            status="ready",
            parameters={"network_id": "net-1"},
        )
        self.sync = ForwardSync.objects.create(
            name="chain-sync",
            source=source,
            parameters={"snapshot_id": "latestProcessed", "dcim.interface": True},
        )
        maker = Manufacturer.objects.create(name="Cisco", slug="cisco")
        kind = DeviceType.objects.create(
            manufacturer=maker, model="model-a", slug="model-a"
        )
        role = DeviceRole.objects.create(name="role-a", slug="role-a")
        self.hall = Site.objects.create(name="hall-a", slug="hall-a")
        self.campus = Site.objects.create(name="campus", slug="campus")
        self.hall_vlan = VLAN.objects.create(site=self.hall, vid=10, name="users")
        self.campus_vlan = VLAN.objects.create(site=self.campus, vid=10, name="users")
        self.device = Device.objects.create(
            name="sw-1", device_type=kind, role=role, site=self.hall
        )
        Interface.objects.create(
            device=self.device,
            name="eth1",
            type="1000base-t",
            mode="access",
            untagged_vlan=self.hall_vlan,
        )
        ForwardDeviceIdentity.objects.create(
            sync=self.sync, source_device_key="sw-1", device=self.device
        )
        self.runner = ForwardSyncRunner(
            sync=self.sync, ingestion=None, client=None, logger_=Mock()
        )
        self.runner._record_issue = Mock()

    def _device_row(self, site):
        return {
            "name": "sw-1",
            "site": site,
            "site_slug": site,
            "role": "role-a",
            "role_slug": "role-a",
            "role_color": "9e9e9e",
            "manufacturer": "Cisco",
            "manufacturer_slug": "cisco",
            "device_type": "model-a",
            "device_type_slug": "model-a",
            "platform": "",
            "platform_slug": "",
            "status": "active",
        }

    def _interface_row(self):
        return {
            "device": "sw-1",
            "name": "eth1",
            "type": "1000base-t",
            "enabled": True,
            "mode": "access",
            "untagged_vlan": 10,
        }

    def test_the_bulk_interface_path_also_restores_the_new_sites_vlan(self):
        bulk_orm_apply_device(self.runner, [self._device_row("campus")])

        bulk_orm_apply_interface(self.runner, [self._interface_row()])

        eth1 = Interface.objects.get(device=self.device, name="eth1")
        self.assertEqual(eth1.untagged_vlan, self.campus_vlan)

    def test_the_reapplied_interface_points_at_the_new_sites_vlan(self):
        bulk_orm_apply_device(self.runner, [self._device_row("campus")])
        self.device.refresh_from_db()
        eth1 = Interface.objects.get(device=self.device, name="eth1")

        # The move happened and the old site's VLAN had to come off.
        self.assertEqual(self.device.site, self.campus)
        self.assertIsNone(eth1.untagged_vlan)

        # The unchanged row, re-applied because its device moved, restores it.
        apply_dcim_interface(
            self.runner,
            {
                "device": "sw-1",
                "name": "eth1",
                "type": "1000base-t",
                "enabled": True,
                "mode": "access",
                "untagged_vlan": 10,
            },
        )

        eth1.refresh_from_db()
        self.assertEqual(eth1.untagged_vlan, self.campus_vlan)
