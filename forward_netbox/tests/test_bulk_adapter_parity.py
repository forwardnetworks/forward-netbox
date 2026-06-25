from unittest.mock import Mock
from unittest.mock import patch

from dcim.models import Device
from dcim.models import DeviceRole
from dcim.models import DeviceType
from dcim.models import Interface
from dcim.models import MACAddress
from dcim.models import Manufacturer
from dcim.models import Platform
from dcim.models import Site
from django.contrib.contenttypes.models import ContentType
from django.db import connection
from django.test import TestCase
from ipam.models import IPAddress
from ipam.models import Prefix
from ipam.models import VLAN
from ipam.models import VRF

from forward_netbox.models import ForwardSource
from forward_netbox.models import ForwardSync
from forward_netbox.utilities.apply_engine_bulk import bulk_orm_apply_interface
from forward_netbox.utilities.apply_engine_bulk import bulk_orm_apply_ipaddress
from forward_netbox.utilities.apply_engine_bulk import bulk_orm_apply_macaddress
from forward_netbox.utilities.apply_engine_bulk import bulk_orm_apply_simple_models
from forward_netbox.utilities.apply_engine_bulk import bulk_orm_apply_virtualchassis
from forward_netbox.utilities.sync import ForwardSyncRunner
from forward_netbox.utilities.sync_core_models import apply_dcim_devicerole
from forward_netbox.utilities.sync_core_models import apply_dcim_devicetype
from forward_netbox.utilities.sync_core_models import apply_dcim_manufacturer
from forward_netbox.utilities.sync_core_models import apply_dcim_platform
from forward_netbox.utilities.sync_core_models import apply_dcim_site
from forward_netbox.utilities.sync_interface import apply_dcim_interface
from forward_netbox.utilities.sync_interface import apply_dcim_macaddress
from forward_netbox.utilities.sync_ipam import apply_ipam_ipaddress
from forward_netbox.utilities.sync_ipam import apply_ipam_prefix
from forward_netbox.utilities.sync_ipam import apply_ipam_vlan
from forward_netbox.utilities.sync_ipam import apply_ipam_vrf


class BulkAdapterParityTest(TestCase):
    """Prove the experimental bulk paths produce the same DB state as the adapter.

    Runs the adapter on a row set, snapshots the resulting state, rolls back to a
    savepoint (leaving only the pre-created fixtures), runs the bulk path on the
    same rows, and asserts the snapshots match.
    """

    def setUp(self):
        self.source = ForwardSource.objects.create(
            name="parity-source",
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
            name="parity-sync",
            source=self.source,
            parameters={"snapshot_id": "latestProcessed"},
        )
        mfr = Manufacturer.objects.create(name="MfrP", slug="mfr-p")
        dt = DeviceType.objects.create(manufacturer=mfr, model="dt-p", slug="dt-p")
        role = DeviceRole.objects.create(name="RoleP", slug="role-p")
        site = Site.objects.create(name="SiteP", slug="site-p")
        self.device = Device.objects.create(
            name="dev-p", device_type=dt, role=role, site=site
        )
        self.interface = Interface.objects.create(
            device=self.device, name="Ethernet1", type="1000base-t"
        )

    def _runner(self):
        return ForwardSyncRunner(
            sync=self.sync, ingestion=None, client=None, logger_=Mock()
        )

    def _run_both_and_compare(self, *, seed, adapter_apply, bulk_apply, capture):
        # Savepoint is taken BEFORE seeding so rollback also undoes the seed,
        # leaving a clean slate for the bulk run.
        sid = connection.savepoint()
        seed()
        adapter_apply(self._runner())
        adapter_state = capture()
        connection.savepoint_rollback(sid)

        sid2 = connection.savepoint()
        seed()
        bulk_apply(self._runner())
        bulk_state = capture()
        connection.savepoint_rollback(sid2)

        self.assertEqual(adapter_state, bulk_state)
        return adapter_state

    def test_ipaddress_bulk_matches_adapter(self):
        rows = [
            {
                "device": "dev-p",
                "interface": "Ethernet1",
                "address": "10.1.1.1/24",
                "status": "active",
                "vrf": None,
            },
            {
                "device": "dev-p",
                "interface": "Ethernet1",
                "address": "10.1.1.2/24",
                "status": "active",
                "vrf": "blue",
            },
            {
                "device": "dev-p",
                "interface": "Ethernet1",
                "address": "10.1.1.3/24",
                "status": "active",
                "vrf": None,
            },
        ]

        def seed():
            IPAddress.objects.create(address="10.1.1.3/24", status="deprecated")

        def capture():
            return [
                (
                    str(ip.address),
                    ip.vrf.name if ip.vrf else None,
                    ip.status,
                    ip.assigned_object_id,
                    ip.assigned_object_type_id,
                )
                for ip in IPAddress.objects.order_by("address")
            ]

        def adapter_apply(runner):
            for row in rows:
                apply_ipam_ipaddress(runner, row)

        state = self._run_both_and_compare(
            seed=seed,
            adapter_apply=adapter_apply,
            bulk_apply=lambda runner: bulk_orm_apply_ipaddress(runner, rows),
            capture=capture,
        )
        # Sanity: all three addresses present, updated row flipped to active.
        self.assertEqual(len(state), 3)
        self.assertTrue(all(row[2] == "active" for row in state))

    def test_interface_bulk_matches_adapter(self):
        rows = [
            {
                "device": "dev-p",
                "name": "Ethernet2",
                "type": "1000base-t",
                "enabled": True,
                "mtu": 1500,
            },
            {
                "device": "dev-p",
                "name": "Ethernet1",
                "type": "1000base-t",
                "enabled": False,
                "description": "uplink",
            },
        ]

        def seed():
            pass

        def capture():
            return [
                (
                    iface.name,
                    iface.type,
                    iface.enabled,
                    iface.mtu,
                    iface.description,
                )
                for iface in Interface.objects.filter(device=self.device).order_by(
                    "name"
                )
            ]

        def adapter_apply(runner):
            for row in rows:
                apply_dcim_interface(runner, row)

        state = self._run_both_and_compare(
            seed=seed,
            adapter_apply=adapter_apply,
            bulk_apply=lambda runner: bulk_orm_apply_interface(runner, rows),
            capture=capture,
        )
        names = {row[0] for row in state}
        self.assertIn("Ethernet2", names)

    def test_interface_lag_membership_bulk_matches_adapter(self):
        # LAG-membership rows are delegated by the bulk path to the adapter; this
        # proves the hybrid batch+delegate split yields the same state (parent
        # LAG ensured, member's lag FK set) as running everything via the adapter.
        rows = [
            {
                "device": "dev-p",
                "name": "Ethernet1",
                "type": "1000base-t",
                "enabled": True,
                "lag": "Po1",
            }
        ]

        def seed():
            pass

        def capture():
            return [
                (iface.name, iface.type, iface.lag.name if iface.lag else None)
                for iface in Interface.objects.filter(device=self.device).order_by(
                    "name"
                )
            ]

        def adapter_apply(runner):
            for row in rows:
                apply_dcim_interface(runner, row)

        state = self._run_both_and_compare(
            seed=seed,
            adapter_apply=adapter_apply,
            bulk_apply=lambda runner: bulk_orm_apply_interface(runner, rows),
            capture=capture,
        )
        by_name = {row[0]: row for row in state}
        self.assertEqual(by_name["Po1"][1], "lag")
        self.assertEqual(by_name["Ethernet1"][2], "Po1")

    def test_interface_lag_parent_and_member_same_batch_no_duplicate(self):
        # Regression: a plain LAG parent (Po9, type=lag) and a member that
        # references it (lag=Po9) arrive in the SAME bulk batch. The parent is
        # staged in the bulk create dict (uncommitted) while the member is
        # delegated to the adapter, which would independently create Po9 — and
        # the trailing bulk_create then violated
        # ``dcim_interface_unique_device_name`` ("po9 already exists"). Deferring
        # the member until after the bulk write commits + caches the parent fixes
        # it: exactly one Po9, member bound to it.
        rows = [
            {"device": "dev-p", "name": "Po9", "type": "lag", "enabled": True},
            {
                "device": "dev-p",
                "name": "Ethernet9",
                "type": "1000base-t",
                "enabled": True,
                "lag": "Po9",
            },
        ]
        bulk_orm_apply_interface(self._runner(), rows)
        po9 = Interface.objects.filter(device=self.device, name="Po9")
        self.assertEqual(po9.count(), 1)
        self.assertEqual(po9.first().type, "lag")
        member = Interface.objects.get(device=self.device, name="Ethernet9")
        self.assertEqual(member.lag.name, "Po9")

    def test_interface_duplicate_spellings_in_batch_collapse(self):
        # Forward emitting both an abbreviated and a canonical spelling of the
        # same interface in one batch (po10 + Port-channel10) must create ONE
        # interface — the second row canonically matches the first staged create
        # even though neither is committed yet.
        rows = [
            {"device": "dev-p", "name": "po10", "type": "lag", "enabled": True},
            {
                "device": "dev-p",
                "name": "Port-channel10",
                "type": "lag",
                "enabled": True,
            },
        ]
        bulk_orm_apply_interface(self._runner(), rows)
        created = Interface.objects.filter(
            device=self.device, name__in=["po10", "Port-channel10"]
        )
        self.assertEqual(created.count(), 1)

    def test_macaddress_bulk_matches_adapter(self):
        interface_ct = ContentType.objects.get_for_model(Interface)
        rows = [
            {"device": "dev-p", "interface": "Ethernet1", "mac": "00:11:22:33:44:01"},
            {"device": "dev-p", "interface": "Ethernet1", "mac": "00:11:22:33:44:02"},
        ]

        def seed():
            # Existing MAC assigned to a different interface — the row reassigns
            # it to Ethernet1 (the update path); the other row is a create.
            other = Interface.objects.create(
                device=self.device, name="Eth-seed", type="1000base-t"
            )
            MACAddress.objects.create(
                mac_address="00:11:22:33:44:02",
                assigned_object_type=interface_ct,
                assigned_object_id=other.pk,
            )

        def capture():
            return [
                (
                    str(mac.mac_address),
                    mac.assigned_object_id,
                    mac.assigned_object_type_id,
                )
                for mac in MACAddress.objects.order_by("mac_address")
            ]

        def adapter_apply(runner):
            for row in rows:
                apply_dcim_macaddress(runner, row)

        state = self._run_both_and_compare(
            seed=seed,
            adapter_apply=adapter_apply,
            bulk_apply=lambda runner: bulk_orm_apply_macaddress(runner, rows),
            capture=capture,
        )
        # Both MACs now point at Ethernet1.
        self.assertEqual(len(state), 2)
        self.assertTrue(all(row[1] == self.interface.pk for row in state))

    def test_lag_member_does_not_override_parent_enabled(self):
        # Regression (#41): a LAG parent's `enabled` follows its own Forward row
        # (operStatus-derived, often False for a Port-channel). The member-ensure
        # must NOT force it True, else every sync flips enabled True<->False and
        # churns. Seed the parent disabled, apply a member, assert it stays.
        parent = Interface.objects.create(
            device=self.device, name="Po1", type="lag", enabled=False
        )
        bulk_orm_apply_interface(
            self._runner(),
            [
                {
                    "device": "dev-p",
                    "name": "Ethernet7",
                    "type": "1000base-t",
                    "enabled": True,
                    "lag": "Po1",
                }
            ],
        )
        parent.refresh_from_db()
        self.assertFalse(
            parent.enabled, "member-ensure must not flip LAG parent enabled to True"
        )
        self.assertEqual(
            Interface.objects.get(device=self.device, name="Ethernet7").lag_id,
            parent.pk,
        )

    def test_macaddress_differing_format_matches_existing_no_duplicate(self):
        # Forward emits toString(macAddress) as lowercase / cisco-dot; NetBox
        # stores upper colon-expanded. Re-syncing the same MAC must match the
        # existing row (MACAddress has no uniqueness constraint, so a missed match
        # silently duplicates it every run).
        interface_ct = ContentType.objects.get_for_model(Interface)
        MACAddress.objects.create(
            mac_address="AA:BB:CC:DD:EE:01",
            assigned_object_type=interface_ct,
            assigned_object_id=self.interface.pk,
        )
        before = MACAddress.objects.count()
        for variant in ("aa:bb:cc:dd:ee:01", "aabb.ccdd.ee01", "AA-BB-CC-DD-EE-01"):
            bulk_orm_apply_macaddress(
                self._runner(),
                [{"device": "dev-p", "interface": "Ethernet1", "mac": variant}],
            )
        self.assertEqual(
            MACAddress.objects.count(),
            before,
            "a differently-formatted MAC re-created a duplicate row",
        )

    def _outcomes(self, runner, model_string):
        counts = {}
        for call in runner.logger.increment_statistics.call_args_list:
            args, kwargs = call
            if args and args[0] == model_string:
                counts[kwargs.get("outcome")] = counts.get(kwargs.get("outcome"), 0) + 1
        return counts

    def test_interface_reapply_makes_no_writes(self):
        rows = [
            {
                "device": "dev-p",
                "name": "Ethernet1",
                "type": "1000base-t",
                "enabled": False,
                "description": "uplink",
            }
        ]
        bulk_orm_apply_interface(self._runner(), rows)  # first apply mutates

        runner = self._runner()
        with patch.object(Interface.objects, "bulk_update") as mock_update:
            bulk_orm_apply_interface(runner, rows)
            mock_update.assert_not_called()
        self.assertEqual(self._outcomes(runner, "dcim.interface"), {"unchanged": 1})

    def test_macaddress_reapply_makes_no_writes(self):
        rows = [
            {
                "device": "dev-p",
                "interface": "Ethernet1",
                "mac": "00:11:22:33:44:55",
            }
        ]
        bulk_orm_apply_macaddress(self._runner(), rows)  # first apply creates

        runner = self._runner()
        with patch.object(MACAddress.objects, "bulk_update") as mock_update:
            bulk_orm_apply_macaddress(runner, rows)
            mock_update.assert_not_called()
        self.assertEqual(self._outcomes(runner, "dcim.macaddress"), {"unchanged": 1})

    def test_ipaddress_reapply_makes_no_writes(self):
        rows = [
            {
                "device": "dev-p",
                "interface": "Ethernet1",
                "address": "10.9.9.9/24",
                "status": "active",
                "vrf": None,
            }
        ]
        bulk_orm_apply_ipaddress(self._runner(), rows)  # first apply creates

        runner = self._runner()
        with patch.object(IPAddress.objects, "bulk_update") as mock_update:
            bulk_orm_apply_ipaddress(runner, rows)
            mock_update.assert_not_called()
        self.assertEqual(self._outcomes(runner, "ipam.ipaddress"), {"unchanged": 1})

    def test_virtualchassis_reapply_makes_no_writes(self):
        rows = [
            {
                "vc_name": "vc-1",
                "vc_domain": "d1",
                "device": "dev-p",
                "vc_position": 1,
            }
        ]
        bulk_orm_apply_virtualchassis(self._runner(), rows)  # first apply assigns

        runner = self._runner()
        with patch.object(Device.objects, "bulk_update") as mock_update:
            bulk_orm_apply_virtualchassis(runner, rows)
            mock_update.assert_not_called()
        self.assertEqual(
            self._outcomes(runner, "dcim.virtualchassis"), {"unchanged": 1}
        )

    # --- Simple/tree default-bulk model parity (create + update) ---------------

    def _assert_simple_parity(self, *, model_string, seed, rows, adapter_fn, capture):
        def adapter_apply(runner):
            for row in rows:
                adapter_fn(runner, row)

        return self._run_both_and_compare(
            seed=seed,
            adapter_apply=adapter_apply,
            bulk_apply=lambda runner: bulk_orm_apply_simple_models(
                runner, model_string, rows
            ),
            capture=capture,
        )

    def test_site_bulk_matches_adapter(self):
        rows = [
            {"name": "S-new", "slug": "s-new"},
            {"name": "S-updated", "slug": "s-up"},
        ]
        self._assert_simple_parity(
            model_string="dcim.site",
            seed=lambda: Site.objects.create(name="S-old", slug="s-up"),
            rows=rows,
            adapter_fn=apply_dcim_site,
            capture=lambda: [(s.slug, s.name) for s in Site.objects.order_by("slug")],
        )

    def test_manufacturer_bulk_matches_adapter(self):
        rows = [
            {"name": "Mfr-new", "slug": "mfr-new"},
            {"name": "Mfr-updated", "slug": "mfr-up"},
        ]
        self._assert_simple_parity(
            model_string="dcim.manufacturer",
            seed=lambda: Manufacturer.objects.create(name="Mfr-old", slug="mfr-up"),
            rows=rows,
            adapter_fn=apply_dcim_manufacturer,
            capture=lambda: [
                (m.slug, m.name) for m in Manufacturer.objects.order_by("slug")
            ],
        )

    def test_devicerole_bulk_matches_adapter(self):
        rows = [
            {"name": "Role-new", "slug": "role-new", "color": "222222"},
            {"name": "Role-updated", "slug": "role-up", "color": "333333"},
        ]
        self._assert_simple_parity(
            model_string="dcim.devicerole",
            seed=lambda: DeviceRole.objects.create(
                name="Role-old", slug="role-up", color="111111"
            ),
            rows=rows,
            adapter_fn=apply_dcim_devicerole,
            capture=lambda: [
                (r.slug, r.name, r.color) for r in DeviceRole.objects.order_by("slug")
            ],
        )

    def test_platform_bulk_matches_adapter(self):
        def seed():
            Manufacturer.objects.create(name="Cisco", slug="cisco")
            Platform.objects.create(name="P-old", slug="p-up")

        rows = [
            {
                "name": "P-new",
                "slug": "p-new",
                "manufacturer": "Cisco",
                "manufacturer_slug": "cisco",
            },
            {
                "name": "P-updated",
                "slug": "p-up",
                "manufacturer": "Cisco",
                "manufacturer_slug": "cisco",
            },
        ]
        self._assert_simple_parity(
            model_string="dcim.platform",
            seed=seed,
            rows=rows,
            adapter_fn=apply_dcim_platform,
            capture=lambda: [
                (p.slug, p.name, p.manufacturer.slug if p.manufacturer else None)
                for p in Platform.objects.order_by("slug")
            ],
        )

    def test_devicetype_bulk_matches_adapter(self):
        def seed():
            Manufacturer.objects.create(name="Cisco", slug="cisco")

        rows = [
            {
                "model": "DT-new",
                "device_type": "DT-new",
                "slug": "dt-new",
                "device_type_slug": "dt-new",
                "manufacturer": "Cisco",
                "manufacturer_slug": "cisco",
            },
        ]
        self._assert_simple_parity(
            model_string="dcim.devicetype",
            seed=seed,
            rows=rows,
            adapter_fn=apply_dcim_devicetype,
            capture=lambda: [
                (dt.slug, dt.model, dt.manufacturer.slug if dt.manufacturer else None)
                for dt in DeviceType.objects.order_by("slug")
            ],
        )

    def test_vlan_bulk_matches_adapter(self):
        def seed():
            Site.objects.create(name="VlanSite", slug="vlan-site")

        rows = [
            {
                "name": "V10",
                "vid": 10,
                "status": "active",
                "site": "VlanSite",
                "site_slug": "vlan-site",
            }
        ]
        self._assert_simple_parity(
            model_string="ipam.vlan",
            seed=seed,
            rows=rows,
            adapter_fn=apply_ipam_vlan,
            capture=lambda: [
                (v.vid, v.name, v.status, v.site.slug if v.site else None)
                for v in VLAN.objects.order_by("vid")
            ],
        )

    def test_vrf_bulk_matches_adapter(self):
        rows = [
            {
                "name": "VRF-A",
                "rd": "65000:1",
                "description": "prod",
                "enforce_unique": False,
            },
            {
                "name": "VRF-B",
                "rd": "",
                "description": "",
                "enforce_unique": True,
            },
        ]
        self._assert_simple_parity(
            model_string="ipam.vrf",
            seed=lambda: None,
            rows=rows,
            adapter_fn=apply_ipam_vrf,
            capture=lambda: [
                (v.name, v.rd, v.description, v.enforce_unique)
                for v in VRF.objects.order_by("name")
            ],
        )

    def test_prefix_bulk_matches_adapter(self):
        # Parent + child + a global (null-VRF) prefix. Capturing `_depth` proves
        # the bulk per-object tree path triggers NetBox's hierarchy signal exactly
        # like the adapter; the global prefix proves null-VRF identity parity.
        rows = [
            {"prefix": "10.0.0.0/16", "vrf": None, "status": "active"},
            {"prefix": "10.0.1.0/24", "vrf": None, "status": "active"},
            {"prefix": "192.168.0.0/24", "vrf": None, "status": "reserved"},
        ]
        self._assert_simple_parity(
            model_string="ipam.prefix",
            seed=lambda: None,
            rows=rows,
            adapter_fn=apply_ipam_prefix,
            capture=lambda: [
                (str(p.prefix), p.vrf_id, p.status, p._depth)
                for p in Prefix.objects.order_by("prefix")
            ],
        )
