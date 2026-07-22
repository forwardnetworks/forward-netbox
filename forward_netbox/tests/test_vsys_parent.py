from contextlib import nullcontext
from unittest.mock import Mock
from unittest.mock import patch

from dcim.models import Device
from dcim.models import DeviceRole
from dcim.models import DeviceType
from dcim.models import Manufacturer
from dcim.models import Site
from django.test import TestCase

from forward_netbox.choices import ForwardSyncStatusChoices
from forward_netbox.models import ForwardIngestion
from forward_netbox.models import ForwardSource
from forward_netbox.models import ForwardSync
from forward_netbox.models import ForwardVirtualParentClaim
from forward_netbox.utilities.ownership import OwnershipConflictError
from forward_netbox.utilities.vsys_parent import link_vsys_parents
from forward_netbox.utilities.vsys_parent import PARENT_DEVICE_CF


class VsysParentTest(TestCase):
    def setUp(self):
        self.source = ForwardSource.objects.create(
            name="vsys-source",
            type="saas",
            url="https://fwd.app",
            parameters={"network_id": "network-1"},
        )
        self.sync = ForwardSync.objects.create(
            name="vsys-sync",
            source=self.source,
            status=ForwardSyncStatusChoices.COMPLETED,
            parameters={"snapshot_id": "latestProcessed"},
        )
        self.ingestion = ForwardIngestion.objects.create(
            sync=self.sync,
            snapshot_id="snapshot-1",
            baseline_ready=True,
        )
        mfr = Manufacturer.objects.create(name="Palo Alto", slug="palo-alto")
        self.dt = DeviceType.objects.create(
            manufacturer=mfr, model="PA-5250", slug="pa-5250"
        )
        self.role = DeviceRole.objects.create(name="Firewall", slug="firewall")
        self.site = Site.objects.create(name="DC1", slug="dc1")

    def _device(self, name):
        return Device.objects.create(
            name=name, device_type=self.dt, role=self.role, site=self.site
        )

    def _rows(self, pairs):
        return lambda sync, client: [{"name": n, "parent": p} for n, p in pairs]

    def test_links_virtual_device_to_parent(self):
        chassis = self._device("fw-chassis-a")
        vsys = self._device("fw-chassis-a_vsys3_APP")
        result = link_vsys_parents(
            self.sync, fetch_rows=self._rows([(vsys.name, chassis.name)])
        )
        self.assertEqual(result["linked"], 1)
        self.assertEqual(result["orphan_parent"], 0)
        vsys.refresh_from_db()
        chassis.refresh_from_db()
        self.assertEqual(vsys.custom_field_data.get(PARENT_DEVICE_CF), chassis.pk)
        # The physical chassis itself is never linked.
        self.assertIn(chassis.custom_field_data.get(PARENT_DEVICE_CF), (None, ""))
        # The context is also modeled as a VirtualDeviceContext under the chassis.
        from dcim.models import VirtualDeviceContext

        self.assertEqual(result["vdc_created"], 1)
        self.assertTrue(
            VirtualDeviceContext.objects.filter(
                device=chassis, name=vsys.name, status="active"
            ).exists()
        )
        # Re-running is idempotent — the VDC is found, not re-created.
        again = link_vsys_parents(
            self.sync, fetch_rows=self._rows([(vsys.name, chassis.name)])
        )
        self.assertEqual(again["vdc_created"], 0)
        self.assertEqual(again["vdc_existing"], 1)
        self.assertEqual(
            VirtualDeviceContext.objects.filter(device=chassis, name=vsys.name).count(),
            1,
        )

    def test_orphan_parent_not_linked(self):
        # vsys present in NetBox, but its physical chassis is not collected.
        vsys = self._device("fw-chassis-c_vsys1")
        result = link_vsys_parents(
            self.sync, fetch_rows=self._rows([(vsys.name, "fw-chassis-c")])
        )
        self.assertEqual(result["linked"], 0)
        self.assertEqual(result["orphan_parent"], 1)
        vsys.refresh_from_db()
        self.assertIn(vsys.custom_field_data.get(PARENT_DEVICE_CF), (None, ""))

    def test_idempotent(self):
        chassis = self._device("fw-a")
        vsys = self._device("fw-a_vsys1")
        rows = self._rows([(vsys.name, chassis.name)])
        first = link_vsys_parents(self.sync, fetch_rows=rows)
        second = link_vsys_parents(self.sync, fetch_rows=rows)
        self.assertEqual(first["linked"], 1)
        self.assertEqual(second["linked"], 0)
        self.assertEqual(second["already"], 1)

    def test_self_heal_clears_stale_link(self):
        chassis = self._device("fw-b")
        vsys = self._device("fw-b_vsys1")
        link_vsys_parents(self.sync, fetch_rows=self._rows([(vsys.name, chassis.name)]))
        vsys.refresh_from_db()
        self.assertEqual(vsys.custom_field_data.get(PARENT_DEVICE_CF), chassis.pk)
        # Next run: it is no longer reported as a virtual context → link cleared.
        result = link_vsys_parents(self.sync, fetch_rows=self._rows([]))
        self.assertEqual(result["cleared"], 1)
        vsys.refresh_from_db()
        self.assertIsNone(vsys.custom_field_data.get(PARENT_DEVICE_CF))

    def test_live_fetch_uses_pinned_snapshot(self):
        chassis = self._device("fw-pinned")
        vsys = self._device("fw-pinned_vsys1")
        client = Mock()
        client.run_nqe_query.return_value = [
            {"name": vsys.name, "parent": chassis.name}
        ]

        with patch(
            "forward_netbox.utilities.post_sync.current_post_sync_snapshot",
            return_value=nullcontext(
                {"generation": self.ingestion.pk, "snapshot_id": "snapshot-1"}
            ),
        ):
            result = link_vsys_parents(
                self.sync,
                client=client,
                snapshot_id="snapshot-1",
            )

        self.assertEqual(result["linked"], 1)
        self.assertEqual(
            client.run_nqe_query.call_args.kwargs["snapshot_id"],
            "snapshot-1",
        )

    def test_multi_source_claims_use_last_claim_removal(self):
        source_b = ForwardSource.objects.create(
            name="vsys-source-b",
            type="saas",
            url="https://fwd.app",
            parameters={"network_id": "net-b"},
        )
        sync_b = ForwardSync.objects.create(
            name="vsys-sync-b",
            source=source_b,
            status=ForwardSyncStatusChoices.COMPLETED,
            parameters={"snapshot_id": "latestProcessed"},
        )
        ForwardIngestion.objects.create(
            sync=sync_b,
            snapshot_id="snapshot-1",
            baseline_ready=True,
        )
        parent_a = self._device("fw-owner-a")
        parent_b = self._device("fw-owner-b")
        conflicting = self._device("fw-shared-vsys")
        first = link_vsys_parents(
            self.sync,
            fetch_rows=self._rows([(conflicting.name, parent_a.name)]),
        )
        self.assertEqual(first["claims_created"], 1)
        with self.assertRaises(OwnershipConflictError):
            link_vsys_parents(
                sync_b,
                fetch_rows=self._rows([(conflicting.name, parent_b.name)]),
            )

        conflicting.refresh_from_db()
        self.assertEqual(
            conflicting.custom_field_data[PARENT_DEVICE_CF],
            parent_a.pk,
        )
        self.assertEqual(
            ForwardVirtualParentClaim.objects.filter(device=conflicting).count(),
            2,
        )

        released = link_vsys_parents(self.sync, fetch_rows=self._rows([]))
        conflicting.refresh_from_db()
        self.assertEqual(released["claims_released"], 1)
        self.assertEqual(
            conflicting.custom_field_data[PARENT_DEVICE_CF],
            parent_b.pk,
        )

        last = link_vsys_parents(sync_b, fetch_rows=self._rows([]))
        conflicting.refresh_from_db()
        self.assertEqual(last["claims_released"], 1)
        self.assertIsNone(conflicting.custom_field_data[PARENT_DEVICE_CF])
        self.assertEqual(last["vdc_deleted"], 1)
