# What the adapter-model comparisons actually COST.
#
# A deployment's 2.9.0 drift report measured 10 of 32 models and spent
# 637,721 ms doing it - 10.6 minutes - over 557,285 rows, with `dcim.macaddress`
# alone taking 270,640 ms for 121,900 rows (~2.2 ms/row). Those are the BULK
# paths, which classify a whole batch at a time.
#
# The adapter models have no bulk path, so their comparison is a per-row loop.
# The same deployment carries 59,098 `dcim.inventoryitem` rows and 17,281
# `dcim.cable` rows in the not-compared list. If per-row costs materially more
# than bulk per row, wiring these models in makes an already-long preview
# unusable - and that is a design question about the loop, not a tuning
# question, so it wants measuring before more models depend on the shape.
#
# Gated behind FORWARD_ADAPTER_SCALE because seeding is slow:
#
#   cd /opt/netbox/netbox && FORWARD_ADAPTER_SCALE=1 \
#     FORWARD_ADAPTER_SCALE_ROWS=2000 python manage.py test \
#     forward_netbox.tests.test_adapter_drift_scale --keepdb
#
# Reports ms/row and queries/row, both of which extrapolate linearly, rather
# than a wall-clock total on a sample size nobody runs in production.
import os
import time
import unittest

from dcim.models import Device
from dcim.models import DeviceRole
from dcim.models import DeviceType
from dcim.models import Interface
from dcim.models import InventoryItem
from dcim.models import InventoryItemRole
from dcim.models import Manufacturer
from dcim.models import Site
from django.db import connection
from django.test import TestCase
from django.test.utils import CaptureQueriesContext

from forward_netbox.utilities.drift_comparison import compare_model_rows

SCALE_ENABLED = os.environ.get("FORWARD_ADAPTER_SCALE") == "1"
ROWS = int(os.environ.get("FORWARD_ADAPTER_SCALE_ROWS", "2000"))

# The measured bulk baseline from the deployment's own report, so the numbers
# here are compared against production rather than against a guess.
BULK_MS_PER_ROW = 270640 / 121900


@unittest.skipUnless(SCALE_ENABLED, "set FORWARD_ADAPTER_SCALE=1")
class AdapterComparisonCostTest(TestCase):
    """Measure, do not assert a threshold.

    A pass/fail bar here would encode a guess about acceptable cost. The point
    is the ratio against the bulk paths on the same box, printed so a human
    can decide whether the per-row shape is viable at 59,098 rows.
    """

    @classmethod
    def setUpTestData(cls):
        cls.site = Site.objects.create(name="Scale Site", slug="scale-site")
        cls.mfr = Manufacturer.objects.create(name="Scale Mfr", slug="scale-mfr")
        cls.dtype = DeviceType.objects.create(
            manufacturer=cls.mfr, model="Scale DT", slug="scale-dt"
        )
        cls.role = DeviceRole.objects.create(name="Scale Role", slug="scale-role")
        cls.item_role = InventoryItemRole.objects.create(
            name="Scale Item Role", slug="scale-item-role", color="9e9e9e"
        )
        # One device per 10 rows, which is roughly the deployment's shape:
        # 59,098 inventory items across ~3,700 devices is ~16 per device.
        device_count = max(1, ROWS // 16)
        cls.devices = Device.objects.bulk_create(
            [
                Device(
                    name=f"scale-dev-{index}",
                    site=cls.site,
                    device_type=cls.dtype,
                    role=cls.role,
                    status="active",
                )
                for index in range(device_count)
            ]
        )
        cls.devices = list(Device.objects.filter(name__startswith="scale-dev-"))

    def _report(self, label, rows, elapsed_s, queries):
        ms_per_row = (elapsed_s * 1000) / max(1, rows)
        print(
            f"\n[adapter-scale] {label}: {rows} rows in {elapsed_s * 1000:.0f} ms "
            f"= {ms_per_row:.3f} ms/row, {queries} queries "
            f"({queries / max(1, rows):.2f}/row). "
            f"Bulk baseline {BULK_MS_PER_ROW:.3f} ms/row -> "
            f"{ms_per_row / BULK_MS_PER_ROW:.1f}x"
        )
        return ms_per_row

    def test_inventoryitem_comparison_cost(self):
        """The largest not-compared model on the reporting deployment."""
        rows = []
        for index in range(ROWS):
            device = self.devices[index % len(self.devices)]
            rows.append(
                {
                    "device": device.name,
                    "name": f"Slot {index}",
                    "part_id": f"PN-{index}",
                    "serial": f"SN-{index}",
                    "status": "active",
                    "discovered": True,
                    "role": "Scale Item Role",
                    "role_slug": "scale-item-role",
                    "role_color": "9e9e9e",
                    "manufacturer": "Scale Mfr",
                    "manufacturer_slug": "scale-mfr",
                }
            )
        # Half already present, so the run exercises both create and unchanged
        # classification rather than only the cheap absent path.
        # `InventoryItem` is MPTT-managed, so `bulk_create` leaves `lft` null
        # and the insert is rejected. Seeded one at a time; this is fixture
        # cost, not measured cost - the timer starts after.
        for index in range(0, ROWS, 2):
            InventoryItem.objects.create(
                device=self.devices[index % len(self.devices)],
                name=f"Slot {index}",
                part_id=f"PN-{index}",
                serial=f"SN-{index}",
                status="active",
                discovered=True,
                role=self.item_role,
                manufacturer=self.mfr,
            )

        started = time.perf_counter()
        with CaptureQueriesContext(connection) as captured:
            result = compare_model_rows(None, "dcim.inventoryitem", rows)
        elapsed = time.perf_counter() - started

        self.assertIsNotNone(result)
        self._report(
            "dcim.inventoryitem", len(rows), elapsed, len(captured.captured_queries)
        )

    def test_taggeditem_comparison_cost(self):
        rows = [
            {
                "device": self.devices[index % len(self.devices)].name,
                "tag": "Scale Tag",
                "tag_slug": "scale-tag",
                "tag_color": "9e9e9e",
            }
            for index in range(ROWS)
        ]

        started = time.perf_counter()
        with CaptureQueriesContext(connection) as captured:
            result = compare_model_rows(None, "extras.taggeditem", rows)
        elapsed = time.perf_counter() - started

        self.assertIsNotNone(result)
        self._report(
            "extras.taggeditem", len(rows), elapsed, len(captured.captured_queries)
        )

    def test_macaddress_comparison_cost(self):
        """The single most expensive model on the reporting deployment.

        Not an adapter model - `dcim.macaddress` goes through the BULK path and
        is already compared in production, where it spent 270,640 ms on 121,900
        rows: 42% of that deployment's entire comparison. Measured here because
        the adapter loops turned out to be cheap and this is where the time
        actually goes.
        """
        from dcim.models import MACAddress

        interfaces = [
            Interface(device=device, name="Ethernet1", type="1000base-t")
            for device in self.devices
        ]
        Interface.objects.bulk_create(interfaces)

        rows = []
        for index in range(ROWS):
            device = self.devices[index % len(self.devices)]
            rows.append(
                {
                    "device": device.name,
                    "interface": "Ethernet1",
                    "mac": "00:11:22:%02x:%02x:%02x"
                    % (index // 65536 % 256, index // 256 % 256, index % 256),
                }
            )
        # Half present, so both classification branches are exercised.
        MACAddress.objects.bulk_create(
            [
                MACAddress(mac_address=rows[index]["mac"])
                for index in range(0, len(rows), 2)
            ]
        )

        if os.environ.get("FORWARD_ADAPTER_PROFILE") == "1":
            import cProfile
            import io as _io
            import pstats

            profiler = cProfile.Profile()
            profiler.enable()
            compare_model_rows(None, "dcim.macaddress", rows)
            profiler.disable()
            buffer = _io.StringIO()
            pstats.Stats(profiler, stream=buffer).sort_stats("tottime").print_stats(18)
            print(buffer.getvalue())

        started = time.perf_counter()
        with CaptureQueriesContext(connection) as captured:
            result = compare_model_rows(None, "dcim.macaddress", rows)
        elapsed = time.perf_counter() - started

        self.assertIsNotNone(result)
        self._report(
            "dcim.macaddress (BULK)",
            len(rows),
            elapsed,
            len(captured.captured_queries),
        )
        # The cost is query COUNT, not query complexity, so the useful
        # diagnostic is which SQL shape repeats - not which single one is slow.
        import re
        from collections import Counter

        shapes = Counter()
        spent = {}
        for query in captured.captured_queries:
            shape = re.sub(r"\d+", "N", query["sql"])[:150]
            shapes[shape] += 1
            spent[shape] = spent.get(shape, 0.0) + float(query["time"])
        for shape, count in shapes.most_common(5):
            print(
                f"[adapter-scale]   x{count} ({spent[shape] * 1000:.0f} ms total): "
                f"{shape}"
            )

    def test_cable_comparison_cost(self):
        """Cables resolve two devices and two interfaces per row."""
        interfaces = []
        for device in self.devices:
            interfaces.append(
                Interface(device=device, name="Ethernet1", type="1000base-t")
            )
        Interface.objects.bulk_create(interfaces)

        rows = []
        for index in range(ROWS):
            left = self.devices[index % len(self.devices)]
            right = self.devices[(index + 1) % len(self.devices)]
            if left.pk == right.pk:
                continue
            rows.append(
                {
                    "device": left.name,
                    "interface": "Ethernet1",
                    "remote_device": right.name,
                    "remote_interface": "Ethernet1",
                    "status": "connected",
                }
            )

        started = time.perf_counter()
        with CaptureQueriesContext(connection) as captured:
            result = compare_model_rows(None, "dcim.cable", rows)
        elapsed = time.perf_counter() - started

        self.assertIsNotNone(result)
        self._report("dcim.cable", len(rows), elapsed, len(captured.captured_queries))
