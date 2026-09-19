# "Gone from Forward" was one badge for a device that genuinely left and a
# device Forward's own UI still shows under an include tag. The Device Tags
# page reads the network CONFIGURATION; every NQE query, and so the sync, reads
# the SNAPSHOT. A customer compared the two - 461 uncovered devices, 321 of
# them "tagged in Forward" - and filed the difference as the sync dropping
# tags. These tests pin the census naming the configuration fact instead:
# disabled, configured but not collected, or a tag entry that outlived the
# device.
from unittest.mock import Mock
from unittest.mock import patch

from dcim.models import Device
from dcim.models import DeviceRole
from dcim.models import DeviceType
from dcim.models import Manufacturer
from dcim.models import Site
from django.test import SimpleTestCase
from django.test import TestCase

from forward_netbox.choices import ForwardSyncStatusChoices
from forward_netbox.models import ForwardDeviceIdentity
from forward_netbox.models import ForwardIngestion
from forward_netbox.models import ForwardSource
from forward_netbox.models import ForwardSync
from forward_netbox.template_content import _uncovered_prune_offer
from forward_netbox.utilities.forward_api import ForwardClient
from forward_netbox.utilities.scope_reconciliation import _absence_census
from forward_netbox.utilities.scope_reconciliation import _absence_summary
from forward_netbox.utilities.scope_reconciliation import ABSENT_DETAILS
from forward_netbox.utilities.scope_reconciliation import compute_scope_reconciliation
from forward_netbox.utilities.scope_reconciliation import ENDPOINT_ABSENCE_DETAILS


def _client(*, device_rows=(), configured_tags=None, classic=None):
    client = Mock()
    client.run_nqe_query = Mock(side_effect=[list(device_rows), []])
    client.get_configured_device_tags = Mock(return_value=configured_tags or {})
    client.get_classic_device_collection = Mock(return_value=classic or {})
    return client


class ConfiguredAbsenceDetailTest(SimpleTestCase):
    def test_each_configuration_fact_gets_its_own_detail(self):
        client = _client(
            configured_tags={"Prod_Core": {"disabled-1", "uncollected-1", "vsys-1"}},
            classic={"disabled-1": False, "uncollected-1": True},
        )
        kinds, details = _absence_census(
            {"disabled-1", "uncollected-1", "vsys-1", "left-1"},
            client=client,
            network_id="n",
            snapshot_id="s",
            include_tags=["Prod_Core", "Prod_Edge"],
        )
        # The verdict does not widen: every one of these is still `absent`,
        # and the prunes gate on exactly that.
        self.assertEqual(set(kinds.values()), {"absent"})
        self.assertEqual(
            details,
            {
                "disabled-1": "absent_configured_disabled",
                "uncollected-1": "absent_configured_uncollected",
                "vsys-1": "absent_tag_only",
            },
        )
        self.assertNotIn("left-1", details)

    def test_only_the_include_tags_count_as_still_tagged(self):
        client = _client(configured_tags={"Maintain": {"gone-1"}})
        _kinds, details = _absence_census(
            {"gone-1"},
            client=client,
            network_id="n",
            snapshot_id="s",
            include_tags=["Prod_Core"],
        )
        self.assertEqual(details, {})
        # Nothing was still tagged, so the classic config was never read.
        client.get_classic_device_collection.assert_not_called()

    def test_no_configuration_read_without_absent_names_or_include_tags(self):
        client = _client(device_rows=[{"name": "sw-1", "vendor": "Vendor.CISCO"}])
        _absence_census(
            {"sw-1"}, client=client, network_id="n", snapshot_id="s", include_tags=["T"]
        )
        client.get_configured_device_tags.assert_not_called()

        client = _client()
        _absence_census({"gone-1"}, client=client, network_id="n", snapshot_id="s")
        client.get_configured_device_tags.assert_not_called()

    def test_a_failed_configuration_read_leaves_the_census_intact(self):
        # Advisory: an older Forward or a read-only login must not turn the
        # census "unavailable" - that would hide the absent/untagged split the
        # prunes need, to gain a refinement nobody had before.
        client = _client()
        client.get_configured_device_tags = Mock(side_effect=RuntimeError("403"))
        census = _absence_census(
            {"gone-1"},
            client=client,
            network_id="n",
            snapshot_id="s",
            include_tags=["Prod_Core"],
        )
        self.assertEqual(census, ({"gone-1": "absent"}, {}))

        client = _client(configured_tags={"Prod_Core": {"gone-1"}})
        client.get_classic_device_collection = Mock(side_effect=RuntimeError("403"))
        kinds, details = _absence_census(
            {"gone-1"},
            client=client,
            network_id="n",
            snapshot_id="s",
            include_tags=["Prod_Core"],
        )
        self.assertEqual(kinds, {"gone-1": "absent"})
        # Still tagged is still known; which configuration fact is not.
        self.assertEqual(details, {"gone-1": "absent_tag_only"})

    def test_the_summary_breaks_absent_down_and_counts_the_still_tagged(self):
        kinds = {"a": "absent", "b": "absent", "c": "absent", "d": "untagged"}
        details = {
            "a": "absent_configured_disabled",
            "b": "absent_configured_disabled",
            "c": "absent_tag_only",
            "d": "endpoint_cimc",
        }
        summary = _absence_summary(set(kinds), kinds, details)
        self.assertEqual(summary["absent_from_snapshot"], 3)
        self.assertEqual(summary["absent_still_tagged"], 3)
        self.assertEqual(
            [(row["reason"], row["count"]) for row in summary["absent_detail"]],
            [("absent_configured_disabled", 2), ("absent_tag_only", 1)],
        )
        # The two breakdowns do not bleed into each other.
        self.assertEqual(
            [row["reason"] for row in summary["endpoint_detail"]], ["endpoint_cimc"]
        )
        self.assertEqual(_absence_summary(set(), {}, {})["absent_still_tagged"], 0)

    def test_every_detail_has_an_operator_label_and_a_distinct_vocabulary(self):
        for reason, label in ABSENT_DETAILS.items():
            self.assertTrue(reason.startswith("absent_"), reason)
            self.assertTrue(label)
        self.assertFalse(set(ABSENT_DETAILS) & set(ENDPOINT_ABSENCE_DETAILS))


class ForwardConfigurationReadsTest(SimpleTestCase):
    """The two REST reads parse the shapes Forward actually returns."""

    def _client(self, payload):
        client = ForwardClient.__new__(ForwardClient)
        client._api_usage = {}
        import threading

        client._api_usage_lock = threading.Lock()
        response = Mock()
        response.json = Mock(return_value=payload)
        client._request = Mock(return_value=response)
        return client

    def test_device_tags_map_tag_to_device_names(self):
        client = self._client(
            {
                "tags": [
                    {"name": "Prod_Core", "devices": ["sw-1", " sw-2 ", ""]},
                    {"name": "Other", "devices": [{"name": "sw-3"}]},
                    {"name": "", "devices": ["ignored"]},
                    "not-a-dict",
                ]
            }
        )
        self.assertEqual(
            client.get_configured_device_tags("155"),
            {"Prod_Core": {"sw-1", "sw-2"}, "Other": {"sw-3"}},
        )
        client._request.assert_called_once_with(
            "GET", "/networks/155/device-tags", params={"with": "devices"}
        )
        self.assertEqual(client._api_usage, {"configured_device_tag_calls": 1})

    def test_classic_devices_map_name_to_collect_flag(self):
        client = self._client(
            {
                "devices": [
                    {"name": "on-default"},
                    {"name": "on-explicit", "collect": True},
                    {"name": "off", "collect": False},
                    {"name": ""},
                ]
            }
        )
        self.assertEqual(
            client.get_classic_device_collection("155"),
            {"on-default": True, "on-explicit": True, "off": False},
        )
        client._request.assert_called_once_with("GET", "/networks/155/classic-devices")

    def test_a_missing_network_is_refused_before_any_request(self):
        from forward_netbox.exceptions import ForwardClientError

        client = self._client({})
        with self.assertRaises(ForwardClientError):
            client.get_configured_device_tags("")
        with self.assertRaises(ForwardClientError):
            client.get_classic_device_collection(" ")
        client._request.assert_not_called()


class ReportAndDevicePageTest(TestCase):
    """The detail reaches the stored report by pk and the device page by name."""

    def setUp(self):
        self.source = ForwardSource.objects.create(
            name="cfg-src",
            type="saas",
            url="https://fwd.app",
            status="ready",
            parameters={
                "username": "u@example.com",
                "password": "p",
                "verify": True,
                "network_id": "net-1",
                "device_tag_include_tags": ["Prod_Core", "Prod_Edge"],
                "device_tag_include_match": "any",
            },
        )
        self.sync = ForwardSync.objects.create(
            name="cfg-sync",
            source=self.source,
            status=ForwardSyncStatusChoices.COMPLETED,
            parameters={"snapshot_id": "latestProcessed"},
        )
        self.ingestion = ForwardIngestion.objects.create(
            sync=self.sync, snapshot_id="snap-1", baseline_ready=True
        )
        mfr = Manufacturer.objects.create(name="MfrC", slug="mfr-c")
        dt = DeviceType.objects.create(manufacturer=mfr, model="dt-c", slug="dt-c")
        role = DeviceRole.objects.create(name="RoleC", slug="role-c")
        site = Site.objects.create(name="SiteC", slug="site-c")
        self.devices = {}
        for name in ("in-scope", "disabled-1", "left-1"):
            self.devices[name] = Device.objects.create(
                name=name, device_type=dt, role=role, site=site
            )
        for name in ("disabled-1", "left-1"):
            ForwardDeviceIdentity.objects.create(
                sync=self.sync,
                source_device_key=name,
                device=self.devices[name],
                ingestion_id=self.ingestion.pk,
                snapshot_id="snap-1",
            )

    def _report(self):
        client = Mock()
        client.run_nqe_query = Mock(
            side_effect=[
                [{"name": "in-scope", "completed": True, "tagNames": ["Prod_Core"]}],
                [{"name": "in-scope", "vendor": "Vendor.CISCO"}],
                [],
            ]
        )
        client.get_configured_device_tags = Mock(
            return_value={"Prod_Edge": {"disabled-1", "in-scope"}}
        )
        client.get_classic_device_collection = Mock(
            return_value={"disabled-1": False, "in-scope": True}
        )
        with (
            patch.object(ForwardSync, "resolve_snapshot_id", return_value="snap-1"),
            patch.object(ForwardSource, "get_client", return_value=client),
        ):
            return compute_scope_reconciliation(self.sync), client

    def test_the_report_carries_the_detail_by_pk_and_the_breakdown(self):
        report, client = self._report()
        client.get_configured_device_tags.assert_called_once_with("net-1")
        owned = report["unmanaged"]["owned_absence"]
        self.assertEqual(owned["absent_from_snapshot"], 2)
        self.assertEqual(owned["absent_still_tagged"], 1)
        self.assertEqual(
            [(row["reason"], row["sample"]) for row in owned["absent_detail"]],
            [("absent_configured_disabled", ["disabled-1"])],
        )
        self.assertEqual(
            report["unmanaged"]["owned_detail_by_id"],
            {str(self.devices["disabled-1"].pk): "absent_configured_disabled"},
        )
        # Both stay prune candidates: still `absent`, same gate as before.
        self.assertEqual(
            set(report["unmanaged"]["owned_absent_device_ids"]),
            {self.devices["disabled-1"].pk, self.devices["left-1"].pk},
        )

    def test_the_device_page_names_the_configuration_fact(self):
        report, _client = self._report()
        from forward_netbox.utilities.scope_reconciliation import (
            public_scope_report,
        )

        payload = public_scope_report(report)
        with patch(
            "forward_netbox.utilities.scope_reconciliation.latest_scope_report",
            return_value=(None, payload, None, ""),
        ):
            identities = list(
                ForwardDeviceIdentity.objects.filter(sync=self.sync).select_related(
                    "sync"
                )
            )
            disabled = _uncovered_prune_offer(
                self.devices["disabled-1"], identities, []
            )
            left = _uncovered_prune_offer(self.devices["left-1"], identities, [])
        self.assertTrue(disabled["offered"])
        self.assertEqual(disabled["absent_detail"], "absent_configured_disabled")
        self.assertEqual(
            disabled["absent_detail_label"],
            ABSENT_DETAILS["absent_configured_disabled"],
        )
        self.assertTrue(left["offered"])
        self.assertEqual(left["absent_detail"], "")
