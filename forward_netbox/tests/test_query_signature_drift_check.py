"""The Health warning for a stale published query signature.

`pip install -U` never rewrites a query already published into a customer's
Forward org. When a release changes a bundled query's `@query` parameter
list, every execution against the stale published copy fails with HTTP 400 -
v2.9.7's outage. `parameter_signature_drift` already detects this on the
on-demand "Export Live Query Drift Check"; this pins that its last STORED
result (`ForwardNQEMap.last_live_drift`) surfaces as a standing Health
warning, with no live Forward call of its own.
"""

from types import SimpleNamespace
from unittest.mock import Mock

from django.test import SimpleTestCase

from forward_netbox.utilities.health import _query_signature_drift_check


class QuerySignatureDriftCheckTest(SimpleTestCase):
    def _sync(self, maps):
        return Mock(get_maps=Mock(return_value=maps))

    def _map(self, model_string, last_live_drift):
        return SimpleNamespace(
            model_string=model_string, last_live_drift=last_live_drift
        )

    def test_no_map_ever_checked_returns_none(self):
        sync = self._sync([self._map("dcim.interface", {})])
        self.assertIsNone(_query_signature_drift_check(sync))

    def test_every_checked_map_matching_passes(self):
        sync = self._sync(
            [
                self._map("dcim.interface", {"parameter_signature_matches": True}),
                self._map("ipam.ipaddress", {"status": "source_modified"}),
            ]
        )
        check = _query_signature_drift_check(sync)
        self.assertEqual(check["status"], "pass")

    def test_a_mismatched_map_is_a_danger(self):
        sync = self._sync(
            [
                self._map(
                    "dcim.interface",
                    {"status": "live_query_id_parameter_mismatch"},
                )
            ]
        )
        check = _query_signature_drift_check(sync)
        self.assertEqual(check["status"], "danger")
        self.assertIn("dcim.interface", check["message"])
        self.assertIn("Publish Bundled Queries", check["message"])

    def test_names_multiple_mismatched_maps(self):
        sync = self._sync(
            [
                self._map(
                    "dcim.interface",
                    {"status": "live_query_id_parameter_mismatch"},
                ),
                self._map(
                    "ipam.ipaddress",
                    {"status": "live_query_id_parameter_mismatch"},
                ),
            ]
        )
        check = _query_signature_drift_check(sync)
        self.assertEqual(check["status"], "danger")
        self.assertIn("dcim.interface", check["message"])
        self.assertIn("ipam.ipaddress", check["message"])

    def test_a_matching_map_alongside_a_mismatched_one_still_warns(self):
        sync = self._sync(
            [
                self._map("dcim.interface", {"parameter_signature_matches": True}),
                self._map(
                    "ipam.ipaddress",
                    {"status": "live_query_id_parameter_mismatch"},
                ),
            ]
        )
        check = _query_signature_drift_check(sync)
        self.assertEqual(check["status"], "danger")
        self.assertIn("ipam.ipaddress", check["message"])
        self.assertNotIn("dcim.interface", check["message"])
