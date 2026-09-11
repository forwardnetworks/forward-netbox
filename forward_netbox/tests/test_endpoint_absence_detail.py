# "In Forward but untagged" is one badge for two opposite situations on an
# estate that imports SNMP endpoints: a console server whose endpoint tags
# were never added to the include set (fix the scope), and a generic endpoint
# or CIMC controller the scope excludes by design (nothing to fix). A field
# report of 189 console servers and SNMP endpoints among 550 uncovered devices
# is what this split is for.
from unittest.mock import Mock

from django.test import SimpleTestCase

from forward_netbox.utilities.scope_reconciliation import _absence_census
from forward_netbox.utilities.scope_reconciliation import _absence_summary
from forward_netbox.utilities.scope_reconciliation import _endpoint_absence_detail
from forward_netbox.utilities.scope_reconciliation import ENDPOINT_ABSENCE_DETAILS


def _endpoint(**overrides):
    row = {"has_snmp": True, "cimc": False, "console": True, "tags": ["Console"]}
    row.update(overrides)
    return row


def _scope(**overrides):
    scope = {
        "enabled": True,
        "generic": False,
        "include_tags": ["Mgmt_Vl211"],
        "exclude_tags": [],
        "include_match": "any",
    }
    scope.update(overrides)
    return scope


class EndpointAbsenceDetailTest(SimpleTestCase):
    def test_endpoint_sync_off_is_named_first(self):
        self.assertEqual(
            _endpoint_absence_detail(_endpoint(), _scope(enabled=False)),
            "endpoint_scope_off",
        )

    def test_no_snmp_output(self):
        self.assertEqual(
            _endpoint_absence_detail(_endpoint(has_snmp=False), _scope()),
            "endpoint_no_snmp",
        )

    def test_cimc_is_excluded_by_design(self):
        self.assertEqual(
            _endpoint_absence_detail(_endpoint(cimc=True), _scope()), "endpoint_cimc"
        )

    def test_a_generic_endpoint_without_generic_import(self):
        self.assertEqual(
            _endpoint_absence_detail(_endpoint(console=False), _scope()),
            "endpoint_generic",
        )

    def test_a_generic_endpoint_with_generic_import_falls_through_to_tags(self):
        self.assertEqual(
            _endpoint_absence_detail(_endpoint(console=False), _scope(generic=True)),
            "endpoint_untagged",
        )

    def test_a_console_server_whose_tags_miss_the_include_set(self):
        # The customer case: console servers are tagged in Forward, but not
        # with the device include tags, so the endpoint scope never admits
        # them and every one reads as uncovered.
        self.assertEqual(
            _endpoint_absence_detail(_endpoint(tags=["Console"]), _scope()),
            "endpoint_untagged",
        )

    def test_all_match_requires_every_include_tag(self):
        row = _endpoint(tags=["A"])
        self.assertEqual(
            _endpoint_absence_detail(
                row, _scope(include_tags=["A", "B"], include_match="all")
            ),
            "endpoint_untagged",
        )
        self.assertEqual(
            _endpoint_absence_detail(
                row, _scope(include_tags=["A", "B"], include_match="any")
            ),
            "endpoint_in_scope",
        )

    def test_no_include_scoping_on_endpoints_means_only_excludes_apply(self):
        self.assertEqual(
            _endpoint_absence_detail(_endpoint(tags=[]), _scope(include_tags=[])),
            "endpoint_in_scope",
        )
        self.assertEqual(
            _endpoint_absence_detail(
                _endpoint(tags=["Skip"]), _scope(include_tags=[], exclude_tags=["Skip"])
            ),
            "endpoint_untagged",
        )

    def test_every_detail_has_an_operator_label(self):
        for reason in (
            "endpoint_scope_off",
            "endpoint_no_snmp",
            "endpoint_cimc",
            "endpoint_generic",
            "endpoint_untagged",
            "endpoint_in_scope",
        ):
            self.assertIn(reason, ENDPOINT_ABSENCE_DETAILS)


class AbsenceCensusEndpointsTest(SimpleTestCase):
    def _client(self, device_rows, endpoint_rows):
        client = Mock()
        client.run_nqe_query = Mock(side_effect=[device_rows, endpoint_rows])
        return client

    def test_the_endpoint_table_is_probed_even_with_endpoint_sync_off(self):
        # Turning endpoint sync off is exactly when every endpoint-derived
        # device would otherwise become `absent` - and prunable.
        client = self._client([], [_endpoint(name="cons-1")])
        kinds, details = _absence_census(
            {"cons-1"},
            client=client,
            network_id="n",
            snapshot_id="s",
            endpoint_scope={"enabled": False},
        )
        self.assertEqual(client.run_nqe_query.call_count, 2)
        self.assertEqual(kinds, {"cons-1": "untagged"})
        self.assertEqual(details, {"cons-1": "endpoint_scope_off"})

    def test_kinds_stay_three_valued_and_details_refine_them(self):
        client = self._client(
            [{"name": "sw-1", "vendor": "Vendor.CISCO"}],
            [
                _endpoint(name="cons-1", tags=["Console"]),
                _endpoint(name="cimc-1", cimc=True),
            ],
        )
        kinds, details = _absence_census(
            {"sw-1", "cons-1", "cimc-1", "gone-1"},
            client=client,
            network_id="n",
            snapshot_id="s",
            endpoint_scope=_scope(),
        )
        self.assertEqual(
            kinds,
            {
                "sw-1": "untagged",
                "cons-1": "untagged",
                "cimc-1": "untagged",
                "gone-1": "absent",
            },
        )
        self.assertEqual(
            details, {"cons-1": "endpoint_untagged", "cimc-1": "endpoint_cimc"}
        )

    def test_the_summary_breaks_untagged_down_by_endpoint_reason(self):
        kinds = {
            "cons-1": "untagged",
            "cons-2": "untagged",
            "cimc-1": "untagged",
            "sw-1": "untagged",
        }
        details = {
            "cons-1": "endpoint_untagged",
            "cons-2": "endpoint_untagged",
            "cimc-1": "endpoint_cimc",
        }
        summary = _absence_summary(set(kinds), kinds, details)
        self.assertEqual(summary["present_untagged"], 4)
        self.assertEqual(
            [(row["reason"], row["count"]) for row in summary["endpoint_detail"]],
            [("endpoint_cimc", 1), ("endpoint_untagged", 2)],
        )

    def test_a_failed_endpoint_probe_leaves_the_census_unavailable(self):
        client = Mock()
        client.run_nqe_query = Mock(side_effect=[[], RuntimeError("boom")])
        self.assertIsNone(
            _absence_census({"x"}, client=client, network_id="n", snapshot_id="s")
        )
