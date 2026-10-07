# Tests for the pure primary-IP resolver (Mgmt_<iface> feature).
from django.test import SimpleTestCase

from forward_netbox.utilities.primary_ip import resolve_primary_ip_assignments


class ResolvePrimaryIpAssignmentsTest(SimpleTestCase):
    def test_resolves_v4_on_abbreviated_tag(self):
        result = resolve_primary_ip_assignments(
            {"r1": ["Mgmt_Vl211"]},
            {
                "r1": {
                    "Vlan211": ["10.0.211.2/24"],
                    "GigabitEthernet0/0": ["10.1.1.1/30"],
                }
            },
        )
        self.assertEqual(result["r1"]["interface"], "Vlan211")
        self.assertEqual(result["r1"]["v4"], "10.0.211.2/24")
        self.assertIsNone(result["r1"]["v6"])

    def test_resolves_both_v4_and_v6(self):
        result = resolve_primary_ip_assignments(
            {"r1": ["Mgmt_Lo0"]},
            {"r1": {"Loopback0": ["192.0.2.1/32", "2001:db8::1/128"]}},
        )
        self.assertEqual(result["r1"]["v4"], "192.0.2.1/32")
        self.assertEqual(result["r1"]["v6"], "2001:db8::1/128")

    def test_lowest_address_wins_when_multiple(self):
        result = resolve_primary_ip_assignments(
            {"r1": ["Mgmt_Vl10"]},
            {"r1": {"Vlan10": ["10.0.0.5/24", "10.0.0.2/24", "10.0.0.9/24"]}},
        )
        self.assertEqual(result["r1"]["v4"], "10.0.0.2/24")

    def test_non_mgmt_tags_ignored(self):
        result = resolve_primary_ip_assignments(
            {"r1": ["Prot_BGP", "Site_NYC"]},
            {"r1": {"Vlan211": ["10.0.211.2/24"]}},
        )
        self.assertEqual(result, {})

    def test_unmatched_interface_skipped(self):
        result = resolve_primary_ip_assignments(
            {"r1": ["Mgmt_Vl999"]},
            {"r1": {"Vlan211": ["10.0.211.2/24"]}},
        )
        self.assertEqual(result, {})

    def test_matched_interface_without_ips_skipped(self):
        result = resolve_primary_ip_assignments(
            {"r1": ["Mgmt_Vl211"]},
            {"r1": {"Vlan211": []}},
        )
        self.assertEqual(result, {})

    def test_first_resolvable_tag_wins(self):
        result = resolve_primary_ip_assignments(
            {"r1": ["Mgmt_Vl999", "Mgmt_Lo0"]},
            {"r1": {"Loopback0": ["192.0.2.1/32"]}},
        )
        self.assertEqual(result["r1"]["interface"], "Loopback0")
        self.assertEqual(result["r1"]["v4"], "192.0.2.1/32")

    def test_bare_ip_without_mask(self):
        result = resolve_primary_ip_assignments(
            {"r1": ["Mgmt_Vl211"]},
            {"r1": {"Vlan211": ["10.0.211.2"]}},
        )
        self.assertEqual(result["r1"]["v4"], "10.0.211.2")

    def test_empty_inputs(self):
        self.assertEqual(resolve_primary_ip_assignments({}, {}), {})
        self.assertEqual(resolve_primary_ip_assignments(None, None), {})

    def test_multiple_devices(self):
        result = resolve_primary_ip_assignments(
            {"r1": ["Mgmt_Vl211"], "r2": ["Mgmt_Lo0"], "r3": ["Prot_BGP"]},
            {
                "r1": {"Vlan211": ["10.0.211.2/24"]},
                "r2": {"Loopback0": ["192.0.2.2/32"]},
                "r3": {"Vlan1": ["10.0.0.1/24"]},
            },
        )
        self.assertEqual(set(result.keys()), {"r1", "r2"})


class ResolveManagementIpAssignmentsTest(SimpleTestCase):
    """The fallback for devices with no Mgmt_ tag: narrow on purpose."""

    def _resolve(self, management_ips, interface_ips, **kwargs):
        from forward_netbox.utilities.primary_ip import (
            resolve_management_ip_assignments,
        )

        return resolve_management_ip_assignments(
            management_ips, interface_ips, **kwargs
        )

    def test_the_single_address_on_one_interface_is_chosen(self):
        result = self._resolve(
            {"r1": ["10.0.0.1"]},
            {"r1": {"Loopback0": ["10.0.0.1/32"], "Gi0/0": ["192.0.2.1/24"]}},
        )

        self.assertEqual(
            result, {"r1": {"interface": "Loopback0", "v4": "10.0.0.1/32", "v6": None}}
        )

    def test_the_netbox_prefix_length_does_not_matter(self):
        result = self._resolve(
            {"r1": ["10.0.0.1/24"]}, {"r1": {"Vlan10": ["10.0.0.1/32"]}}
        )

        self.assertEqual(result["r1"]["v4"], "10.0.0.1/32")

    def test_an_ipv6_management_address_fills_v6(self):
        result = self._resolve(
            {"r1": ["2001:db8::1"]}, {"r1": {"Lo0": ["2001:db8::1/128"]}}
        )

        self.assertEqual(
            result, {"r1": {"interface": "Lo0", "v4": None, "v6": "2001:db8::1/128"}}
        )

    def test_several_recorded_addresses_are_ambiguous_and_skipped(self):
        result = self._resolve(
            {"r1": ["10.0.0.1", "10.0.0.2"]},
            {"r1": {"Lo0": ["10.0.0.1/32"], "Lo1": ["10.0.0.2/32"]}},
        )

        self.assertEqual(result, {})

    def test_the_same_address_recorded_twice_is_one_address(self):
        result = self._resolve(
            {"r1": ["10.0.0.1", "10.0.0.1/32"]}, {"r1": {"Lo0": ["10.0.0.1/32"]}}
        )

        self.assertIn("r1", result)

    def test_an_address_on_no_interface_is_skipped(self):
        result = self._resolve({"r1": ["10.0.0.1"]}, {"r1": {"Lo0": ["10.9.9.9/32"]}})

        self.assertEqual(result, {})

    def test_an_address_on_two_interfaces_is_skipped(self):
        result = self._resolve(
            {"r1": ["10.0.0.1"]},
            {"r1": {"Lo0": ["10.0.0.1/32"], "Vlan1": ["10.0.0.1/24"]}},
        )

        self.assertEqual(result, {})

    def test_unresolved_devices_report_why(self):
        reasons = {}
        self._resolve(
            {
                "many": ["10.0.0.1", "10.0.0.2"],
                "none": ["10.0.0.3"],
                "two": ["10.0.0.4"],
            },
            {
                "many": {"Lo0": ["10.0.0.1/32"]},
                "none": {"Lo0": ["10.9.9.9/32"]},
                "two": {"Lo0": ["10.0.0.4/32"], "Vlan1": ["10.0.0.4/24"]},
            },
            reasons=reasons,
        )

        self.assertEqual(
            reasons,
            {
                "many": "multiple-addresses",
                "none": "no-interface",
                "two": "several-interfaces",
            },
        )

    def test_a_device_with_a_mgmt_tag_is_never_given_the_fallback(self):
        result = self._resolve(
            {"r1": ["10.0.0.1"]},
            {"r1": {"Lo0": ["10.0.0.1/32"]}},
            skip={"r1"},
        )

        self.assertEqual(result, {})

    def test_a_device_with_no_interfaces_or_garbage_addresses_is_skipped(self):
        result = self._resolve(
            {"r1": ["10.0.0.1"], "r2": ["not-an-ip"], "r3": []},
            {"r2": {"Lo0": ["10.0.0.1/32"]}},
        )

        self.assertEqual(result, {})


class FallbackSummaryTest(SimpleTestCase):
    def test_the_job_log_sentence_reads_back_as_counts(self):
        from forward_netbox.utilities.primary_ip import format_fallback_summary
        from forward_netbox.utilities.primary_ip import parse_fallback_summary

        counts = {
            "shared": 587,
            "no-interface": 55,
            "multiple-addresses": 12,
            "several-interfaces": 25,
        }
        parsed = parse_fallback_summary(format_fallback_summary(679, counts))
        self.assertEqual(
            parsed,
            {
                "left_without_primary_ip": 679,
                "shared_with_another_device": 587,
                "address_on_no_synced_interface": 55,
                "several_management_addresses": 12,
                "address_on_several_interfaces": 25,
            },
        )

    def test_any_other_line_is_not_a_summary(self):
        from forward_netbox.utilities.primary_ip import parse_fallback_summary

        self.assertIsNone(parse_fallback_summary("primary_ip-from-tag: set primary IP"))
        self.assertIsNone(parse_fallback_summary(None))
