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


class AmbiguousManagementInterfaceTest(SimpleTestCase):
    """A tag can agree with several interfaces; the one that has an address wins.

    From a customer's data: `Mgmt_Ma1` agreed with `Management1` (a device-type
    template interface with no address) and `ma1` (the interface Forward reports,
    which carries the management address). Taking the first match left about three
    hundred devices without a primary IP.
    """

    def test_the_interface_with_an_address_wins_over_one_without(self):
        result = resolve_primary_ip_assignments(
            {"sw1": ["Mgmt_Ma1"]},
            {"sw1": {"Management1": [], "ma1": ["192.0.2.10/24"]}},
        )

        self.assertEqual(result["sw1"]["interface"], "ma1")
        self.assertEqual(result["sw1"]["v4"], "192.0.2.10/24")

    def test_it_does_not_matter_which_is_listed_first(self):
        for order in (("Management1", "ma1"), ("ma1", "Management1")):
            names = {name: [] for name in order}
            names["ma1"] = ["10.0.0.1/24"]

            result = resolve_primary_ip_assignments(
                {"sw1": ["Mgmt_Ma1"]}, {"sw1": names}
            )

            self.assertEqual(result["sw1"]["interface"], "ma1", order)

    def test_the_literal_name_wins_when_both_have_addresses(self):
        result = resolve_primary_ip_assignments(
            {"sw1": ["Mgmt_Ma1"]},
            {"sw1": {"Management1": ["10.0.0.9/24"], "ma1": ["10.0.0.1/24"]}},
        )

        self.assertEqual(result["sw1"]["interface"], "ma1")

    def test_a_gigabit_template_interface_without_an_address_is_skipped(self):
        result = resolve_primary_ip_assignments(
            {"r1": ["Mgmt_Gi0"]},
            {"r1": {"GigabitEthernet0": [], "gi0": ["192.0.2.20/24"]}},
        )

        self.assertEqual(result["r1"]["interface"], "gi0")

    def test_every_match_without_an_address_is_still_unresolved(self):
        result = resolve_primary_ip_assignments(
            {"sw1": ["Mgmt_Ma1"]},
            {"sw1": {"Management1": [], "ma1": []}},
        )

        self.assertNotIn("sw1", result)

    def test_an_unrelated_interface_with_an_address_is_never_used(self):
        result = resolve_primary_ip_assignments(
            {"sw1": ["Mgmt_Ma1"]},
            {"sw1": {"Management1": [], "Ethernet1": ["10.9.9.9/24"]}},
        )

        self.assertNotIn("sw1", result)


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


class DeviceReasonLinesTest(SimpleTestCase):
    def test_outcomes_round_trip_through_job_log_lines(self):
        from forward_netbox.utilities.primary_ip import format_device_reason_lines
        from forward_netbox.utilities.primary_ip import parse_device_reason_lines

        outcomes = {
            7: ("shared", 40),
            8: ("no-interface", None),
            9: ("holder", 12),
        }

        lines = format_device_reason_lines(outcomes)

        self.assertEqual(parse_device_reason_lines(lines), outcomes)

    def test_a_long_bucket_is_split_across_lines_and_still_parses(self):
        from forward_netbox.utilities.primary_ip import format_device_reason_lines
        from forward_netbox.utilities.primary_ip import parse_device_reason_lines

        outcomes = {pk: ("shared", pk + 1) for pk in range(1, 401)}

        lines = format_device_reason_lines(outcomes)

        self.assertGreater(len(lines), 1)
        self.assertEqual(parse_device_reason_lines(lines), outcomes)

    def test_a_marker_is_written_even_when_nothing_is_recorded(self):
        from forward_netbox.utilities.primary_ip import device_reasons_marked
        from forward_netbox.utilities.primary_ip import format_device_reason_lines

        lines = format_device_reason_lines({})

        self.assertTrue(device_reasons_marked(lines))
        self.assertFalse(device_reasons_marked(["primary_ip-from-tag: set primary"]))

    def test_a_malformed_id_list_is_ignored_rather_than_raised(self):
        from forward_netbox.utilities.primary_ip import parse_device_reason_lines

        self.assertEqual(
            parse_device_reason_lines(
                ["primary_ip-from-tag device reasons [shared]: 5>6>7,>5"]
            ),
            {},
        )

    def test_other_log_lines_are_ignored(self):
        from forward_netbox.utilities.primary_ip import parse_device_reason_lines

        self.assertEqual(
            parse_device_reason_lines(
                [None, "primary_ip-from-tag: set primary IP on 1 device(s)", ""]
            ),
            {},
        )
