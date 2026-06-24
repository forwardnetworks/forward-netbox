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
