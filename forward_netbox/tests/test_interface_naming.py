# Tests for interface-name abbreviation matching (Mgmt_<iface> primary-IP feature).
from django.test import SimpleTestCase

from forward_netbox.utilities.interface_naming import canonical_interface_key
from forward_netbox.utilities.interface_naming import interface_names_match
from forward_netbox.utilities.interface_naming import parse_mgmt_tag
from forward_netbox.utilities.interface_naming import resolve_mgmt_interface_name


class CanonicalInterfaceKeyTest(SimpleTestCase):
    def test_abbreviated_and_expanded_share_a_key(self):
        self.assertEqual(canonical_interface_key("Vl211"), ("vlan", "211"))
        self.assertEqual(canonical_interface_key("Vlan211"), ("vlan", "211"))
        self.assertEqual(
            canonical_interface_key("Vl211"), canonical_interface_key("Vlan211")
        )

    def test_slotted_names(self):
        self.assertEqual(canonical_interface_key("Gi0/0"), ("gigabitethernet", "0/0"))
        self.assertEqual(
            canonical_interface_key("GigabitEthernet0/0"),
            ("gigabitethernet", "0/0"),
        )

    def test_loopback_and_port_channel(self):
        self.assertEqual(canonical_interface_key("Lo0"), ("loopback", "0"))
        self.assertEqual(canonical_interface_key("Loopback0"), ("loopback", "0"))
        self.assertEqual(canonical_interface_key("Po10"), ("port-channel", "10"))
        self.assertEqual(
            canonical_interface_key("Port-channel10"), ("port-channel", "10")
        )

    def test_unknown_prefix_falls_back_to_identity(self):
        # Unknown type still produces a key so identical forms match.
        self.assertEqual(canonical_interface_key("Xyz5"), ("xyz", "5"))

    def test_non_interface_strings_return_none(self):
        self.assertIsNone(canonical_interface_key(""))
        self.assertIsNone(canonical_interface_key(None))
        self.assertIsNone(canonical_interface_key("management"))


class InterfaceNamesMatchTest(SimpleTestCase):
    def test_abbrev_vs_expanded(self):
        self.assertTrue(interface_names_match("Vl211", "Vlan211"))
        self.assertTrue(interface_names_match("Gi0/1", "GigabitEthernet0/1"))
        self.assertTrue(interface_names_match("Loopback0", "Lo0"))

    def test_wells_live_abbreviations(self):
        # Forms observed in live Wells Mgmt_ tags vs collected (lowercase) names.
        self.assertTrue(interface_names_match("v910", "vlan910"))
        self.assertTrue(interface_names_match("v201", "vlan201"))
        self.assertTrue(interface_names_match("Ma0", "mgmt0"))
        self.assertTrue(interface_names_match("Ma1", "mgmt1"))
        self.assertTrue(interface_names_match("G0/0/3", "gi0/0/3"))
        self.assertTrue(interface_names_match("Lo0", "loopback0"))
        # A bare v<num> must not match a non-vlan interface of the same number.
        self.assertFalse(interface_names_match("v910", "gi0/910"))

    def test_case_insensitive_exact(self):
        self.assertTrue(interface_names_match("vlan211", "Vlan211"))

    def test_mismatched_numbers_do_not_match(self):
        self.assertFalse(interface_names_match("Vl211", "Vlan212"))
        self.assertFalse(interface_names_match("Gi0/0", "Gi0/1"))

    def test_different_types_do_not_match(self):
        self.assertFalse(interface_names_match("Vl211", "Lo211"))

    def test_none_safe(self):
        self.assertFalse(interface_names_match(None, "Vlan1"))
        self.assertFalse(interface_names_match("Vlan1", None))


class ParseMgmtTagTest(SimpleTestCase):
    def test_extracts_interface_token(self):
        self.assertEqual(parse_mgmt_tag("Mgmt_Vl211"), "Vl211")
        self.assertEqual(parse_mgmt_tag("mgmt_Gi0/0"), "Gi0/0")

    def test_non_mgmt_tag_returns_none(self):
        self.assertIsNone(parse_mgmt_tag("Prot_BGP"))
        self.assertIsNone(parse_mgmt_tag("Mgmt_"))
        self.assertIsNone(parse_mgmt_tag(""))
        self.assertIsNone(parse_mgmt_tag(None))


class ResolveMgmtInterfaceNameTest(SimpleTestCase):
    def test_resolves_against_real_interface_list(self):
        interfaces = ["GigabitEthernet0/0", "Vlan211", "Loopback0"]
        self.assertEqual(
            resolve_mgmt_interface_name("Mgmt_Vl211", interfaces), "Vlan211"
        )
        self.assertEqual(
            resolve_mgmt_interface_name("Mgmt_Lo0", interfaces), "Loopback0"
        )

    def test_no_match_returns_none(self):
        self.assertIsNone(resolve_mgmt_interface_name("Mgmt_Vl999", ["Vlan211"]))
        self.assertIsNone(resolve_mgmt_interface_name("Prot_BGP", ["Vlan211"]))
