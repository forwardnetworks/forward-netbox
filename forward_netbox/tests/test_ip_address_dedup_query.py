from django.test import SimpleTestCase

from forward_netbox.utilities.query_registry import read_builtin_query_source


class IpAddressDedupQueryTest(SimpleTestCase):
    # The IPv4/IPv6 IP queries collapse each address to one row. Both the global
    # (host_ip) and the VRF ({address,vrf}) dedup must pin the chosen interface to
    # the chosen device; choosing min(device) and min(interface) independently can
    # emit an impossible (device, interface) pair (interface that lives on a
    # different device), which the apply path then drops as "target interface was
    # not imported" and which strands Mgmt_-tag primary-IP resolution.
    def _assert_interface_pinned_to_device(self, filename):
        source = read_builtin_query_source(filename)
        self.assertEqual(
            source.count("candidate.device == chosen_device"),
            2,
            f"{filename}: both global and VRF dedup must pin the chosen "
            "interface to the chosen device (found a different count).",
        )

    def test_ipv4_dedup_pins_interface_to_device(self):
        self._assert_interface_pinned_to_device("forward_ip_addresses_ipv4.nqe")

    def test_ipv6_dedup_pins_interface_to_device(self):
        self._assert_interface_pinned_to_device("forward_ip_addresses_ipv6.nqe")
