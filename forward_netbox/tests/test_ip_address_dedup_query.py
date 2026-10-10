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

    # A virtual system or vdom reports its chassis's management address, and the
    # dedup hands each address to one device. The physical device must win even
    # when a virtual one sorts first by name; only a group with no physical
    # holder falls back to the lowest name.
    def _assert_physical_device_preferred(self, filename):
        source = read_builtin_query_source(filename)
        self.assertEqual(
            source.count("virtual_rank: if isPresent(device.system.physicalName)"),
            4,
            f"{filename}: every candidate source must rank a virtual device "
            "(physicalName differs from its name) after a physical one.",
        )
        self.assertEqual(
            source.count("candidate.virtual_rank == chosen_rank"),
            2,
            f"{filename}: both global and VRF dedup must pick the device "
            "among the lowest-ranked (physical) candidates.",
        )

    def test_ipv4_dedup_prefers_the_physical_device(self):
        self._assert_physical_device_preferred("forward_ip_addresses_ipv4.nqe")

    def test_ipv6_dedup_prefers_the_physical_device(self):
        self._assert_physical_device_preferred("forward_ip_addresses_ipv6.nqe")
