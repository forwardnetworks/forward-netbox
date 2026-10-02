# Endpoint IPv4 addresses from SNMP's ipAddrTable carry a real netmask under
# `ipAdEntNetMask` (column .1.3); this pins the prefix-length lookup and the
# query's shape, since neither can be exercised outside Forward.
import re
from unittest import TestCase

from forward_netbox.utilities.query_registry import _read_query


def _code():
    src = _read_query("forward_ip_addresses_ipv4.nqe")
    return re.sub(r"/\*.*?\*/", "", src, flags=re.S)


class NetmaskPrefixLengthTableTest(TestCase):
    """Every valid IPv4 netmask maps to its prefix length; this is the only
    way to pin a long literal lookup table without running it in NQE."""

    _NETMASKS = {
        "0.0.0.0": 0,
        "128.0.0.0": 1,
        "255.0.0.0": 8,
        "255.255.0.0": 16,
        "255.255.255.0": 24,
        "255.255.255.128": 25,
        "255.255.255.192": 26,
        "255.255.255.224": 27,
        "255.255.255.240": 28,
        "255.255.255.248": 29,
        "255.255.255.252": 30,
        "255.255.255.254": 31,
        "255.255.255.255": 32,
    }

    def test_every_sample_netmask_has_its_own_clause(self):
        code = _code()
        for netmask, prefix_length in self._NETMASKS.items():
            self.assertIn(
                f'netmask == "{netmask}" then {prefix_length}',
                code,
                netmask,
            )

    def test_the_table_has_exactly_the_33_valid_netmasks(self):
        code = _code()
        start = code.index("netmaskPrefixLength(netmask: String) =")
        end = code.index(";", start)
        body = code[start:end]
        self.assertEqual(body.count('netmask == "'), 33)

    def test_an_unanswered_or_unrecognized_mask_falls_back_to_32(self):
        code = _code()
        start = code.index("netmaskPrefixLength(netmask: String) =")
        end = code.index(";", start)
        body = code[start:end]
        self.assertTrue(body.rstrip().endswith("else 32"))


class EndpointIpv4QueryShapeTest(TestCase):
    def test_it_reads_the_netmask_column_keyed_by_the_same_host_ip(self):
        code = _code()
        self.assertIn('maskOutput.requestedOid == "1.3.6.1.2.1.4.20"', code)
        self.assertIn('maskEntry.oid == "1.3.6.1.2.1.4.20.1.3." + hostIpRaw', code)

    def test_the_resolved_prefix_length_feeds_ipsubnet_not_a_literal_32(self):
        code = _code()
        self.assertIn("ipSubnet(ipAddress(hostIpRaw), prefixLength)", code)
        self.assertNotIn("ipSubnet(ipAddress(hostIpRaw), 32)", code)

    def test_a_missing_netmask_answer_still_defaults_to_a_32(self):
        code = _code()
        self.assertIn(
            "if isPresent(netmaskOpt) then netmaskPrefixLength(netmaskOpt) else 32",
            code,
        )
