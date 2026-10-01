import re
from pathlib import Path
from unittest import TestCase

from forward_netbox.utilities.query_execution_contract import (
    declared_query_parameters,
)
from forward_netbox.utilities.query_registry import QUERY_DIR

# The peer address-family query reads policy attachments from device
# configuration, which cannot be exercised outside Forward. These pin what can:
# the shape a pinned org's published copy depends on, and the properties of the
# patterns that were validated live.


def _source(name="forward_bgp_peer_address_families.nqe"):
    return Path(QUERY_DIR, name).read_text(encoding="utf-8")


def _code(name="forward_bgp_peer_address_families.nqe"):
    return re.sub(r"/\*.*?\*/", "", _source(name), flags=re.S)


class BgpPeerAfQueryContractTest(TestCase):
    def test_the_signature_is_unchanged_so_a_published_copy_keeps_working(self):
        declared = declared_query_parameters(_source())

        self.assertEqual([p.name for p in declared], ["forward_netbox_shard_keys"])

    def test_it_emits_the_four_policy_columns(self):
        code = _code()

        for column in (
            "routemap_in",
            "routemap_out",
            "prefixlist_in",
            "prefixlist_out",
        ):
            self.assertIn(f"{column}: if isPresent(", code)

    def test_a_row_without_a_policy_carries_null_not_a_missing_row(self):
        # The policy lookups are optional: an absent one must become null, never
        # drop the address-family row it hangs off.
        code = _code()

        self.assertEqual(code.count("else null : String"), 5)

    def test_it_never_emits_an_empty_list_literal(self):
        self.assertNotIn("[]", _code())

    def test_each_vendor_family_has_its_patterns(self):
        code = _code()

        for pattern in (
            "template peer {tmpl:string}",  # NX-OS peer templates
            "inherit peer {tmpl:string}",  # NX-OS inheritance
            "vrf {vrf:string}",  # VRF-scoped neighbors
            "address-family {afi:string} vrf {vrf:string}",  # IOS-XE VRF AFs
            "neighbor {peer:string} route-map {policy:string} {direction:string}",
            "neighbor {peer:string} prefix-list {policy:string} {direction:string}",
        ):
            self.assertIn(pattern, code)

    def test_capture_types_are_ones_nqe_accepts(self):
        # `{x:ip}` is a compile error; only string, number and ipv4Address work.
        code = _code()

        self.assertNotIn(":ip}", code)

    def test_the_configured_afi_text_maps_to_forwards_names(self):
        code = _code()

        for configured, forward in (
            ("ipv4 unicast", "AfiSafiType.IPV4_UNICAST"),
            ("l2vpn evpn", "AfiSafiType.L2VPN_EVPN"),
            ("vpnv4 unicast", "AfiSafiType.L3VPN_IPV4_UNICAST"),
        ):
            self.assertIn(f'"{configured}"', code)
            self.assertIn(forward, code)


class RouteMapQueryHoldersTest(TestCase):
    """A peer links to the route-map variant its own device holds, so the
    route-map rows must say who holds each non-owner variant."""

    def test_route_map_rows_carry_the_variant_holders(self):
        code = _code("forward_routing_route_maps.nqe")

        self.assertIn("holder_devices: r.holders", code)
        self.assertIn("has_variants: r.has_variants", code)

    def test_the_route_map_signature_is_unchanged(self):
        declared = declared_query_parameters(_source("forward_routing_route_maps.nqe"))

        self.assertEqual([p.name for p in declared], ["forward_netbox_shard_keys"])
