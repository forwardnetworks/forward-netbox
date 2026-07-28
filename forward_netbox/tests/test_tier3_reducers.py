from django.test import SimpleTestCase

from forward_netbox.utilities.tier3_reducers import diff_normalized_model_rows
from forward_netbox.utilities.tier3_reducers import reduce_contributor_rows
from forward_netbox.utilities.tier3_reducers import ScopeSide


def _scope(*, devices=(), include=("managed",), sync_device_tags=()):
    return ScopeSide(
        include_tags=frozenset(include),
        exclude_tags=frozenset(),
        include_match="any",
        scoped_device_names=frozenset(devices),
        scoped_site_names=frozenset(),
        sync_device_tags=frozenset(sync_device_tags),
    )


IN_SCOPE = _scope(devices={"device-a"})
OUT_OF_SCOPE = _scope(devices={"other-device"})


class Tier3ReducerTest(SimpleTestCase):
    def _assert_scope_entry_and_exit(
        self,
        *,
        reducer_id,
        model_string,
        contributor,
        coalesce_fields,
        scope_by_membership=False,
    ):
        if scope_by_membership:
            before_contributors = after_contributors = [contributor]
            in_scope = IN_SCOPE
            out_of_scope = OUT_OF_SCOPE
        else:
            before_contributors = [{**contributor, "contributor_tags": ["managed"]}]
            after_contributors = [{**contributor, "contributor_tags": ["unmanaged"]}]
            in_scope = IN_SCOPE
            out_of_scope = IN_SCOPE

        present = reduce_contributor_rows(
            reducer_id,
            before_contributors,
            in_scope,
        )
        absent = reduce_contributor_rows(
            reducer_id,
            after_contributors,
            out_of_scope,
        )
        upserts, deletes = diff_normalized_model_rows(
            model_string,
            present,
            absent,
            coalesce_fields,
        )
        self.assertEqual(upserts, [])
        self.assertEqual(deletes, present)

        upserts, deletes = diff_normalized_model_rows(
            model_string,
            absent,
            present,
            coalesce_fields,
        )
        self.assertEqual(upserts, present)
        self.assertEqual(deletes, [])

    def test_scope_entry_and_exit_is_preserved_for_every_tier3_map(self):
        cases = [
            {
                "reducer_id": "tier3_locations",
                "model_string": "dcim.site",
                "contributor": {
                    "contributor_device": "device-a",
                    "name": "site-a",
                    "slug": "site-a",
                    "status": "active",
                    "physical_address": "city, country",
                    "comments": "managed",
                },
                "coalesce_fields": [["slug"], ["name"]],
            },
            {
                "reducer_id": "tier3_vlans",
                "model_string": "ipam.vlan",
                "contributor": {
                    "contributor_device": "device-a",
                    "site": "site-a",
                    "site_slug": "site-a",
                    "vid": 100,
                    "name": "users",
                },
                "coalesce_fields": [["site", "vid"]],
            },
            {
                "reducer_id": "tier3_vrfs",
                "model_string": "ipam.vrf",
                "contributor": {
                    "contributor_device": "device-a",
                    "name": "blue",
                    "rd": None,
                    "description": "",
                    "enforce_unique": False,
                },
                "coalesce_fields": [["name"]],
            },
            {
                "reducer_id": "tier3_prefixes",
                "model_string": "ipam.prefix",
                "contributor": {
                    "contributor_device": "device-a",
                    "vrf": None,
                    "prefix": "192.0.2.0/24",
                    "status": "active",
                },
                "coalesce_fields": [["prefix", "vrf"]],
                "label": "prefixes_ipv4",
            },
            {
                "reducer_id": "tier3_prefixes",
                "model_string": "ipam.prefix",
                "contributor": {
                    "contributor_device": "device-a",
                    "vrf": None,
                    "prefix": "2001:db8::/64",
                    "status": "active",
                },
                "coalesce_fields": [["prefix", "vrf"]],
                "label": "prefixes_ipv6",
            },
            {
                "reducer_id": "tier3_hsrp_groups",
                "model_string": "ipam.fhrpgroup",
                "contributor": {
                    "device": "device-a",
                    "protocol": "hsrp",
                    "group_id": 10,
                    "name": "hsrp",
                    "interface": "Vlan10",
                    "vrf": None,
                    "address": "192.0.2.1/32",
                    "state": "active",
                    "priority": 100,
                    "status": "active",
                },
                "coalesce_fields": [["protocol", "group_id", "address", "vrf"]],
            },
            {
                "reducer_id": "tier3_mac_addresses",
                "model_string": "dcim.macaddress",
                "contributor": {
                    "device": "device-a",
                    "interface": "Ethernet1",
                    "mac": "00:11:22:33:44:55",
                    "mac_address": "00:11:22:33:44:55",
                },
                "coalesce_fields": [["mac_address"]],
                "scope_by_membership": True,
            },
            {
                "reducer_id": "tier3_ip_addresses",
                "model_string": "ipam.ipaddress",
                "contributor": {
                    "device": "device-a",
                    "interface": "Ethernet1",
                    "vrf": None,
                    "address": "192.0.2.0/24",
                    "host_ip": "192.0.2.10",
                    "prefix_length": 24,
                    "status": "active",
                },
                "coalesce_fields": [["address", "vrf"], ["address"]],
                "scope_by_membership": True,
                "label": "ip_addresses_ipv4",
            },
            {
                "reducer_id": "tier3_ip_addresses",
                "model_string": "ipam.ipaddress",
                "contributor": {
                    "device": "device-a",
                    "interface": "Ethernet1",
                    "vrf": None,
                    "address": "2001:db8::/64",
                    "host_ip": "2001:db8::10",
                    "prefix_length": 64,
                    "status": "active",
                },
                "coalesce_fields": [["address", "vrf"], ["address"]],
                "scope_by_membership": True,
                "label": "ip_addresses_ipv6",
            },
            {
                "reducer_id": "tier3_device_feature_tags",
                "model_string": "extras.taggeditem",
                "contributor": {
                    "candidate_kind": "structured_rule",
                    "candidate_source_key": "bgp|routing",
                    "device": "device-a",
                    "tag": "Routing",
                    "tag_slug": "routing",
                    "tag_color": "abcdef",
                },
                "coalesce_fields": [["device", "tag_slug"]],
                "scope_by_membership": True,
            },
        ]
        for case in cases:
            with self.subTest(case.get("label") or case["reducer_id"]):
                self._assert_scope_entry_and_exit(
                    reducer_id=case["reducer_id"],
                    model_string=case["model_string"],
                    contributor=case["contributor"],
                    coalesce_fields=case["coalesce_fields"],
                    scope_by_membership=case.get("scope_by_membership", False),
                )

    def test_mac_representative_falls_back_without_delete(self):
        selected = {
            "device": "device-a",
            "interface": "Ethernet1",
            "mac": "00:11:22:33:44:55",
            "mac_address": "00:11:22:33:44:55",
        }
        alternate = {
            **selected,
            "device": "device-b",
            "interface": "Ethernet2",
        }
        scope = _scope(devices={"device-a", "device-b"}, include=())

        before = reduce_contributor_rows(
            "tier3_mac_addresses",
            [selected, alternate],
            scope,
        )
        after = reduce_contributor_rows(
            "tier3_mac_addresses",
            [alternate],
            scope,
        )
        upserts, deletes = diff_normalized_model_rows(
            "dcim.macaddress",
            before,
            after,
            [["mac_address"]],
        )

        self.assertEqual(upserts, [after[0]])
        self.assertEqual(after[0]["device"], "device-b")
        self.assertEqual(deletes, [])

    def test_ip_representative_falls_back_without_delete(self):
        selected = {
            "device": "device-a",
            "interface": "Ethernet1",
            "vrf": None,
            "address": "192.0.2.0/24",
            "host_ip": "192.0.2.10",
            "prefix_length": 24,
            "status": "active",
        }
        alternate = {
            **selected,
            "device": "device-b",
            "interface": "Ethernet2",
        }
        scope = _scope(devices={"device-a", "device-b"}, include=())

        before = reduce_contributor_rows(
            "tier3_ip_addresses",
            [selected, alternate],
            scope,
        )
        after = reduce_contributor_rows(
            "tier3_ip_addresses",
            [alternate],
            scope,
        )
        upserts, deletes = diff_normalized_model_rows(
            "ipam.ipaddress",
            before,
            after,
            [["address", "vrf"], ["address"]],
        )

        self.assertEqual(upserts, [after[0]])
        self.assertEqual(after[0]["device"], "device-b")
        self.assertEqual(deletes, [])

    def test_vlan_selected_name_falls_back_deterministically(self):
        rows = [
            {
                "contributor_device": "device-a",
                "contributor_tags": ["managed"],
                "site": "site-a",
                "site_slug": "site-a",
                "vid": 100,
                "name": "VLAN 100",
            },
            {
                "contributor_device": "device-b",
                "contributor_tags": ["managed"],
                "site": "site-a",
                "site_slug": "site-a",
                "vid": 100,
                "name": "users",
            },
        ]

        preferred = reduce_contributor_rows("tier3_vlans", rows, IN_SCOPE)
        fallback = reduce_contributor_rows("tier3_vlans", rows[:1], IN_SCOPE)

        self.assertEqual(preferred[0]["name"], "users")
        self.assertEqual(fallback[0]["name"], "VLAN 100")

    def test_feature_tag_state_keeps_rules_and_filters_raw_tags(self):
        rows = [
            {
                "candidate_kind": "structured_rule",
                "candidate_source_key": "bgp|routing",
                "device": "device-a",
                "tag": "Routing",
                "tag_slug": "routing",
                "tag_color": "abcdef",
            },
            {
                "candidate_kind": "raw_forward_tag",
                "candidate_source_key": "Mgmt_Loopback0",
                "device": "device-a",
                "tag": "Mgmt_Loopback0",
                "tag_slug": "mgmt-loopback0",
                "tag_color": "9e9e9e",
            },
        ]
        disabled = reduce_contributor_rows(
            "tier3_device_feature_tags",
            rows,
            _scope(devices={"device-a"}, sync_device_tags=()),
        )
        enabled = reduce_contributor_rows(
            "tier3_device_feature_tags",
            rows,
            _scope(
                devices={"device-a"},
                sync_device_tags={"Mgmt_Loopback0"},
            ),
        )

        self.assertEqual([row["tag"] for row in disabled], ["Routing"])
        self.assertEqual(
            {row["tag"] for row in enabled},
            {"Routing", "Mgmt_Loopback0"},
        )
