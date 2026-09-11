from types import SimpleNamespace
from unittest import TestCase

from forward_netbox.utilities.sync_aci import apply_netbox_cisco_aci_aciappprofile
from forward_netbox.utilities.sync_aci import apply_netbox_cisco_aci_acibridgedomain
from forward_netbox.utilities.sync_aci import apply_netbox_cisco_aci_acicontract
from forward_netbox.utilities.sync_aci import apply_netbox_cisco_aci_aciendpointgroup
from forward_netbox.utilities.sync_aci import apply_netbox_cisco_aci_acifabric
from forward_netbox.utilities.sync_aci import apply_netbox_cisco_aci_acifilter
from forward_netbox.utilities.sync_aci import apply_netbox_cisco_aci_acifilterentry
from forward_netbox.utilities.sync_aci import apply_netbox_cisco_aci_acil3out
from forward_netbox.utilities.sync_aci import apply_netbox_cisco_aci_acinode
from forward_netbox.utilities.sync_aci import apply_netbox_cisco_aci_acipod
from forward_netbox.utilities.sync_aci import apply_netbox_cisco_aci_acisubject
from forward_netbox.utilities.sync_aci import apply_netbox_cisco_aci_acitenant
from forward_netbox.utilities.sync_aci import apply_netbox_cisco_aci_acivrf
from forward_netbox.utilities.sync_aci import delete_netbox_cisco_aci_acibridgedomain
from forward_netbox.utilities.sync_aci import delete_netbox_cisco_aci_acifabric
from forward_netbox.utilities.sync_aci import delete_netbox_cisco_aci_acifilter
from forward_netbox.utilities.sync_aci import delete_netbox_cisco_aci_acil3out
from forward_netbox.utilities.sync_aci import delete_netbox_cisco_aci_acinode
from forward_netbox.utilities.sync_aci import delete_netbox_cisco_aci_acipod
from forward_netbox.utilities.sync_aci import delete_netbox_cisco_aci_acitenant
from forward_netbox.utilities.sync_aci import delete_netbox_cisco_aci_acivrf


class _FakeField:
    def __init__(self, name):
        self.name = name
        self.auto_created = False


class _FakeMeta:
    def __init__(self, label_lower, fields):
        self.label_lower = label_lower
        self.fields = [_FakeField(field_name) for field_name in fields]


class _FakeACIModel:
    pass


def _fake_model(label_lower, fields):
    return type(
        str(label_lower).replace(".", "_"),
        (_FakeACIModel,),
        {"_meta": _FakeMeta(label_lower, fields)},
    )


class _ACIRunner:
    def __init__(self):
        self.models = {
            "ACIFabric": _fake_model(
                "netbox_cisco_aci.acifabric",
                ("name", "fabric_id", "description"),
            ),
            "ACIPod": _fake_model(
                "netbox_cisco_aci.acipod",
                ("aci_fabric", "name", "pod_id", "description"),
            ),
            "ACINode": _fake_model(
                "netbox_cisco_aci.acinode",
                (
                    "aci_pod",
                    "node_id",
                    "name",
                    "role",
                    "node_type",
                    "serial_number",
                    "pod_tep_pool",
                    "firmware_version",
                    "node_object_type",
                    "node_object_id",
                    "description",
                ),
            ),
            "ACITenant": _fake_model(
                "netbox_cisco_aci.acitenant",
                ("aci_fabric", "name", "description"),
            ),
            "ACIAppProfile": _fake_model(
                "netbox_cisco_aci.aciappprofile",
                ("aci_tenant", "name", "description"),
            ),
            "ACIEndpointGroup": _fake_model(
                "netbox_cisco_aci.aciendpointgroup",
                (
                    "aci_tenant",
                    "aci_app_profile",
                    "aci_bridge_domain",
                    "name",
                    "admin_shutdown",
                    "is_useg",
                    "intra_epg_isolation",
                    "preferred_group_member",
                    "qos_class",
                    "description",
                ),
            ),
            "ACIContract": _fake_model(
                "netbox_cisco_aci.acicontract",
                (
                    "aci_tenant",
                    "name",
                    "scope",
                    "qos_class",
                    "target_dscp",
                    "description",
                ),
            ),
            "ACISubject": _fake_model(
                "netbox_cisco_aci.acisubject",
                (
                    "aci_contract",
                    "name",
                    "apply_both_directions",
                    "reverse_filter_ports",
                    "qos_class",
                    "target_dscp",
                    "description",
                ),
            ),
            "ACIFilterEntry": _fake_model(
                "netbox_cisco_aci.acifilterentry",
                (
                    "aci_filter",
                    "name",
                    "ether_type",
                    "ip_protocol",
                    "source_port_from",
                    "source_port_to",
                    "destination_port_from",
                    "destination_port_to",
                    "tcp_rules",
                    "match_only_fragments",
                    "arp_opcode",
                    "stateful",
                    "description",
                ),
            ),
            "ACIVRF": _fake_model(
                "netbox_cisco_aci.acivrf",
                (
                    "aci_tenant",
                    "name",
                    "policy_enforcement_preference",
                    "policy_enforcement_direction",
                    "bd_enforcement_enabled",
                    "preferred_group_enabled",
                    "description",
                ),
            ),
            "ACIBridgeDomain": _fake_model(
                "netbox_cisco_aci.acibridgedomain",
                (
                    "aci_tenant",
                    "aci_vrf",
                    "name",
                    "unicast_routing_enabled",
                    "arp_flooding_enabled",
                    "limit_ip_learn_to_subnets",
                    "l2_unknown_unicast",
                    "l3_unknown_multicast",
                    "multi_destination_flooding",
                    "mac_address",
                    "description",
                ),
            ),
            "ACIFilter": _fake_model(
                "netbox_cisco_aci.acifilter",
                ("aci_tenant", "name", "description"),
            ),
            "ACIL3Out": _fake_model(
                "netbox_cisco_aci.acil3out",
                (
                    "aci_tenant",
                    "aci_vrf",
                    "name",
                    "protocol_bgp",
                    "protocol_ospf",
                    "protocol_eigrp",
                    "protocol_static",
                    "target_dscp",
                    "description",
                ),
            ),
        }
        self.upserts = []
        self.deletes = []
        self.lookups = {}
        self.devices = {}
        self.interfaces = {}
        self.skip_warnings = []

    def _parent_key(self, obj):
        return getattr(obj, "pk", obj)

    def _optional_model(self, app_label, model_name, model_string):
        self.last_optional_lookup = (app_label, model_name, model_string)
        return self.models[model_name]

    def _model_field_values(self, model, values):
        field_names = {field.name for field in model._meta.fields}
        return {key: value for key, value in values.items() if key in field_names}

    def _coalesce_sets_for(self, model_string, default_sets):
        return default_sets

    def _upsert_values_from_defaults(
        self, model_string, model, *, values, coalesce_sets
    ):
        existing = self._lookup_existing(model, values, coalesce_sets)
        if existing is not None:
            return existing, False
        obj = SimpleNamespace(
            pk=len(self.upserts) + 1,
            model_string=model_string,
            **values,
        )
        self.upserts.append(
            {
                "model_string": model_string,
                "values": values,
                "coalesce_sets": coalesce_sets,
            }
        )
        if model_string == "netbox_cisco_aci.acifabric":
            self.lookups[(model, ("name", values["name"]))] = obj
        elif model_string == "netbox_cisco_aci.acipod":
            self.lookups[
                (
                    model,
                    (
                        "aci_fabric",
                        self._parent_key(values["aci_fabric"]),
                        "pod_id",
                        values["pod_id"],
                    ),
                )
            ] = obj
        elif model_string == "netbox_cisco_aci.acitenant":
            self.lookups[
                (
                    model,
                    (
                        "aci_fabric",
                        self._parent_key(values["aci_fabric"]),
                        "name",
                        values["name"],
                    ),
                )
            ] = obj
        elif model_string in {
            "netbox_cisco_aci.acivrf",
            "netbox_cisco_aci.acifilter",
            "netbox_cisco_aci.aciappprofile",
            "netbox_cisco_aci.acicontract",
        }:
            self.lookups[
                (
                    model,
                    (
                        "aci_tenant",
                        self._parent_key(values["aci_tenant"]),
                        "name",
                        values["name"],
                    ),
                )
            ] = obj
        elif model_string == "netbox_cisco_aci.acibridgedomain":
            self.lookups[
                (
                    model,
                    (
                        "aci_tenant",
                        self._parent_key(values["aci_tenant"]),
                        "name",
                        values["name"],
                    ),
                )
            ] = obj
        elif model_string == "netbox_cisco_aci.acil3out":
            self.lookups[
                (
                    model,
                    (
                        "aci_tenant",
                        self._parent_key(values["aci_tenant"]),
                        "name",
                        values["name"],
                    ),
                )
            ] = obj
        elif model_string == "netbox_cisco_aci.acinode":
            self.lookups[
                (
                    model,
                    (
                        "aci_pod",
                        self._parent_key(values["aci_pod"]),
                        "node_id",
                        values["node_id"],
                    ),
                )
            ] = obj
            self.lookups[
                (
                    model,
                    (
                        "aci_pod",
                        self._parent_key(values["aci_pod"]),
                        "name",
                        values["name"],
                    ),
                )
            ] = obj
        return obj, True

    def _lookup_existing(self, model, values, coalesce_sets):
        for coalesce_set in coalesce_sets:
            if coalesce_set == ("name",):
                existing = self.lookups.get((model, ("name", values["name"])))
            elif coalesce_set == ("aci_fabric", "pod_id"):
                existing = self.lookups.get(
                    (
                        model,
                        (
                            "aci_fabric",
                            self._parent_key(values["aci_fabric"]),
                            "pod_id",
                            values["pod_id"],
                        ),
                    )
                )
            elif coalesce_set == ("aci_fabric", "name"):
                existing = self.lookups.get(
                    (
                        model,
                        (
                            "aci_fabric",
                            self._parent_key(values["aci_fabric"]),
                            "name",
                            values["name"],
                        ),
                    )
                )
            elif coalesce_set == ("aci_tenant", "name"):
                existing = self.lookups.get(
                    (
                        model,
                        (
                            "aci_tenant",
                            self._parent_key(values["aci_tenant"]),
                            "name",
                            values["name"],
                        ),
                    )
                )
            elif coalesce_set == ("aci_app_profile", "name"):
                existing = self.lookups.get(
                    (
                        model,
                        (
                            "aci_app_profile",
                            self._parent_key(values["aci_app_profile"]),
                            "name",
                            values["name"],
                        ),
                    )
                )
            elif coalesce_set == (
                "aci_endpoint_group",
                "dcim_interface",
                "encap_vlan",
            ):
                existing = self.lookups.get(
                    (
                        model,
                        (
                            "aci_endpoint_group",
                            self._parent_key(values["aci_endpoint_group"]),
                            "dcim_interface",
                            self._parent_key(values["dcim_interface"]),
                            "encap_vlan",
                            values["encap_vlan"],
                        ),
                    )
                )
            elif coalesce_set == ("aci_pod", "name"):
                existing = self.lookups.get(
                    (
                        model,
                        (
                            "aci_pod",
                            self._parent_key(values["aci_pod"]),
                            "name",
                            values["name"],
                        ),
                    )
                )
            elif coalesce_set == ("aci_pod", "node_id"):
                existing = self.lookups.get(
                    (
                        model,
                        (
                            "aci_pod",
                            self._parent_key(values["aci_pod"]),
                            "node_id",
                            values["node_id"],
                        ),
                    )
                )
            else:
                existing = None
            if existing is not None:
                return existing
        return None

    def _lookup_device_by_name(self, device_name):
        return self.devices.get(device_name)

    def _lookup_device_by_name_insensitive(self, device_name):
        matches = [
            device
            for name, device in self.devices.items()
            if name.lower() == str(device_name).lower()
        ]
        if len(matches) == 1:
            return matches[0]
        return "ambiguous" if matches else None

    def _record_aggregated_skip_warning(self, **kwargs):
        self.skip_warnings.append(kwargs)

    def _lookup_interface(self, device, interface_name):
        if device is None:
            return None
        return self.interfaces.get((device.pk, interface_name))

    def _content_type_for(self, model):
        return model

    def _get_unique_or_raise(self, model, lookup):
        if set(lookup) == {"name"}:
            return self.lookups.get((model, ("name", lookup["name"])))
        if set(lookup) == {"aci_fabric", "pod_id"}:
            return self.lookups.get(
                (
                    model,
                    (
                        "aci_fabric",
                        self._parent_key(lookup["aci_fabric"]),
                        "pod_id",
                        lookup["pod_id"],
                    ),
                )
            )
        if set(lookup) == {"aci_fabric", "name"}:
            return self.lookups.get(
                (
                    model,
                    (
                        "aci_fabric",
                        self._parent_key(lookup["aci_fabric"]),
                        "name",
                        lookup["name"],
                    ),
                )
            )
        for parent_field in ("aci_app_profile", "aci_contract", "aci_filter"):
            if set(lookup) == {parent_field, "name"}:
                return self.lookups.get(
                    (
                        model,
                        (
                            parent_field,
                            self._parent_key(lookup[parent_field]),
                            "name",
                            lookup["name"],
                        ),
                    )
                )
        if set(lookup) == {"aci_tenant", "name"}:
            return self.lookups.get(
                (
                    model,
                    (
                        "aci_tenant",
                        self._parent_key(lookup["aci_tenant"]),
                        "name",
                        lookup["name"],
                    ),
                )
            )
        if set(lookup) == {"aci_app_profile", "name"}:
            return self.lookups.get(
                (
                    model,
                    (
                        "aci_app_profile",
                        self._parent_key(lookup["aci_app_profile"]),
                        "name",
                        lookup["name"],
                    ),
                )
            )
        if set(lookup) == {"aci_pod", "node_id"}:
            return self.lookups.get(
                (
                    model,
                    (
                        "aci_pod",
                        self._parent_key(lookup["aci_pod"]),
                        "node_id",
                        lookup["node_id"],
                    ),
                )
            )
        if set(lookup) == {"aci_pod", "name"}:
            return self.lookups.get(
                (
                    model,
                    (
                        "aci_pod",
                        self._parent_key(lookup["aci_pod"]),
                        "name",
                        lookup["name"],
                    ),
                )
            )
        return None

    def _delete_by_coalesce(self, model, lookups):
        self.deletes.append({"model": model, "lookups": lookups})
        return True


class SyncACIAdapterTest(TestCase):
    def test_apply_acinode_upserts_fabric_pod_and_node_contract_fields(self):
        runner = _ACIRunner()

        apply_netbox_cisco_aci_acinode(
            runner,
            {
                "fabric_name": "fabric-a",
                "pod_name": "pod-1",
                "pod_id": "1",
                "node_id": "101",
                "name": "leaf-101",
                "role": "leaf",
                "node_type": "physical",
                "serial_number": "SERIAL1",
                "pod_tep_pool": "10.0.0.1",
                "firmware_version": "",
                "node_object_name": "leaf-101",
                "description": "Forward observed ACI node.",
            },
        )

        self.assertEqual(
            [call["model_string"] for call in runner.upserts],
            [
                "netbox_cisco_aci.acifabric",
                "netbox_cisco_aci.acipod",
                "netbox_cisco_aci.acinode",
            ],
        )
        self.assertEqual(runner.upserts[0]["values"]["name"], "fabric-a")
        self.assertEqual(runner.upserts[1]["values"]["pod_id"], 1)
        self.assertEqual(runner.upserts[2]["values"]["node_id"], 101)
        self.assertEqual(runner.upserts[2]["values"]["role"], "leaf")
        self.assertEqual(runner.upserts[2]["values"]["node_type"], "physical")
        self.assertEqual(
            runner.upserts[2]["coalesce_sets"],
            [("aci_pod", "node_id"), ("aci_pod", "name")],
        )

    def test_acinode_links_its_device_case_insensitively(self):
        # APIC names nodes as configured (DC01LEAF101); Forward collected the
        # device as dc01leaf101. 0 of 667 nodes matched exactly on a real fabric.
        runner = _ACIRunner()
        device = SimpleNamespace(pk=7, __class__=type("Device", (), {}))
        runner.devices["dc01leaf101"] = device

        apply_netbox_cisco_aci_acinode(
            runner,
            {
                "fabric_name": "fabric-a",
                "pod_name": "pod-1",
                "pod_id": "1",
                "node_id": "101",
                "name": "DC01LEAF101",
                "role": "leaf",
                "node_type": "physical",
                "serial_number": "SERIAL1",
                "pod_tep_pool": "10.0.0.1",
                "firmware_version": "",
                "node_object_name": "DC01LEAF101",
                "description": "Forward observed ACI node.",
            },
        )

        self.assertEqual(runner.upserts[2]["values"]["node_object_id"], 7)
        self.assertEqual(runner.skip_warnings, [])

    def test_acinode_without_a_device_is_stored_unlinked_and_says_so(self):
        runner = _ACIRunner()
        apply_netbox_cisco_aci_acinode(
            runner,
            {
                "fabric_name": "fabric-a",
                "pod_name": "pod-1",
                "pod_id": "1",
                "node_id": "101",
                "name": "leaf-101",
                "role": "leaf",
                "node_type": "physical",
                "serial_number": "SERIAL1",
                "pod_tep_pool": "10.0.0.1",
                "firmware_version": "",
                "node_object_name": "leaf-101",
                "description": "Forward observed ACI node.",
            },
        )
        self.assertIsNone(runner.upserts[2]["values"]["node_object_id"])
        self.assertEqual(
            [w["reason"] for w in runner.skip_warnings], ["aci-node-device-missing"]
        )

    def test_acinode_never_guesses_between_two_case_variants(self):
        runner = _ACIRunner()
        runner.devices["leaf-101"] = SimpleNamespace(pk=1, __class__=type("D", (), {}))
        runner.devices["LEAF-101"] = SimpleNamespace(pk=2, __class__=type("D", (), {}))
        apply_netbox_cisco_aci_acinode(
            runner,
            {
                "fabric_name": "fabric-a",
                "pod_name": "pod-1",
                "pod_id": "1",
                "node_id": "101",
                "name": "Leaf-101",
                "role": "leaf",
                "node_type": "physical",
                "serial_number": "SERIAL1",
                "pod_tep_pool": "10.0.0.1",
                "firmware_version": "",
                "node_object_name": "Leaf-101",
                "description": "Forward observed ACI node.",
            },
        )
        self.assertIsNone(runner.upserts[2]["values"]["node_object_id"])
        self.assertEqual(
            [w["reason"] for w in runner.skip_warnings], ["aci-node-device-ambiguous"]
        )

    def test_apply_acinode_skips_duplicate_observation_in_same_run(self):
        runner = _ACIRunner()
        row = {
            "fabric_name": "fabric-a",
            "pod_name": "pod-1",
            "pod_id": "1",
            "node_id": "101",
            "name": "leaf-101",
            "role": "leaf",
            "node_type": "physical",
            "serial_number": "SERIAL1",
            "pod_tep_pool": "10.0.0.1",
            "firmware_version": "",
            "node_object_name": "leaf-101",
            "description": "Forward observed ACI node.",
        }

        first_node = apply_netbox_cisco_aci_acinode(runner, row)
        duplicate_node = apply_netbox_cisco_aci_acinode(
            runner,
            {
                **row,
                "serial_number": "SERIAL2",
                "pod_tep_pool": "10.0.0.2",
            },
        )

        self.assertIs(duplicate_node, first_node)
        self.assertEqual(
            [call["model_string"] for call in runner.upserts],
            [
                "netbox_cisco_aci.acifabric",
                "netbox_cisco_aci.acipod",
                "netbox_cisco_aci.acinode",
            ],
        )

    def test_delete_acipod_skips_malformed_pod_id(self):
        runner = _ACIRunner()
        runner.lookups[
            (
                runner.models["ACIFabric"],
                ("name", "fabric-a"),
            )
        ] = SimpleNamespace(pk=1, name="fabric-a")

        result = delete_netbox_cisco_aci_acipod(
            runner,
            {"fabric_name": "fabric-a", "pod_id": "not-an-int"},
        )

        self.assertFalse(result)
        self.assertEqual(runner.deletes, [])

    def test_apply_acivrf_upserts_fabric_tenant_and_vrf_contract_fields(self):
        runner = _ACIRunner()

        apply_netbox_cisco_aci_acivrf(
            runner,
            {
                "fabric_name": "fabric-a",
                "tenant_name": "tenant-a",
                "name": "vrf-a",
                "policy_enforcement_preference": "enforced",
                "policy_enforcement_direction": "ingress",
                "bd_enforcement_enabled": "false",
                "preferred_group_enabled": "true",
                "description": "Forward observed ACI VRF.",
            },
        )

        self.assertEqual(
            [call["model_string"] for call in runner.upserts],
            [
                "netbox_cisco_aci.acifabric",
                "netbox_cisco_aci.acitenant",
                "netbox_cisco_aci.acivrf",
            ],
        )
        self.assertEqual(runner.upserts[1]["values"]["name"], "tenant-a")
        self.assertEqual(runner.upserts[2]["values"]["name"], "vrf-a")
        self.assertFalse(runner.upserts[2]["values"]["bd_enforcement_enabled"])
        self.assertTrue(runner.upserts[2]["values"]["preferred_group_enabled"])
        self.assertEqual(
            runner.upserts[2]["coalesce_sets"],
            [("aci_tenant", "name")],
        )

    def test_apply_acifilter_upserts_under_common_tenant(self):
        runner = _ACIRunner()

        apply_netbox_cisco_aci_acifilter(
            runner,
            {
                "fabric_name": "fabric-a",
                "tenant_name": "common",
                "name": "filter-a",
                "description": "Forward observed ACI filter.",
            },
        )

        self.assertEqual(
            [call["model_string"] for call in runner.upserts],
            [
                "netbox_cisco_aci.acifabric",
                "netbox_cisco_aci.acitenant",
                "netbox_cisco_aci.acifilter",
            ],
        )
        self.assertEqual(runner.upserts[1]["values"]["name"], "common")
        self.assertEqual(runner.upserts[2]["values"]["name"], "filter-a")

    def test_remaining_aci_helpers_are_repeat_sync_noop(self):
        cases = [
            (
                "fabric",
                apply_netbox_cisco_aci_acifabric,
                {"name": "fabric-a", "fabric_id": 1},
            ),
            (
                "tenant",
                apply_netbox_cisco_aci_acitenant,
                {"fabric_name": "fabric-a", "name": "tenant-a"},
            ),
            (
                "vrf",
                apply_netbox_cisco_aci_acivrf,
                {
                    "fabric_name": "fabric-a",
                    "tenant_name": "tenant-a",
                    "name": "vrf-a",
                },
            ),
            (
                "bridge-domain",
                apply_netbox_cisco_aci_acibridgedomain,
                {
                    "fabric_name": "fabric-a",
                    "tenant_name": "tenant-a",
                    "vrf_tenant_name": "tenant-a",
                    "vrf_name": "vrf-a",
                    "name": "bd-a",
                },
            ),
            (
                "filter",
                apply_netbox_cisco_aci_acifilter,
                {
                    "fabric_name": "fabric-a",
                    "tenant_name": "tenant-a",
                    "name": "filter-a",
                },
            ),
            (
                "l3out",
                apply_netbox_cisco_aci_acil3out,
                {
                    "fabric_name": "fabric-a",
                    "tenant_name": "tenant-a",
                    "vrf_tenant_name": "tenant-a",
                    "vrf_name": "vrf-a",
                    "name": "l3out-a",
                },
            ),
        ]

        for label, apply_fn, row in cases:
            with self.subTest(label=label):
                runner = _ACIRunner()
                first = apply_fn(runner, row)
                before_upserts = len(runner.upserts)
                duplicate = apply_fn(runner, row)

                self.assertIs(duplicate, first)
                self.assertEqual(len(runner.upserts), before_upserts)

    def test_aci_delete_helpers_resolve_expected_identity(self):
        cases = [
            (
                "fabric",
                apply_netbox_cisco_aci_acifabric,
                delete_netbox_cisco_aci_acifabric,
                {"name": "fabric-a", "fabric_id": 1},
                {"name": "fabric-a"},
                "ACIFabric",
            ),
            (
                "pod",
                apply_netbox_cisco_aci_acipod,
                delete_netbox_cisco_aci_acipod,
                {
                    "fabric_name": "fabric-a",
                    "pod_id": "1",
                    "name": "pod-1",
                },
                {
                    "fabric_name": "fabric-a",
                    "pod_id": "1",
                },
                "ACIPod",
            ),
            (
                "tenant",
                apply_netbox_cisco_aci_acitenant,
                delete_netbox_cisco_aci_acitenant,
                {"fabric_name": "fabric-a", "name": "tenant-a"},
                {"fabric_name": "fabric-a", "name": "tenant-a"},
                "ACITenant",
            ),
            (
                "vrf",
                apply_netbox_cisco_aci_acivrf,
                delete_netbox_cisco_aci_acivrf,
                {"fabric_name": "fabric-a", "tenant_name": "tenant-a", "name": "vrf-a"},
                {"fabric_name": "fabric-a", "tenant_name": "tenant-a", "name": "vrf-a"},
                "ACIVRF",
            ),
            (
                "bridge-domain",
                apply_netbox_cisco_aci_acibridgedomain,
                delete_netbox_cisco_aci_acibridgedomain,
                {
                    "fabric_name": "fabric-a",
                    "tenant_name": "tenant-a",
                    "vrf_tenant_name": "tenant-a",
                    "vrf_name": "vrf-a",
                    "name": "bd-a",
                },
                {
                    "fabric_name": "fabric-a",
                    "tenant_name": "tenant-a",
                    "vrf_tenant_name": "tenant-a",
                    "vrf_name": "vrf-a",
                    "name": "bd-a",
                },
                "ACIBridgeDomain",
            ),
            (
                "filter",
                apply_netbox_cisco_aci_acifilter,
                delete_netbox_cisco_aci_acifilter,
                {
                    "fabric_name": "fabric-a",
                    "tenant_name": "tenant-a",
                    "name": "filter-a",
                },
                {
                    "fabric_name": "fabric-a",
                    "tenant_name": "tenant-a",
                    "name": "filter-a",
                },
                "ACIFilter",
            ),
            (
                "l3out",
                apply_netbox_cisco_aci_acil3out,
                delete_netbox_cisco_aci_acil3out,
                {
                    "fabric_name": "fabric-a",
                    "tenant_name": "tenant-a",
                    "vrf_tenant_name": "tenant-a",
                    "vrf_name": "vrf-a",
                    "name": "l3out-a",
                },
                {
                    "fabric_name": "fabric-a",
                    "tenant_name": "tenant-a",
                    "vrf_tenant_name": "tenant-a",
                    "vrf_name": "vrf-a",
                    "name": "l3out-a",
                },
                "ACIL3Out",
            ),
            (
                "node",
                apply_netbox_cisco_aci_acinode,
                delete_netbox_cisco_aci_acinode,
                {
                    "fabric_name": "fabric-a",
                    "pod_name": "pod-1",
                    "pod_id": "1",
                    "node_id": "101",
                    "name": "leaf-101",
                    "role": "leaf",
                    "node_type": "physical",
                    "serial_number": "SERIAL1",
                    "pod_tep_pool": "10.0.0.1",
                    "firmware_version": "",
                    "node_object_name": "leaf-101",
                    "description": "Forward observed ACI node.",
                },
                {
                    "fabric_name": "fabric-a",
                    "pod_id": "1",
                    "node_id": "101",
                },
                "ACINode",
            ),
        ]

        for label, create_fn, delete_fn, create_row, delete_row, model_name in cases:
            with self.subTest(label=label):
                runner = _ACIRunner()
                if label == "node":
                    device = SimpleNamespace(pk=101, name="leaf-101")
                    runner.devices[device.name] = device
                create_fn(runner, create_row)
                deleted = delete_fn(runner, delete_row)

                self.assertTrue(deleted)
                self.assertEqual(runner.deletes[-1]["model"], runner.models[model_name])
                if label == "fabric":
                    expected_lookup = [{"name": "fabric-a"}]
                elif label == "tenant":
                    fabric = runner.lookups[
                        (runner.models["ACIFabric"], ("name", "fabric-a"))
                    ]
                    expected_lookup = [{"aci_fabric": fabric, "name": "tenant-a"}]
                elif label == "vrf":
                    tenant = runner.lookups[
                        (
                            runner.models["ACITenant"],
                            (
                                "aci_fabric",
                                runner.lookups[
                                    (runner.models["ACIFabric"], ("name", "fabric-a"))
                                ].pk,
                                "name",
                                "tenant-a",
                            ),
                        )
                    ]
                    expected_lookup = [{"aci_tenant": tenant, "name": "vrf-a"}]
                elif label == "bridge-domain":
                    tenant = runner.lookups[
                        (
                            runner.models["ACITenant"],
                            (
                                "aci_fabric",
                                runner.lookups[
                                    (runner.models["ACIFabric"], ("name", "fabric-a"))
                                ].pk,
                                "name",
                                "tenant-a",
                            ),
                        )
                    ]
                    expected_lookup = [{"aci_tenant": tenant, "name": "bd-a"}]
                elif label == "filter":
                    tenant = runner.lookups[
                        (
                            runner.models["ACITenant"],
                            (
                                "aci_fabric",
                                runner.lookups[
                                    (runner.models["ACIFabric"], ("name", "fabric-a"))
                                ].pk,
                                "name",
                                "tenant-a",
                            ),
                        )
                    ]
                    expected_lookup = [{"aci_tenant": tenant, "name": "filter-a"}]
                elif label == "l3out":
                    tenant = runner.lookups[
                        (
                            runner.models["ACITenant"],
                            (
                                "aci_fabric",
                                runner.lookups[
                                    (runner.models["ACIFabric"], ("name", "fabric-a"))
                                ].pk,
                                "name",
                                "tenant-a",
                            ),
                        )
                    ]
                    expected_lookup = [{"aci_tenant": tenant, "name": "l3out-a"}]
                elif label == "pod":
                    fabric = runner.lookups[
                        (runner.models["ACIFabric"], ("name", "fabric-a"))
                    ]
                    expected_lookup = [{"aci_fabric": fabric, "pod_id": 1}]
                elif label == "node":
                    pod = runner.lookups[
                        (
                            runner.models["ACIPod"],
                            (
                                "aci_fabric",
                                runner.lookups[
                                    (runner.models["ACIFabric"], ("name", "fabric-a"))
                                ].pk,
                                "pod_id",
                                1,
                            ),
                        )
                    ]
                    expected_lookup = [{"aci_pod": pod, "node_id": 101}]
                else:
                    expected_lookup = None
                self.assertEqual(runner.deletes[-1]["lookups"], expected_lookup)

    def test_delete_acinode_skips_malformed_node_id(self):
        runner = _ACIRunner()
        fabric = SimpleNamespace(pk=1, name="fabric-a")
        pod = SimpleNamespace(pk=2, aci_fabric=fabric, pod_id=1)
        runner.lookups[
            (
                runner.models["ACIFabric"],
                ("name", "fabric-a"),
            )
        ] = fabric
        runner.lookups[
            (
                runner.models["ACIPod"],
                ("aci_fabric", fabric.pk, "pod_id", 1),
            )
        ] = pod

        result = delete_netbox_cisco_aci_acinode(
            runner,
            {
                "fabric_name": "fabric-a",
                "pod_id": "1",
                "node_id": "not-an-int",
            },
        )

        self.assertFalse(result)
        self.assertEqual(runner.deletes, [])


class TenantPolicyAdapterTest(TestCase):
    """EPGs, application profiles, contracts, subjects and filter entries."""

    def _bridge_domain(self, runner, name="BD-WEB"):
        return apply_netbox_cisco_aci_acibridgedomain(
            runner,
            {
                "fabric_name": "fabric-a",
                "tenant_name": "TN-A",
                "vrf_tenant_name": "TN-A",
                "vrf_name": "VRF-A",
                "name": name,
                "unicast_routing_enabled": True,
                "arp_flooding_enabled": False,
                "limit_ip_learn_to_subnets": True,
                "l2_unknown_unicast": "proxy",
                "l3_unknown_multicast": "flood",
                "multi_destination_flooding": "bd-flood",
                "mac_address": "00:22:BD:F8:19:FF",
                "description": "",
            },
        )

    def _epg_row(self, **extra):
        row = {
            "fabric_name": "fabric-a",
            "tenant_name": "TN-A",
            "app_profile_name": "AP-A",
            "name": "EPG-WEB",
            "bridge_domain_name": "BD-WEB",
            "admin_shutdown": False,
            "is_useg": False,
            "intra_epg_isolation": True,
            "preferred_group_member": True,
            "qos_class": "level3",
            "description": "",
        }
        row.update(extra)
        return row

    def test_an_endpoint_group_creates_its_tenant_and_application_profile(self):
        runner = _ACIRunner()
        self._bridge_domain(runner)
        apply_netbox_cisco_aci_aciendpointgroup(runner, self._epg_row())

        self.assertEqual(
            [call["model_string"] for call in runner.upserts][-2:],
            ["netbox_cisco_aci.aciappprofile", "netbox_cisco_aci.aciendpointgroup"],
        )
        values = runner.upserts[-1]["values"]
        self.assertEqual(values["name"], "EPG-WEB")
        self.assertTrue(values["intra_epg_isolation"])
        self.assertTrue(values["preferred_group_member"])
        self.assertEqual(values["qos_class"], "level3")
        self.assertEqual(
            runner.upserts[-1]["coalesce_sets"], [("aci_app_profile", "name")]
        )

    def test_an_unknown_qos_class_falls_back_to_unspecified(self):
        runner = _ACIRunner()
        self._bridge_domain(runner)
        apply_netbox_cisco_aci_aciendpointgroup(runner, self._epg_row(qos_class="gold"))
        self.assertEqual(runner.upserts[-1]["values"]["qos_class"], "unspecified")

    def test_an_endpoint_group_without_its_bridge_domain_is_skipped_not_failed(self):
        # The plugin requires the bridge domain; it is resolved, never created
        # here. 4,799 merge failures on the first acceptance run is what a
        # silent None produced.
        runner = _ACIRunner()
        outcome = apply_netbox_cisco_aci_aciendpointgroup(
            runner, self._epg_row(bridge_domain_name="BD-MISSING")
        )
        self.assertIs(outcome, False)
        self.assertNotIn(
            "netbox_cisco_aci.acibridgedomain",
            [call["model_string"] for call in runner.upserts],
        )
        self.assertNotIn(
            "netbox_cisco_aci.aciendpointgroup",
            [call["model_string"] for call in runner.upserts],
        )
        self.assertEqual(
            [w["reason"] for w in runner.skip_warnings],
            ["aci-epg-bridge-domain-missing"],
        )

    def test_an_endpoint_group_links_an_imported_bridge_domain(self):
        runner = _ACIRunner()
        bd = self._bridge_domain(runner)
        entry = apply_netbox_cisco_aci_aciendpointgroup(runner, self._epg_row())
        self.assertIs(runner.upserts[-1]["values"]["aci_bridge_domain"], bd)
        self.assertEqual(entry.name, "EPG-WEB")

    def test_a_contract_maps_scope_and_dscp(self):
        runner = _ACIRunner()
        apply_netbox_cisco_aci_acicontract(
            runner,
            {
                "fabric_name": "fabric-a",
                "tenant_name": "TN-A",
                "name": "CT-WEB",
                "scope": "context",
                "qos_class": "unspecified",
                "target_dscp": "unspecified",
                "description": "",
            },
        )
        values = runner.upserts[-1]["values"]
        self.assertEqual(values["scope"], "context")
        self.assertEqual(values["target_dscp"], "")
        self.assertEqual(runner.upserts[-1]["coalesce_sets"], [("aci_tenant", "name")])

    def test_a_subject_hangs_off_its_contract(self):
        runner = _ACIRunner()
        apply_netbox_cisco_aci_acisubject(
            runner,
            {
                "fabric_name": "fabric-a",
                "tenant_name": "TN-A",
                "contract_name": "CT-WEB",
                "name": "SUBJ-HTTP",
                "reverse_filter_ports": True,
                "qos_class": "unspecified",
                "target_dscp": "CS0",
                "description": "",
            },
        )
        self.assertEqual(
            [call["model_string"] for call in runner.upserts][-2:],
            ["netbox_cisco_aci.acicontract", "netbox_cisco_aci.acisubject"],
        )
        values = runner.upserts[-1]["values"]
        self.assertTrue(values["apply_both_directions"])
        self.assertTrue(values["reverse_filter_ports"])
        self.assertEqual(values["target_dscp"], "CS0")

    def test_a_filter_entry_turns_apic_ports_into_numbers(self):
        runner = _ACIRunner()
        apply_netbox_cisco_aci_acifilterentry(
            runner,
            {
                "fabric_name": "fabric-a",
                "tenant_name": "TN-A",
                "filter_name": "FT-HTTP",
                "name": "e-http",
                "ether_type": "ip",
                "ip_protocol": "tcp",
                "source_port_from": "unspecified",
                "source_port_to": "unspecified",
                "destination_port_from": "http",
                "destination_port_to": "https",
                "tcp_rules": "",
                "match_only_fragments": False,
                "arp_opcode": "unspecified",
                "stateful": True,
                "description": "",
            },
        )
        values = runner.upserts[-1]["values"]
        self.assertEqual(
            (values["source_port_from"], values["source_port_to"]), (None, None)
        )
        self.assertEqual(
            (values["destination_port_from"], values["destination_port_to"]), (80, 443)
        )
        self.assertEqual(values["arp_opcode"], "")
        self.assertTrue(values["stateful"])
        self.assertEqual(runner.upserts[-1]["coalesce_sets"], [("aci_filter", "name")])

    def test_previews_classify_from_the_leaf_upsert(self):
        runner = _ACIRunner()
        runner.last_upsert_would_change = False
        outcome = apply_netbox_cisco_aci_aciappprofile(
            runner,
            {"fabric_name": "fabric-a", "tenant_name": "TN-A", "name": "AP-A"},
            preview=True,
        )
        self.assertEqual(outcome, "unchanged")


class SuppressAciDeletesTest(TestCase):
    """ACI inventory must not be auto-pruned when an APIC fails collection."""

    def _sync(self, **params):
        return SimpleNamespace(parameters=params)

    def test_aci_deletes_suppressed_by_default(self):
        from forward_netbox.utilities.delete_policy import (
            should_suppress_aci_deletes,
        )

        sync = self._sync()
        self.assertTrue(
            should_suppress_aci_deletes(sync, "netbox_cisco_aci.acibridgedomain")
        )
        self.assertTrue(should_suppress_aci_deletes(sync, "netbox_cisco_aci.acil3out"))

    def test_aci_deletes_applied_when_opted_in(self):
        from forward_netbox.utilities.delete_policy import (
            should_suppress_aci_deletes,
        )

        sync = self._sync(aci_allow_deletes=True)
        self.assertFalse(
            should_suppress_aci_deletes(sync, "netbox_cisco_aci.acibridgedomain")
        )

    def test_non_aci_models_never_suppressed(self):
        from forward_netbox.utilities.delete_policy import (
            should_suppress_aci_deletes,
        )

        sync = self._sync()
        self.assertFalse(should_suppress_aci_deletes(sync, "dcim.interface"))
        self.assertFalse(should_suppress_aci_deletes(sync, "ipam.fhrpgroup"))
