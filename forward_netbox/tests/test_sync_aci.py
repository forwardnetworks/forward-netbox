from types import SimpleNamespace
from unittest import TestCase

from forward_netbox.exceptions import ForwardDependencySkipError
from forward_netbox.utilities.sync_aci import apply_netbox_cisco_aci_aciendpointgroup
from forward_netbox.utilities.sync_aci import apply_netbox_cisco_aci_acifilter
from forward_netbox.utilities.sync_aci import apply_netbox_cisco_aci_acinode
from forward_netbox.utilities.sync_aci import (
    apply_netbox_cisco_aci_acistaticportbinding,
)
from forward_netbox.utilities.sync_aci import apply_netbox_cisco_aci_acivrf
from forward_netbox.utilities.sync_aci import delete_netbox_cisco_aci_acinode
from forward_netbox.utilities.sync_aci import delete_netbox_cisco_aci_acipod


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
            "ACIFilter": _fake_model(
                "netbox_cisco_aci.acifilter",
                ("aci_tenant", "name", "description"),
            ),
            "ACIStaticPortBinding": _fake_model(
                "netbox_cisco_aci.acistaticportbinding",
                (
                    "aci_endpoint_group",
                    "dcim_interface",
                    "encap_vlan",
                    "binding_type",
                    "mode",
                    "deployment_immediacy",
                    "description",
                ),
            ),
        }
        self.upserts = []
        self.deletes = []
        self.lookups = {}
        self.devices = {}
        self.interfaces = {}

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
            "netbox_cisco_aci.aciappprofile",
            "netbox_cisco_aci.acifilter",
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
        elif model_string == "netbox_cisco_aci.aciendpointgroup":
            self.lookups[
                (
                    model,
                    (
                        "aci_app_profile",
                        self._parent_key(values["aci_app_profile"]),
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

    def _lookup_interface(self, device, interface_name):
        if device is None:
            return None
        return self.interfaces.get((device.pk, interface_name))

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

    def test_apply_static_port_binding_accepts_nqe_contract_device_interface_keys(self):
        runner = _ACIRunner()
        apply_netbox_cisco_aci_aciendpointgroup(
            runner,
            {
                "fabric_name": "fabric-a",
                "tenant_name": "tenant-a",
                "app_profile_name": "app-a",
                "bridge_domain_name": "bd-a",
                "vrf_name": "vrf-a",
                "name": "epg-a",
            },
        )
        device = SimpleNamespace(pk=101, name="leaf-101")
        interface = SimpleNamespace(pk=202, name="Eth1/1")
        runner.devices[device.name] = device
        runner.interfaces[(device.pk, interface.name)] = interface

        apply_netbox_cisco_aci_acistaticportbinding(
            runner,
            {
                "fabric_name": "fabric-a",
                "tenant_name": "tenant-a",
                "app_profile_name": "app-a",
                "endpoint_group_name": "epg-a",
                "device_name": "leaf-101",
                "interface_name": "Eth1/1",
                "encap_vlan": "100",
                "deployment_immediacy": "lazy",
                "mode": "regular",
                "binding_type": "regular",
            },
        )

        binding_call = runner.upserts[-1]
        self.assertEqual(
            binding_call["model_string"],
            "netbox_cisco_aci.acistaticportbinding",
        )
        self.assertEqual(binding_call["values"]["dcim_interface"], interface)
        self.assertEqual(binding_call["values"]["encap_vlan"], 100)
        self.assertEqual(
            binding_call["coalesce_sets"],
            [("aci_endpoint_group", "dcim_interface", "encap_vlan")],
        )

    def test_apply_static_port_binding_reports_missing_endpoint_group_dependency(self):
        runner = _ACIRunner()

        with self.assertRaises(ForwardDependencySkipError):
            apply_netbox_cisco_aci_acistaticportbinding(
                runner,
                {
                    "fabric_name": "fabric-a",
                    "tenant_name": "tenant-a",
                    "app_profile_name": "app-a",
                    "endpoint_group_name": "epg-a",
                    "device_name": "leaf-101",
                    "interface_name": "Eth1/1",
                    "encap_vlan": "100",
                },
            )

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
