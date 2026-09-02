import unittest

from dcim.models import Device
from dcim.models import DeviceRole
from dcim.models import DeviceType
from dcim.models import Manufacturer
from dcim.models import Site
from django.apps import apps
from django.test import TestCase

from forward_netbox.utilities.drift_comparison import compare_model_rows

# Slice nine of the adapter-only drift comparison: the eight netbox-cisco-aci
# models. The one #206 never named, because the ACI maps postdate it - and the
# largest block left after slice eight, so `in_sync` was unanswerable for any
# deployment running the plugin.
#
# The audit result: every write in `sync_aci` is behind
# `runner._upsert_values_from_defaults`, which the preview already overrides,
# and every lookup only reads. No shim was needed. What WAS needed is the guard
# these tests pin hardest: a parent the preview reports absent must
# short-circuit the child to a create, because `coalesce_lookup` drops the
# `None` parent and the child would otherwise be resolved by `name` alone -
# and match a sibling under a different tenant, reading `unchanged` for a row
# the apply would create.
#
# The verdict rule is the leaf rule. Every ACI model has its own query and its
# own rows, so a parent create is the parent model's drift.

# These models only exist when the optional plugin is installed, and on
# NetBox 4.7 it cannot be: netbox-cisco-aci declares a max_version in the 4.6
# series, so NetBox refuses to start with it. The suite skips rather than
# fails - but this IS lost coverage, not a clean pass, and the 4.6 lane on
# 2.9.x is where these adapters stay exercised until that ceiling moves.
NETBOX_CISCO_ACI_INSTALLED = apps.is_installed("netbox_cisco_aci")

FABRIC = "netbox_cisco_aci.acifabric"
POD = "netbox_cisco_aci.acipod"
NODE = "netbox_cisco_aci.acinode"
TENANT = "netbox_cisco_aci.acitenant"
VRF = "netbox_cisco_aci.acivrf"
BRIDGE_DOMAIN = "netbox_cisco_aci.acibridgedomain"
FILTER = "netbox_cisco_aci.acifilter"
L3OUT = "netbox_cisco_aci.acil3out"
ALL_MODELS = (FABRIC, POD, NODE, TENANT, VRF, BRIDGE_DOMAIN, FILTER, L3OUT)


def _aci(model_name):
    from forward_netbox.utilities.sync_primitives import optional_model

    return optional_model(
        "netbox_cisco_aci", model_name, f"netbox_cisco_aci.{model_name.lower()}"
    )


def _counts(model_string, rows):
    return compare_model_rows(None, model_string, rows)


@unittest.skipUnless(NETBOX_CISCO_ACI_INSTALLED, "netbox-cisco-aci is not installed")
class AciPreviewTest(TestCase):
    """Real plugin models, one existing object per level, then a preview."""

    def setUp(self):
        from forward_netbox.utilities.sync_aci import ACI_FABRIC_DESCRIPTION
        from forward_netbox.utilities.sync_aci import ACI_POD_DESCRIPTION
        from forward_netbox.utilities.sync_aci import ACI_TENANT_DESCRIPTION
        from forward_netbox.utilities.sync_aci import ACI_VRF_DESCRIPTION

        self.fabric = _aci("ACIFabric").objects.create(
            name="FAB1", fabric_id=1, description=ACI_FABRIC_DESCRIPTION
        )
        self.pod = _aci("ACIPod").objects.create(
            aci_fabric=self.fabric,
            name="pod-1",
            pod_id=1,
            description=ACI_POD_DESCRIPTION,
        )
        self.tenant = _aci("ACITenant").objects.create(
            aci_fabric=self.fabric,
            name="TENANT-A",
            description=ACI_TENANT_DESCRIPTION,
        )
        self.vrf = _aci("ACIVRF").objects.create(
            aci_tenant=self.tenant,
            name="VRF-A",
            policy_enforcement_preference="enforced",
            policy_enforcement_direction="ingress",
            bd_enforcement_enabled=False,
            preferred_group_enabled=False,
            description=ACI_VRF_DESCRIPTION,
        )
        site = Site.objects.create(name="A Site", slug="a-site")
        mfr = Manufacturer.objects.create(name="A Mfr", slug="a-mfr")
        dtype = DeviceType.objects.create(manufacturer=mfr, model="A DT", slug="a-dt")
        role = DeviceRole.objects.create(name="A Role", slug="a-role")
        self.device = Device.objects.create(
            name="leaf-101", site=site, device_type=dtype, role=role, status="active"
        )

    # --- rows ---------------------------------------------------------------

    def _fabric_row(self, **extra):
        return {"name": "FAB1", "fabric_id": 1, **extra}

    def _pod_row(self, **extra):
        return {"fabric_name": "FAB1", "name": "pod-1", "pod_id": 1, **extra}

    def _tenant_row(self, **extra):
        return {"fabric_name": "FAB1", "name": "TENANT-A", **extra}

    def _vrf_row(self, **extra):
        return {
            "fabric_name": "FAB1",
            "tenant_name": "TENANT-A",
            "name": "VRF-A",
            **extra,
        }

    def _bd_row(self, **extra):
        return {
            "fabric_name": "FAB1",
            "tenant_name": "TENANT-A",
            "vrf_name": "VRF-A",
            "name": "BD-A",
            **extra,
        }

    def _filter_row(self, **extra):
        return {
            "fabric_name": "FAB1",
            "tenant_name": "TENANT-A",
            "name": "FILTER-A",
            **extra,
        }

    def _l3out_row(self, **extra):
        return {
            "fabric_name": "FAB1",
            "tenant_name": "TENANT-A",
            "vrf_name": "VRF-A",
            "name": "L3OUT-A",
            **extra,
        }

    def _node_row(self, **extra):
        return {
            "fabric_name": "FAB1",
            "pod_id": 1,
            "pod_name": "pod-1",
            "node_id": 101,
            "name": "leaf-101",
            "role": "leaf",
            "node_type": "physical",
            **extra,
        }

    def _existing_bd(self):
        return _aci("ACIBridgeDomain").objects.create(
            aci_tenant=self.tenant,
            aci_vrf=self.vrf,
            name="BD-A",
            unicast_routing_enabled=True,
            arp_flooding_enabled=False,
            limit_ip_learn_to_subnets=True,
            l2_unknown_unicast="proxy",
            l3_unknown_multicast="flood",
            multi_destination_flooding="bd-flood",
        )

    def _existing_node(self, **overrides):
        from django.contrib.contenttypes.models import ContentType

        values = {
            "aci_pod": self.pod,
            "name": "leaf-101",
            "node_id": 101,
            "role": "leaf",
            "node_type": "physical",
            "node_object_type": ContentType.objects.get_for_model(Device),
            "node_object_id": self.device.pk,
        }
        values.update(overrides)
        return _aci("ACINode").objects.create(**values)

    # --- the firewall --------------------------------------------------------

    def test_a_preview_writes_nothing_for_any_model(self):
        rows = {
            FABRIC: self._fabric_row(name="FAB2"),
            POD: self._pod_row(name="pod-2", pod_id=2),
            NODE: self._node_row(name="leaf-102", node_id=102),
            TENANT: self._tenant_row(name="TENANT-B"),
            VRF: self._vrf_row(name="VRF-B"),
            BRIDGE_DOMAIN: self._bd_row(),
            FILTER: self._filter_row(),
            L3OUT: self._l3out_row(),
        }
        before = {
            name: _aci(name).objects.count()
            for name in (
                "ACIFabric",
                "ACIPod",
                "ACINode",
                "ACITenant",
                "ACIVRF",
                "ACIBridgeDomain",
                "ACIFilter",
                "ACIL3Out",
            )
        }
        for model_string, row in rows.items():
            counts = _counts(model_string, [row])
            self.assertIsNotNone(counts, model_string)
            self.assertEqual(counts["creates"], 1, (model_string, counts))
        after = {name: _aci(name).objects.count() for name in before}
        self.assertEqual(after, before)

    # --- classification, one per model ---------------------------------------

    def test_an_existing_fabric_is_unchanged(self):
        self.assertEqual(_counts(FABRIC, [self._fabric_row()])["unchanged"], 1)

    def test_a_fabric_whose_description_drifted_is_an_update(self):
        counts = _counts(FABRIC, [self._fabric_row(description="edited")])
        self.assertEqual(counts["updates"], 1)

    def test_an_existing_pod_is_unchanged(self):
        self.assertEqual(_counts(POD, [self._pod_row()])["unchanged"], 1)

    def test_an_existing_tenant_is_unchanged(self):
        self.assertEqual(_counts(TENANT, [self._tenant_row()])["unchanged"], 1)

    def test_an_existing_vrf_is_unchanged(self):
        self.assertEqual(_counts(VRF, [self._vrf_row()])["unchanged"], 1)

    def test_a_vrf_whose_enforcement_drifted_is_an_update(self):
        counts = _counts(
            VRF, [self._vrf_row(policy_enforcement_preference="unenforced")]
        )
        self.assertEqual(counts["updates"], 1)

    def test_an_existing_bridge_domain_is_unchanged(self):
        self._existing_bd()
        self.assertEqual(_counts(BRIDGE_DOMAIN, [self._bd_row()])["unchanged"], 1)

    def test_a_bridge_domain_whose_flooding_drifted_is_an_update(self):
        self._existing_bd()
        counts = _counts(BRIDGE_DOMAIN, [self._bd_row(arp_flooding_enabled="true")])
        self.assertEqual(counts["updates"], 1)

    def test_an_absent_filter_is_a_create(self):
        self.assertEqual(_counts(FILTER, [self._filter_row()])["creates"], 1)

    def test_an_existing_filter_is_unchanged(self):
        _aci("ACIFilter").objects.create(aci_tenant=self.tenant, name="FILTER-A")
        self.assertEqual(_counts(FILTER, [self._filter_row()])["unchanged"], 1)

    def test_an_absent_l3out_is_a_create(self):
        self.assertEqual(_counts(L3OUT, [self._l3out_row()])["creates"], 1)

    def test_an_existing_l3out_is_unchanged(self):
        _aci("ACIL3Out").objects.create(
            aci_tenant=self.tenant,
            aci_vrf=self.vrf,
            name="L3OUT-A",
            protocol_bgp=False,
            protocol_ospf=False,
            protocol_eigrp=False,
            protocol_static=True,
        )
        self.assertEqual(_counts(L3OUT, [self._l3out_row()])["unchanged"], 1)

    def test_an_existing_node_is_unchanged(self):
        self._existing_node()
        self.assertEqual(_counts(NODE, [self._node_row()])["unchanged"], 1)

    def test_a_node_whose_serial_drifted_is_an_update(self):
        self._existing_node()
        counts = _counts(NODE, [self._node_row(serial_number="FDO1234")])
        self.assertEqual(counts["updates"], 1)

    # --- the leaf rule -------------------------------------------------------

    def test_a_parent_the_row_would_create_is_not_counted_under_the_child(self):
        """One object, one model. The pod's create is the pod model's.

        A node under a pod that does not exist yet is ONE create under
        `acinode`, not two - the pod is reported when `acipod`'s own rows are
        compared. Folding it in here is the double count the leaf rule exists
        to prevent.
        """
        counts = _counts(NODE, [self._node_row(pod_id=9, pod_name="pod-9")])
        self.assertEqual(
            counts, {"creates": 1, "updates": 0, "unchanged": 0, "rejected": 0}
        )

    def test_a_second_observation_of_a_node_in_one_run_is_unchanged(self):
        # The apply dedups repeated observations and writes nothing for the
        # second; the preview says the same.
        self._existing_node()
        counts = _counts(NODE, [self._node_row(), self._node_row()])
        self.assertEqual(counts["unchanged"], 2)
        self.assertEqual(counts["updates"], 0)

    # --- the absent-parent guard ---------------------------------------------

    def test_a_vrf_under_an_absent_tenant_is_a_create_not_a_sibling_match(self):
        """THE defect this slice guards against.

        `VRF-A` exists under `TENANT-A`. A row for `VRF-A` under `TENANT-B`,
        which does not exist, must read `creates`. Without the guard the
        preview's `None` tenant is dropped from the lookup and the row resolves
        to TENANT-A's VRF-A by name - `unchanged` for a row the apply would
        create, which is the confident zero this feature exists to prevent.
        """
        counts = _counts(VRF, [self._vrf_row(tenant_name="TENANT-B")])
        self.assertEqual(counts["creates"], 1)
        self.assertEqual(counts["unchanged"], 0)

    def test_a_bridge_domain_under_an_absent_vrf_is_a_create(self):
        self._existing_bd()
        counts = _counts(BRIDGE_DOMAIN, [self._bd_row(vrf_name="VRF-NEW")])
        self.assertEqual(counts["creates"], 1)
        self.assertEqual(counts["unchanged"], 0)

    def test_a_tenant_under_an_absent_fabric_is_a_create(self):
        counts = _counts(TENANT, [self._tenant_row(fabric_name="FAB-NEW")])
        self.assertEqual(counts["creates"], 1)

    def test_the_guard_is_inert_for_the_real_apply(self):
        # `_parent_absent` only ever sees `None` from the preview shim. The real
        # ensure creates its parent and hands back an object, so the guard
        # cannot change what the apply does.
        from forward_netbox.utilities.sync_aci import _parent_absent

        self.assertFalse(_parent_absent(self.fabric, self.tenant))
        self.assertTrue(_parent_absent(self.fabric, None))

    # --- rejection -----------------------------------------------------------

    def test_a_row_with_an_unparseable_node_id_is_rejected_not_drift(self):
        counts = _counts(NODE, [self._node_row(node_id="leaf")])
        self.assertEqual(counts["rejected"], 1)
        self.assertEqual(counts["creates"], 0)

    def test_every_aci_model_is_registered(self):
        for model_string in ALL_MODELS:
            self.assertIsNotNone(_counts(model_string, []), model_string)
