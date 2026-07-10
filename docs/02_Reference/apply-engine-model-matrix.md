# Apply Engine Model Matrix

This matrix is the architecture contract for which models can run on the
opt-in `bulk_orm` apply engine versus models that must remain on the adapter
path.

Source of truth:

- `forward_netbox/utilities/apply_engine.py`
- `BULK_ORM_ENABLED_MODELS`
- `ADAPTER_REQUIRED_MODELS`
- `ADAPTER_MODEL_BLOCKERS`

## Bulk ORM Safe Set

These models are parity-tested for the current `bulk_orm` path:

- `dcim.site`
- `dcim.manufacturer`
- `dcim.devicerole`
- `dcim.platform`
- `dcim.devicetype`
- `dcim.device`
- `dcim.macaddress`
- `dcim.virtualchassis`
- `dcim.interface`
- `ipam.vlan`
- `ipam.vrf`
- `ipam.ipaddress`
- `ipam.prefix`

## Adapter-Required Set

These models remain adapter-required with explicit blocker codes:

- `dcim.cable` | `relationship_identity_directionality`
- `dcim.inventoryitem` | `dependency_resolution`
- `dcim.module` | `dependency_resolution`
- `extras.taggeditem` | `generic_foreign_key_relations`
- `ipam.fhrpgroup` | `generic_foreign_key_relations`
- `netbox_cisco_aci.acifabric` | `plugin_model_dependencies`
- `netbox_cisco_aci.acinode` | `plugin_model_dependencies`
- `netbox_cisco_aci.acipod` | `plugin_model_dependencies`
- `netbox_cisco_aci.aciappprofile` | `plugin_model_dependencies`
- `netbox_cisco_aci.acibridgedomain` | `plugin_model_dependencies`
- `netbox_cisco_aci.acicontract` | `plugin_model_dependencies`
- `netbox_cisco_aci.aciendpointgroup` | `plugin_model_dependencies`
- `netbox_cisco_aci.acifilter` | `plugin_model_dependencies`
- `netbox_cisco_aci.acil3out` | `plugin_model_dependencies`
- `netbox_cisco_aci.acistaticportbinding` | `plugin_model_dependencies`
- `netbox_cisco_aci.acitenant` | `plugin_model_dependencies`
- `netbox_cisco_aci.acivrf` | `plugin_model_dependencies`
- `netbox_peering_manager.peeringsession` | `plugin_model_dependencies`
- `netbox_dlm.cve` | `plugin_model_dependencies`
- `netbox_dlm.devicesoftware` | `plugin_model_dependencies`
- `netbox_dlm.hardwarenotice` | `plugin_model_dependencies`
- `netbox_dlm.softwareversion` | `plugin_model_dependencies`
- `netbox_dlm.vulnerability` | `plugin_model_dependencies`
- `netbox_routing.bgpaddressfamily` | `plugin_model_dependencies`
- `netbox_routing.bgppeer` | `plugin_model_dependencies`
- `netbox_routing.bgppeeraddressfamily` | `plugin_model_dependencies`
- `netbox_routing.ospfarea` | `plugin_model_dependencies`
- `netbox_routing.ospfinstance` | `plugin_model_dependencies`
- `netbox_routing.ospfinterface` | `plugin_model_dependencies`

## Drift Guard

The regression test `scripts/tests/test_apply_engine_model_matrix.py` validates
this file against the live constants in `apply_engine.py` so docs and behavior
cannot silently diverge.

`forward_architecture_audit --fail-on-gap` is the strict runtime gate. It also
verifies:

- no model falls back to `adapter_default_unclassified_model` in either default
  or bulk-enabled eligibility decisions
- all supported models have a valid shard fetch contract coverage record
