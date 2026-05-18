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
- `ipam.vlan`
- `ipam.vrf`

## Adapter-Required Set

These models remain adapter-required with explicit blocker codes:

- `dcim.cable` | `relationship_identity_directionality`
- `dcim.device` | `dependency_resolution`
- `dcim.interface` | `relationship_side_effects`
- `dcim.inventoryitem` | `dependency_resolution`
- `dcim.macaddress` | `relationship_side_effects`
- `dcim.module` | `dependency_resolution`
- `dcim.virtualchassis` | `optional_contract_guarding`
- `extras.taggeditem` | `generic_foreign_key_relations`
- `ipam.ipaddress` | `ipam_parent_prefix_semantics`
- `ipam.prefix` | `ipam_hierarchy_semantics`
- `netbox_peering_manager.peeringsession` | `plugin_model_dependencies`
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
