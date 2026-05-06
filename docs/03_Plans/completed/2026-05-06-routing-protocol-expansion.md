# Routing Protocol Expansion

## Goal

Extend the existing beta routing sync surface to import more native routing state from Forward into optional NetBox routing plugins. The first implementation pass should add BGP address-family state and OSPF state using Forward normalized protocol data, while keeping policy objects such as communities, prefix lists, and route maps out of the default path until they can be sourced reliably.

## Constraints

- Keep the feature behind `PLUGINS_CONFIG["forward_netbox"]["enable_bgp_sync"] = True`.
- Do not add a separate sync path or bypass NetBox/Branching-native model application.
- Do not commit customer identifiers, network IDs, snapshot IDs, credentials, or live row examples.
- Prefer Forward normalized protocol/RIB data over raw configuration parsing.
- OSPF process IDs in Forward can be strings, but `netbox-routing` stores `OSPFInstance.process_id` as an integer. Preserve the original label in the object name/comments when a stable integer translation is required.
- Keep newly shipped maps disabled by default.

## Touched Surfaces

- `forward_netbox/queries/`
- `forward_netbox/choices.py`
- `forward_netbox/forms.py`
- `forward_netbox/models.py`
- `forward_netbox/utilities/query_registry.py`
- `forward_netbox/utilities/sync_contracts.py`
- `forward_netbox/utilities/sync.py`
- `forward_netbox/utilities/branch_budget.py`
- Tests under `forward_netbox/tests/`
- User/reference docs under `docs/`

## Approach

1. Correct the existing BGP peer and peering-session NQE type annotations from `Int` to `Integer` so they execute on the live Forward NQE runtime.
2. Add optional maps for:
   - `netbox_routing.bgpaddressfamily`
   - `netbox_routing.bgppeeraddressfamily`
   - `netbox_routing.ospfinstance`
   - `netbox_routing.ospfarea`
   - `netbox_routing.ospfinterface`
3. For BGP address-family data, join `device.bgpRib.afiSafis[].neighbors[]` back to structured BGP neighbors and reuse the existing BGP router/scope/peer helpers.
4. For OSPF, create native instances, areas, and interface bindings from `networkInstances[].protocols[].ospf.areas[]`. Numeric process IDs are preserved. Non-numeric process IDs get a deterministic local integer while the original process label/domain are preserved in name/comments.
5. Leave BGP policy objects for a later optional config-derived layer. Communities, route maps, prefix lists, and route-policy attachments are not exposed in the current normalized BGP neighbor/RIB docs, and parsing them from raw configs needs vendor-specific contracts.

## Validation

- Run live NQE smoke queries with sanitized output.
- Run targeted Django tests for query registry, forms, model gating, contracts, and sync adapters.
- Run the synthetic local Docker sync smoke for the optional routing models.
- Run the repo harness gate before any release work.

## Rollback

Disable `enable_bgp_sync` to hide the optional routing models. To remove the implementation, revert the optional query maps, NQE files, contracts, adapter methods, docs, and tests without changing the existing core inventory models.

## Decision Log

- Rejected: importing communities/route maps/prefix lists in the same pass. The live normalized data confirms useful BGP/OSPF protocol state, but policy objects require config-derived parsing and should not be mixed with the default normalized path.
- Rejected: dropping OSPF rows with named process IDs. Live datasets can use named OSPF process labels, and silently skipping them would lose useful OSPF data.
