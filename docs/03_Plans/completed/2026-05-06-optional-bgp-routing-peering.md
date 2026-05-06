# Optional BGP Routing and Peering Sync

## Goal

Add an opt-in BGP sync path that imports Forward structured BGP neighbor state into native NetBox plugin models, using `netbox-routing` for BGP peers and `netbox-peering-manager` as an optional peering metadata overlay.

## Constraints

- Keep current default behavior unchanged; BGP sync must be hidden and disabled unless `PLUGINS_CONFIG["forward_netbox"]["enable_bgp_sync"]` is true.
- Do not add hard package dependencies on optional NetBox plugins.
- Use NetBox model adapters and existing branch/diff/sharding flow, not a side-channel importer.
- Avoid BGP RIB/route import in this pass; model configured/session peer state only.
- Do not commit customer identifiers, network IDs, snapshot IDs, credentials, or customer screenshots.

## Touched Surfaces

- Production: `forward_netbox/choices.py`, `models.py`, `forms.py`, `signals.py`, `utilities/query_registry.py`, `utilities/sync_contracts.py`, `utilities/branch_budget.py`, `utilities/sync.py`, and optional smoke command model selection.
- Queries: new built-in optional NQE maps under `forward_netbox/queries/`.
- Docs: configuration and model mapping references.
- Tests: model gating, form gating, query registry, contracts, branch budget, adapter behavior where optional plugins are unavailable.

## Approach

1. Add feature-flag-aware model list helpers so BGP models are part of the known model catalog but not part of the configured UI/runtime model set by default.
2. Seed disabled optional NQE maps for `netbox_routing.bgppeer` and `netbox_peering_manager.peeringsession` when the optional plugin ContentTypes exist.
3. Add contracts and branch-density hints so BGP rows validate and shard through the same branch-native planning path as device-associated models.
4. Add sync adapters that create/reuse core NetBox ASNs, VRFs, unassigned peer IP addresses, `netbox-routing` routers/scopes/peers, and optional `netbox-peering-manager` peering sessions.
5. Fail each BGP row clearly if required optional plugin models are absent, while preserving existing row-failure isolation.
6. Update docs to describe the dependencies, feature flag, and beta scope.

## Validation

- Targeted unit tests for feature-flag gating, registry shape, contracts, sharding, and adapter failure when optional plugins are missing.
- Run relevant Django test modules locally.
- Run broader harness checks if the targeted tests are clean.

Validation completed:

- `invoke lint`
- `docker compose --project-name forward-netbox --project-directory development exec -T netbox bash -lc "cd /opt/netbox/netbox && python manage.py makemigrations forward_netbox --check --dry-run"`
- `invoke test`
- `invoke harness-check`
- `invoke check`
- `invoke docs`
- `invoke harness-test`
- `invoke scenario-test`
- `invoke ci`

## Rollback

Disable `enable_bgp_sync` to remove BGP models from the UI/runtime sync surface. To fully remove the implementation, revert the BGP model constants, adapters, contracts, query maps, and docs without changing existing non-BGP model behavior.

## Decision Log

- Rejected: `netbox-bgp` as a target model. `netbox-peering-manager` depends on `netbox-routing`, and `netbox-routing` is the more current shared model surface.
- Rejected: importing BGP routes/RIB data in this pass. The customer need is peer/session modeling, and routes would be much larger and need a separate policy.
