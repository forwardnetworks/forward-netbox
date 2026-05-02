# Inferred Interface Cable Import

## Goal

Import NetBox `dcim.cable` objects from Forward resolved interface-link evidence, including Forward inferred topology where available, when both endpoints resolve to exact NetBox device/interface names.

## Constraints

- Keep the implementation inside the existing NQE map and sync-adapter architecture.
- Do not infer or rewrite endpoint names in Python.
- Do not overwrite existing unrelated NetBox cables.
- Preserve native Branching review before merge.

## Touched Surfaces

- `forward_netbox/choices.py`
- `forward_netbox/queries/forward_inferred_interface_cables.nqe`
- `forward_netbox/utilities/query_registry.py`
- `forward_netbox/utilities/sync_contracts.py`
- `forward_netbox/utilities/branch_budget.py`
- `forward_netbox/utilities/sync.py`
- `forward_netbox/tests/test_query_registry.py`
- `forward_netbox/tests/test_sync.py`
- `README.md`
- `docs/README.md`
- `docs/02_Reference/built-in-nqe-maps.md`
- `docs/02_Reference/model-mapping-matrix.md`

## Approach

1. Add `dcim.cable` as a supported sync model.
2. Ship a built-in `Forward Inferred Interface Cables` NQE map using `interface.links`.
3. Canonicalize endpoint order in NQE so symmetric link reports do not create duplicate cable rows.
5. Add a `dcim.cable` adapter that creates a connected NetBox cable between two interfaces, reuses the same cable when already present, and fails instead of rewiring an interface already connected elsewhere.
6. Add delete support for diff-mode cable removals.

## Validation

- Query registry tests verify the shipped cable query fields and resolved-link behavior.
- Sync adapter tests verify create, reverse idempotency, conflict handling, delete, and row contract validation.
- Run targeted NetBox tests for query registry and sync behavior.

## Rollback

- Remove `dcim.cable` from the supported model list and built-in query registry.
- Revert the cable adapter and docs changes.
- Existing NetBox cable objects created by a merged run would need operator review/removal like any other Branching-merged inventory object.

## Decision Log

- Rejected: infer neighbor endpoint names in Python.
  - Reason: interface-name normalization after NQE would hide data ambiguity and could cable the wrong ports.
- Rejected: overwrite existing cables on either endpoint.
  - Reason: an existing different cable is operator-owned state and should surface as a sync issue.
