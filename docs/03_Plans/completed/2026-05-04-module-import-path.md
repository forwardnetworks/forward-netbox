## Goal

Add a native NetBox `dcim.module` path for bay-aware hardware such as chassis blades, while keeping the existing inventory-item path as the default fallback for generic components.

## Constraints

- Do not change the current inventory-item behavior by default.
- Keep the module path opt-in so it does not run on every sync unless explicitly enabled.
- Use NetBox-native models and lookups; do not invent a parallel module storage format.
- Keep the change compatible with the current branch-planning and query-registry flow.

## Touched Surfaces

- `forward_netbox/choices.py`
- `forward_netbox/forms.py`
- `forward_netbox/models.py`
- `forward_netbox/management/commands/forward_smoke_sync.py`
- `forward_netbox/utilities/branch_budget.py`
- `forward_netbox/utilities/query_registry.py`
- `forward_netbox/utilities/sync.py`
- `forward_netbox/utilities/sync_contracts.py`
- `forward_netbox/queries/forward_modules.nqe`
- `forward_netbox/tests/test_forms.py`
- `forward_netbox/tests/test_models.py`
- `forward_netbox/tests/test_query_registry.py`
- `forward_netbox/tests/test_sync.py`
- `docs/02_Reference/model-mapping-matrix.md`
- `docs/02_Reference/built-in-nqe-maps.md`

## Approach

Add `dcim.module` as an optional NetBox surface with a disabled built-in query seeded by default. Keep the query device-first and component-oriented, then resolve module types and module bays in Python. Treat module bays as pre-existing NetBox topology, not something the sync layer invents.

## Validation

- `invoke harness-check`
- `python -m compileall forward_netbox/tests/test_sync.py forward_netbox/utilities/sync.py forward_netbox/utilities/query_registry.py forward_netbox/choices.py forward_netbox/forms.py forward_netbox/models.py`
- Focused Django test run:
  - `forward_netbox.tests.test_query_registry`
  - `forward_netbox.tests.test_forms`
  - `forward_netbox.tests.test_models`
  - `forward_netbox.tests.test_sync`
- `invoke ci`

## Rollback

Remove `dcim.module` from the supported model list and optional query maps, delete the module query file and adapter helpers, and restore the default model-selection behavior if the path causes sync failures.

## Decision Log

- Chosen: keep `dcim.module` optional and disabled by default so the inventory-item fallback remains the default path.
- Chosen: resolve module bays by lookup only. NetBox expects module bays to come from device-type templates or manual setup, so the plugin should not synthesize them.
- Chosen: keep module sharding device-keyed so planning stays consistent with other device-scoped models.
- Chosen: seed the built-in module query disabled so enabling it is an explicit operator choice.
