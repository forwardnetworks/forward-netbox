# Module Readiness Workflow

## Goal

Provide a low-friction operator path for optional `dcim.module` imports when the Forward module query identifies chassis modules but NetBox does not yet have matching module bays.

## Constraints

- Keep NQE as the module classifier and normalizer.
- Keep NetBox as the source of module-bay topology.
- Do not create module bays through raw SQL or other non-native side channels.
- Do not commit customer identifiers, network IDs, snapshot IDs, or live output artifacts.

## Touched Surfaces

- `forward_netbox/utilities/module_readiness.py`
- `forward_netbox/management/commands/forward_module_readiness.py`
- `forward_netbox/utilities/query_registry.py`
- `forward_netbox/utilities/sync.py`
- `forward_netbox/tests/`
- `docs/01_User_Guide/usage.md`
- `docs/02_Reference/built-in-nqe-maps.md`

## Approach

Add a read-only readiness command that runs the same module NQE map as the sync, compares `(device, module_bay)` rows to existing NetBox module bays, and writes a native NetBox module-bay import CSV for missing bays. Create missing module bays through native NetBox model operations when `dcim.module` is enabled so the low-friction path stays in the normal Branching diff.

Set a conservative default `dcim.module` branch density because missing module bays can add one extra NetBox change per module row.

## Validation

- Focused Django tests passed:
  - `forward_netbox.tests.test_module_readiness`
  - `forward_netbox.tests.test_query_registry`
  - `forward_netbox.tests.test_sync.ForwardSyncRunnerTest.test_apply_dcim_module_creates_missing_module_bay_natively`
  - `forward_netbox.tests.test_sync.ForwardBranchBudgetPlanTest.test_effective_row_budget_uses_module_default_density`
- `invoke test` passed.
- The readiness command completed against the live smoke dataset in about 11 seconds and reported 3,864 module candidates, 0 missing devices, and 3,864 missing module bays. Generated CSV/JSON output stayed under `.forward-netbox-reports/`, which is ignored.
- Clean NetBox `v4.5.9` validation proved native ORM module-bay creation works; the old test-only raw SQL helper was removed.
- `invoke harness-check` passed.
- `python scripts/check_sensitive_content.py .gitignore docs forward_netbox tasks.py README.md pyproject.toml` passed.
- Customer/private identifier grep over docs and code returned no hits.
- `invoke lint` passed.
- `invoke docs` passed.
- `invoke ci` passed on NetBox `v4.5.9`.

## Rollback

Remove the readiness command/helper and restore lookup-only module-bay behavior. The optional module import remains disabled by default and can be removed independently if needed.

## Decision Log

- Chosen: per-model native auto-create for `dcim.module` module bays, because clean NetBox `v4.5.9` supports ORM creation and those changes remain reviewable in Branching.
- Chosen: retain the readiness CSV as a preview/pre-stage operator path.
- Rejected: global auto-create dependencies | too broad and unsafe for sites, devices, interfaces, cables, and IP objects.
- Rejected: raw SQL module-bay creation | not NetBox-native and not safe for Branching/change logging.
