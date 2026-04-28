# Optional TurboBulk Execution Backend

## Goal

Prepare a future implementation path for using NetBox Labs TurboBulk as an optional execution backend for very large Forward sync workloads while preserving the current NetBox-native UI workflow, Branching review model, branch budgeting, validation/drift gates, and REST/Django adapter path as the default behavior.

This branch does not implement or enable TurboBulk writes.

Reference: https://github.com/netboxlabs/netbox-turbobulk-public

Current branch scope: implement the internal capability probe and execution-backend seam only. TurboBulk writes remain disabled until a later branch proves load-file generation, model-specific conflict handling, and live TurboBulk behavior.

## Constraints

- TurboBulk requires NetBox Cloud or NetBox Enterprise with TurboBulk enabled; community and self-managed installs without the plugin must keep working unchanged.
- TurboBulk write APIs require server-side `enable_writes: True`; the plugin must detect capability and fail back or fail clearly before starting a large run.
- The existing Forward sync UI/API path remains the only operator workflow. Do not add a separate large-import workflow.
- Branching remains native: create branches through NetBox Branching, wait for READY, load rows into those branches, preserve review/merge behavior, and keep branch budgets configurable.
- TurboBulk accepts branch names per request, not a multi-branch batch request. The current branch planner still needs to shard work before execution.
- TurboBulk does not create branches; branch lifecycle remains owned by this plugin.
- Parent objects must be loaded before child objects inside a branch because foreign keys must resolve in the same branch schema.
- Upserts require database conflict fields or named constraints. Existing coalesce rules must be translated deliberately instead of assuming NQE identity fields match TurboBulk conflict semantics.
- Foreign keys must use `_id` columns. The existing model adapters currently resolve natural keys and object identity through Django/NetBox logic, so a TurboBulk backend must produce ID-based load rows.
- Full Django validation is available through TurboBulk validation modes, but validation/error payloads must still map into `ForwardValidationRun` and `ForwardIngestionIssue`.
- Customer identifiers, network IDs, snapshot IDs, credentials, and exported data files must not be committed.

## Touched Surfaces

- `forward_netbox/models.py`: persisted sync settings only if an operator-visible backend mode is eventually added.
- `forward_netbox/forms.py`, `forward_netbox/tables.py`, templates: only if backend capability/status needs to be shown.
- `forward_netbox/utilities/multi_branch.py`: branch lifecycle, per-branch execution handoff, retry, and merge orchestration.
- `forward_netbox/utilities/branch_budget.py`: existing shard sizing remains authoritative.
- `forward_netbox/utilities/sync.py`: current Django adapter path remains default; any TurboBulk work should move behind a new execution boundary rather than expanding this module.
- `forward_netbox/utilities/validation.py`: map TurboBulk dry-run/full-validation failures into current blocking policy and issue reporting.
- New execution boundary, likely under `forward_netbox/utilities/execution/`, after tests pin current behavior.
- `forward_netbox/utilities/turbobulk.py`: capability probing for the public TurboBulk API surface.
- Tests: unit tests for capability detection, row materialization, branch handoff, validation mapping, and fallback behavior; scenario tests for large model planning.
- Docs: configuration, architecture, model mapping, validation matrix, and operator guidance.

## Approach

1. Add a read-only capability probe.
   - Probe `/api/plugins/turbobulk/models/` with the NetBox API token.
   - Treat `404` as unavailable, `401/403` as unavailable or insufficient permission, and explicit write-disabled errors as a clear preflight failure when TurboBulk is selected.
   - Cache capability per run only; do not persist server details beyond normal run metadata.
   - Status: initial client/probe implemented and covered by unit tests.

2. Introduce an execution backend boundary before adding TurboBulk writes.
   - Keep the existing Django/NetBox adapter executor as the default backend.
   - Define a narrow interface around `validate`, `apply_upserts`, `apply_deletes`, and `summarize_result`.
   - `multi_branch.py` should continue to own branch planning, branch readiness, retries, and merge handoff.
   - Status: initial `NativeBranchExecutionBackend` is wired as the default; `TurboBulkBranchExecutionBackend` is intentionally a placeholder.

3. Materialize TurboBulk load files from validated plan rows.
   - Prefer JSONL.gz first because it avoids a required Parquet dependency.
   - Keep Parquet as a possible later optimization for very large models if dependency and packaging impact are acceptable.
   - Convert resolved NetBox references to `_id` fields before load.
   - Keep natural-key/coalesce validation in the current contract layer so bad rows fail before branch creation where possible.

4. Start with one low-risk parent model proof point.
   - Candidate models: `dcim.manufacturer`, `dcim.devicerole`, or `dcim.site`.
   - These have simpler dependencies and make it easier to prove capability detection, dry-run validation, branch targeting, and issue mapping.
   - Defer `dcim.device`, `dcim.interface`, IP addresses, cables, and inventory items until parent behavior is proven.

5. Preserve branch review and budget behavior.
   - Use the same branch budget planner to keep each branch under the configured change count target.
   - Submit one TurboBulk request per branch/model shard using the `branch` parameter.
   - Do not bypass Branching review by writing directly to main for large syncs.
   - If auto-merge is enabled, continue using the existing native branch merge path after successful branch loads.

6. Add validation-mode policy.
   - Use TurboBulk dry-run before write when available.
   - Default to a conservative validation mode for complex models.
   - Allow a future configuration override only after model-specific test coverage exists.
   - Map TurboBulk validation errors into existing ingestion issues with model, row identity, field, and message when the API provides enough detail.

7. Implement delete semantics separately.
   - TurboBulk delete wants primary keys or explicit key fields.
   - Current delete planning must resolve target IDs before file creation.
   - Deletions should remain model-gated until conflict and dependency behavior is verified.

8. Keep fallback behavior explicit.
   - Default backend stays current native adapter execution.
   - If TurboBulk is configured but unavailable, fail fast before branch creation unless the operator explicitly chooses fallback in a later design.
   - Do not silently switch a large run from TurboBulk to the slower adapter path after validation has started.

## Validation

- `invoke harness-check`
- `invoke harness-test`
- `invoke lint`
- `invoke check`
- `invoke scenario-test`
- `invoke test`
- `invoke docs`
- New unit tests for capability detection and unavailable/permission/write-disabled responses.
- New unit tests proving current adapter execution remains default when TurboBulk is unavailable.
- New contract tests for `_id` field generation and conflict-field selection per supported model.
- New scenario test proving branch budget still shards a large model before execution backend handoff.
- Manual NetBox Docker validation for default path unchanged.
- Future TurboBulk validation requires a NetBox Cloud or Enterprise environment with TurboBulk and Branching enabled; this cannot be completed in the current local community Docker setup.

## Rollback

- Delete the optional backend code path and any UI/config toggles.
- Keep existing native adapter execution untouched as the rollback target.
- Remove TurboBulk-specific docs and tests.
- No database migration should be required for an initial capability-probe/prototype unless an operator-visible backend setting is added.

## Decision Log

- Rejected: Replace the current adapter path with TurboBulk. Reason: TurboBulk is not available on all supported NetBox deployments, and current sync behavior must remain the default.
- Rejected: Use TurboBulk as a side-channel import outside the existing sync UI. Reason: operators should not have a second workflow with different validation, branch, and merge semantics.
- Rejected: Write large initial syncs directly to main through TurboBulk. Reason: this removes the Branching review model that the plugin is built around.
- Rejected: Treat NQE coalesce fields as TurboBulk conflict fields automatically. Reason: TurboBulk conflict handling is database-constraint based, so each model needs explicit mapping.
- Rejected: Add Parquet as the first implementation target. Reason: JSONL.gz is sufficient for proving API semantics without introducing a new dependency.
