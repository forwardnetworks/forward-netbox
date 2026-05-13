# Query Contract Observability And Parity

## Goal

Make built-in NQE contracts, ingestion metadata, and object-model parity mechanically visible so sync behavior can be explained without reading logs or source code.

## Constraints

- Keep NetBox-ready output in shipped NQE where possible.
- Continue supporting both `query_id` and raw `query` execution for sync maps.
- Do not add hidden `query_overrides`.
- Do not commit customer tenant labels, network IDs, snapshot IDs, screenshots, or credentials.
- Preserve exact interface matching and identity behavior unless a separate plan changes those contracts.

## Touched Surfaces

- `forward_netbox/queries/`
- `forward_netbox/management/commands/`
- `forward_netbox/models.py`
- `forward_netbox/utilities/`
- `forward_netbox/tests/`
- `docs/02_Reference/built-in-nqe-maps.md`
- `docs/02_Reference/model-mapping-matrix.md`
- `docs/01_User_Guide/usage.md`
- `docs/01_User_Guide/troubleshooting.md`

## Approach

Add explicit output schemas for built-in sync queries and future internal validation queries. Contract tests should fail when a required field is dropped or renamed, slug-safe fields become invalid, model identity fields drift, or interface matching stops being exact.

Persist enough per-model execution metadata on ingestions to answer operational questions:

- executed map name
- execution mode: `query_id` or raw `query`
- query runtime
- returned row count
- failure count
- resolved target snapshot ID
- resolved baseline snapshot ID when applicable
- policy decisions and thresholds once validation policies exist

Improve live smoke reporting so a run states which source, snapshot selector, models, query modes, and ready-to-merge outcome were exercised without storing committed customer identifiers.

Harden issue and job-log rendering so any nested model instances, UUIDs, dates, or other non-primitive payload members are coerced to JSON-safe display values before they reach the UI or API. This keeps routing and merge failures diagnosable even if an unexpected object leaks into an issue payload.

Publish a concrete parity matrix with four states:

- supported now
- partially supported
- not yet implemented
- implemented but not yet live-validated

Use the parity matrix to pick Forward-native additions because they reduce operator investigation time, not because they mirror another integration. Good candidates remain snapshot comparison summaries, destructive-threshold merge blocking, health regression detection, validation-only dry runs, per-model rerun/revalidate actions, and richer Forward snapshot health exposure.

## Validation

- Built-in query output schema tests.
- Internal validation query schema tests when validation queries are introduced.
- Slug validity tests for slug-bearing models.
- Exact interface resolution tests.
- Snapshot selection persistence tests.
- No-hidden-override tests.
- Smoke command tests with synthetic identifiers only.
- Issue-rendering safety tests for nested payload values.
- Documentation checks for the parity matrix and built-in NQE reference.
- `invoke harness-check`
- `invoke harness-test`
- `invoke lint`
- `invoke check`
- `invoke scenario-test`
- `invoke test`
- `invoke docs`

## Rollback

Remove the added contract fixtures/tests, remove any added observability fields or migrations, restore previous smoke output, and revert the parity matrix updates.

## Decision Log

- Rejected broad transform-map UI parity because the project intentionally keeps authoring in Forward and shipped maps in versioned files.
- Rejected committing live smoke outputs because even redacted examples can accidentally preserve customer-derived structure.
- Rejected vague parity language because unsupported, partial, and unvalidated states lead to different operator expectations.
