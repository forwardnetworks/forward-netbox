# Support Bundle Completeness And Compression

## Goal

Make the Forward NetBox support bundle the primary troubleshooting artifact for
sync and execution-run issues by including the highest-value live diagnostics,
query and execution evidence, and recovery context in one export. Reduce the
cost of moving large bundles by compressing the downloaded artifact.

## Constraints

- Keep the export read-only.
- Do not change sync execution behavior, staging semantics, or recovery logic.
- Preserve current support-bundle fields unless there is a clear replacement
  with equal or better diagnostic value.
- Avoid adding customer-specific data or secrets.
- Keep browser download behavior explicit and predictable.

## Touched Surfaces

- `forward_netbox/views.py`
- `forward_netbox/utilities/execution_ledger_serialization.py`
- `forward_netbox/utilities/support_bundle_archive.py`
- `forward_netbox/utilities/health.py`
- `forward_netbox/tests/test_log_export.py`
- `forward_netbox/templates/forward_netbox/forwardsync.html`
- `forward_netbox/templates/forward_netbox/forwardexecutionrun.html`
- `docs/01_User_Guide/troubleshooting.md`
- release metadata in `README.md`, `docs/README.md`, and `pyproject.toml`

## Approach

1. Extend the support-bundle payload so the export carries the most useful
   troubleshooting context already available in the sync health surface and the
   execution-run ledger.
2. Add live diagnostic payloads where they are currently only exposed as
   separate downloads, so a single export can answer the common "what is broken
   and why?" questions.
3. Compress the downloadable support-bundle response so large exports are
   easier to transfer and store, with optional password protection.
4. Update the troubleshooting guide and release metadata to point operators at
   the richer bundle and the compressed download path.

## Validation

- `invoke harness-check`
- `invoke lint`
- `invoke check`
- `invoke test-isolated --test-label forward_netbox.tests.test_log_export`
- Rebuilt the development and `forward-netbox-test` Docker images so the new
  `pyzipper` dependency was present in the test runtime.

## Rollback

Remove the added live-diagnostic payloads and compression helper, then restore
the previous plain JSON download behavior.

## Decision Log

- Prefer one self-contained export plus compression over asking operators to
  collect several separate artifacts for every issue.
- Keep the API action unchanged for now so programmatic consumers retain raw
  JSON until a separate API contract is needed.
