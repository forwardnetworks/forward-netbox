# Sync Row Reporting Boundary Extraction

## Goal
Split the row application, deletion, issue recording, and aggregated warning behavior out of `forward_netbox/utilities/sync.py` into a dedicated reporting boundary without changing how row failures, skips, or dependency problems behave.

## Constraints
- Preserve the current row-level continue-on-error contract.
- Keep missing-interface, dependency-skip, validation, and query errors visible as ingestion issues.
- Do not change model adapter semantics while moving the reporting helpers.
- Keep the row-failure path operator-visible for the `ipam.ipaddress` case pinned earlier in the 0.7.0 work.

## Touched Surfaces
- `forward_netbox/utilities/sync.py`
- `forward_netbox/utilities/sync_reporting.py`
- `forward_netbox/tests/test_sync.py`
- `forward_netbox/tests/` additional focused regression coverage if needed
- `ARCHITECTURE.md`
- `docs/03_Plans/technical-debt.md`

## Validation
- `invoke lint`
- `invoke test`
- `invoke docs`
- `invoke ci`

## Decision Log
- `sync.py` still mixes adapter code with row outcome reporting, and that is the remaining structural hotspot after the model-specific boundary splits.
- This boundary is the right place to keep `ipam.ipaddress` row failures visible while the broader 0.7.0 refactor continues.
