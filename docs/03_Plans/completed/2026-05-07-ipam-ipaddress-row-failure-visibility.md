# IP Address Row Failure Visibility for 0.7.0

## Goal
Keep `ipam.ipaddress` row failures observable and non-aborting while the broader 0.7.0 refactor continues, so missing-interface rows and timeout issues stay visible as ingestion issues instead of collapsing the shard.

## Touched Surfaces
- `forward_netbox/utilities/sync_reporting.py`
- `forward_netbox/utilities/sync_ipam.py`
- `forward_netbox/tests/test_sync.py`
- `forward_netbox/tests/test_synthetic_scenarios.py`
- `docs/03_Plans/technical-debt.md`

## Constraints
- Preserve the existing row-level skip/fail behavior.
- Do not add a separate import path or special customer-only handling.
- Keep timeout and lookup failures surfaced as operator-visible issues.
- Do not commit customer identifiers, network IDs, snapshot IDs, or screenshots.

## Validation
- `invoke lint`
- `invoke test`
- `invoke docs`

## Approach
Pin the current non-aborting behavior with regression coverage and keep the failure details visible while the larger sync/reporting refactor continues.

## Decision Log
- The screenshot indicates the current contract is already non-aborting; the remaining work is to pin it and keep the issue text visible during the 0.7.0 cleanup.
- Missing-interface rows should remain operator-visible failures because they represent a data/model mismatch, even when later rows continue successfully.

## Rollback
If the row failure visibility path regresses, revert the reporting pin and restore the previous error surfacing until the larger refactor can be corrected.
