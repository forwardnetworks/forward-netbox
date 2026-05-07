# IP Address Row Failure Visibility for 0.7.0

## Goal
Keep `ipam.ipaddress` row failures observable and non-aborting while the broader 0.7.0 refactor continues, so missing-interface rows and timeout issues stay visible as ingestion issues instead of collapsing the shard.

## Constraints
- Preserve the existing row-level skip/fail behavior.
- Do not add a separate import path or special customer-only handling.
- Keep timeout and lookup failures surfaced as operator-visible issues.
- Do not commit customer identifiers, network IDs, snapshot IDs, or screenshots.

## Validation
- `invoke lint`
- `invoke test`
- `invoke docs`

## Decision Log
- The screenshot indicates the current contract is already non-aborting; the remaining work is to pin it and keep the issue text visible during the 0.7.0 cleanup.
- Missing-interface rows should remain operator-visible failures because they represent a data/model mismatch, even when later rows continue successfully.
