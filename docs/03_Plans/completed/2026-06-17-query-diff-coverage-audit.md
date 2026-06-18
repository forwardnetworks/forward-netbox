# Query Diff-Coverage Audit

## Goal

Give operators a way to see which enabled Forward NQE maps are eligible for
Forward `nqe-diff` execution versus which fall back to a full fetch every sync,
so they can bind the full-fetch-only maps for diff coverage and reduce Forward
API load.

## Constraints

- Read-only and offline: inspects the configured `ForwardNQEMap` records only;
  no Forward API calls, no credentials required.
- Must not change sync behavior; this is a diagnostic surface.
- Classification must match the runtime rule: a spec is diff-eligible only when
  it resolves to a `query_id` or `query_path`; a raw inline `query` forces a full
  fetch.

## Touched Surfaces

- `forward_netbox/management/commands/forward_query_diff_coverage_audit.py` —
  new management command.
- `forward_netbox/tests/test_query_diff_coverage_audit_command.py` — tests.

## Approach

Iterate enabled maps (or all with `--include-disabled`), classify each by
`execution_mode` into diff-eligible (`query_id`/`query_path`) or full-fetch-only
(`query`), and emit a JSON report with counts, the full-fetch-only list, and a
remediation hint. `--fail-on-full` exits non-zero for CI gating.

## Validation

- `invoke test --test-label forward_netbox.tests.test_query_diff_coverage_audit_command`
  (classification, enabled-only default vs include-disabled, fail-on-full exit).
- `invoke harness-check`, `invoke lint`.

## Rollback

Delete the command and its test. No data, schema, or runtime-path impact.

## Decision Log

- Offline/DB-only (not a live org-repo probe) so it runs without credentials and
  is safe in CI; the live org-repo gate is already covered by
  `forward_validation_org_query_audit`. This audit answers the complementary
  question: are the enabled maps bound for diffs at all.
