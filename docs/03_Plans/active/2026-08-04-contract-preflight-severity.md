# Query-ID Maps Execute Without A Commit; Health Severity Follows Consequence

## Goal

Two operator-visible outcomes.

1. A map bound to a Forward query ID runs its full query by ID, with no commit
   involved at any stage. Forward resolves the latest commit server-side. The
   plugin no longer refuses such a map as `unresolved_full_commit`, so a
   repository reorganisation, a permissions gap, or one failed history lookup
   can no longer empty an entire sync.
2. The health page stops describing a dead run as informational. A run in which
   every enabled model failed to fetch is reported as a blocking check that
   names the consequence and the remedy; a partial failure warns and names the
   models; a clean run says nothing.

## Contract

- A `QuerySpec` with a direct `query_id` yields `full_reason_code == "eligible"`
  and `full_eligible is True` when `commit_id` is `None`, `""`, or `"head"`,
  and reports `full_unpinned_head is True`.
- A path-bound spec with no commit still yields `unresolved_full_commit` and
  `full_eligible is False`.
- Dropping the commit does not drop the shape checks: `unverified_full_source`,
  `unverified_full_declarations`, and `unsupported_full_parameters` continue to
  close the full contract for an ID-bound map.
- `diff_reason_code` and `diff_eligible` are unchanged in every case.
- `_persisted_query_contract_preflight` reports no issue for a query-ID map with
  an empty commit; it still reports one for a path-bound map.
- `_fetch_failure_check` returns `fail` when every attempted model failed,
  `warn` when some did, and `None` when none did or when there is no ingestion.

## Constraints

- No NetBox/Branching behaviour change, no migration, no model field change.
- `ResolvedExecutionContract.fingerprint` must not change. Contributor
  baselines, diff baseline selection, and expected-contributor matching are keyed
  by it, so `full_unpinned_head` is deliberately excluded from the fingerprint
  (`commit_id` already participates through `full_revision.fingerprint`).
- Diff execution is out of scope and must not be relaxed. `run_nqe_diff` POSTs to
  `/nqe-diffs/{before}/{after}`, where the two commits are path segments rather
  than optional body fields, so a diff genuinely cannot run without concrete
  commits.
- The Docker stack was not used. Django tests that need `netbox.settings` were
  not run in this change.
- No customer identifiers in code, comments, tests, or docs.

## Touched Surfaces

- `forward_netbox/utilities/query_execution_contract.py` — the commit
  requirement for direct-query-ID full contracts; new `full_unpinned_head`.
- `forward_netbox/utilities/query_fetch_execution.py` — `validate_rows` accepts
  the contract and names the map on a row-shape failure.
- `forward_netbox/utilities/health.py` — persisted preflight scope, reworded
  messages, new `_fetch_failure_check`.
- `forward_netbox/tests/test_query_execution_contract.py` — updated matrix and
  new eligibility/shape tests.
- `forward_netbox/tests/test_contract_preflight_message.py` — updated wording
  assertions, new persisted-scope and fetch-failure tests.

## Approach

1. In `resolve_execution_contract`, compute `runs_by_direct_query_id` from
   `spec.query_id` and guard the `unresolved_full_commit` branch with it. The
   branch order is otherwise untouched, so an ID-bound map that fails a later
   check reports that later reason instead of being masked by the commit one.
2. Add `full_unpinned_head` to `ResolvedExecutionContract`, set when an ID-bound
   map runs with no commit. It is reporting only; it is not part of the
   fingerprint and does not affect eligibility.
3. Carry the map identity into the fetch-time row-shape failure. The row check
   already exists and already runs per model, but its message named only the
   model. `validate_rows` now takes the contract and re-raises with the map name,
   map id, and whether it ran at unpinned head.
4. In `health.py`, stop listing query-ID maps in the persisted preflight, reword
   the remaining path-bound case truthfully, and give the blocking variant the
   consequence and the remedy.
5. Add `_fetch_failure_check`, which reads `latest_ingestion.model_results` —
   what the last run actually did — and sets severity from the consequence.

## Validation

- `python3 -m unittest discover -s scripts/tests` — 312 tests, OK.
- `python3 scripts/check_harness.py` and `--base origin/main`.
- `pre-commit run --all-files`, twice.
- Contract behaviour exercised directly against the real module source, which is
  pure stdlib and loads without NetBox: ID-bound map with `None`/`""`/`"head"`
  becomes eligible with `full_unpinned_head`; path-bound map still reports
  `unresolved_full_commit`; oversupplied parameters still report
  `unsupported_full_parameters`; source drift still reports
  `unverified_full_source`.
- Health functions exercised by extracting them from the real `health.py` by AST
  and executing them with a stub `_check`, since the module imports NetBox.
- NOT RUN, needs the stack: the Django suites
  `forward_netbox/tests/test_query_execution_contract.py` and
  `forward_netbox/tests/test_contract_preflight_message.py`, and any live sync.

## Rollback

Revert the commit. No migration, no persisted state, no data cleanup. Reverting
restores the commit requirement for ID-bound maps and the previous health
wording. Any ingestion rows written while the change was live remain valid;
`_fetch_failure_check` only reads them.

## Decision Log

- **Rejected: making the persisted health check blocking.** The first framing of
  this work was that the health page's `unresolved_full_commit` was killing every
  sync and should therefore be a failure. It is a different computation from the
  runtime contract that happens to share the reason-code name:
  `_persisted_query_contract_preflight` reads `ForwardNQEMap.commit_id` at page
  render, while the runtime code reads the resolved spec. The existing comment in
  `health.py` records a support bundle where 32 of 32 maps reported it both while
  a sync applied nothing and while the same sync applied 24,748 changes. Making
  it blocking would have fired a failure on every healthy query-ID install.
- **Rejected: relaxing the whole reason-code family.** Only the commit
  requirement is removed, and only for direct-ID bindings. `unverified_full_source`,
  `unverified_full_declarations`, and `unsupported_full_parameters` are the
  protection the commit requirement was standing in front of: the plugin injects
  parameters (shard keys, device-tag selection, endpoint opt-ins), and a query
  that no longer declares them would silently return the wrong row set rather
  than the wrong row shape. Those checks are kept.
- **Rejected: relaxing the commit for path-bound maps too.** A path is not an
  identity. `QuerySpec.resolve` deliberately refuses to adopt a head commit when
  the path now resolves to a different query, because that would execute another
  query's revision. Execution never sends a path, so an ID binding has no
  equivalent hazard.
- **Excluded `full_unpinned_head` from the contract fingerprint** so baseline and
  contributor matching are unaffected.
- **Left `_build_workload_jobs`'s all-or-nothing behaviour in place.** One
  ineligible map still returns zero jobs for the whole sync. Removing the commit
  requirement removes the condition that was making that fire across every map at
  once, but the amplifier itself is a deliberate fail-closed with its own tests
  and is out of scope here. See Open.

## Open

- **A custom (non-built-in) map bound by query ID with no commit still cannot
  run.** It carries no query source, so `full_query_source` is `None`,
  `source_verified` is `False`, and the contract closes on
  `unverified_full_source` rather than on the commit. Source hydration is keyed
  by commit (`_hydrate_diff_contract_sources` returns early when the commit is
  empty), so there is currently no path that verifies a custom ID-bound query's
  source without one. Built-in maps are unaffected because they carry their
  bundled source and hash. Closing this needs a way to read a query's source by
  ID at head, which is a separate change.
- **One ineligible map still zeroes the whole run.** `_build_workload_jobs` sets
  `contract_preflight_blocked` and returns `[]`, so a single refused model still
  stops every other model. A per-model skip would be the honest behaviour and
  would make the new warn-level fetch check the normal reporting path.
- **Row-shape validation cannot detect an ignored parameter.** It checks that
  required fields are present, not that a parameter was honoured. A query that
  silently stopped accepting a shard key would return correctly shaped rows, just
  too many of them. This is why the declaration checks were kept rather than
  replaced by the fetch-time check.
- The health-page `warn`/`fail` split for fetch failures has not been seen
  against a live run.
