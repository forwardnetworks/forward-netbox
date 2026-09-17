# Replace the hand-rolled Forward client with forward-sdk

## Goal

`forward_netbox/utilities/forward_api_impl.py` is retired on the 3.x lane and
`forward-sdk` becomes the plugin's only Forward Networks API layer. The plugin
keeps the Forward-specific policy that is genuinely its own -- scope predicates,
the NQE local-import compiler, head-commit resolution, the usage gate -- and
stops maintaining a second HTTP client to reach Forward.

The client is 2308 lines, of which roughly 200 build a URL, add Basic auth and
call `.json()`. The rest is retry policy, two paginators, two async poll loops, a
two-tier read cache, a cross-process rate limiter, 24 usage counters and thirteen
response-shape normalisations. `forward-sdk` already carries the transport half,
including several behaviours first learned here: the 409 `INVALID_CHANGE_PATH`
commit retry, the non-advancing-page detector, the row and page ceilings,
abbreviated-commit sanitizing and `Retry-After` in both forms.

This plan is delivered as a sequence of independently releasable changes. This
commit is the first of them.

## Constraints

- **`forward-sdk` is not yet published.** Nothing may declare a runtime
  dependency on it until it is on PyPI and pinned in `constraints.txt`, because
  this plugin ships into environments the operator does not control.
- **The usage gate must keep failing builds.** `evaluate_forward_api_usage`
  reads `observed_http_attempts_per_minute` and FAILs above the Forward SaaS
  hard block. Any change that leaves it without evidence degrades a failure into
  a silent pass.
- **Scope predicates are a data-loss path.** `build_device_tag_scope_where` and
  `build_endpoint_tag_scope_where` must keep producing byte-identical NQE to the
  bundled query branches. Divergence makes prune-out-of-scope delete previously
  imported objects. These builders stay plugin-owned for the whole migration.
- **Failure classification is read by operators.** `diagnostics.py` composes the
  persisted `ForwardIngestionIssue.message` from exception type and text, and
  recovers HTTP status from `exc.__cause__`. Changing exception classes silently
  changes what a support bundle says.
- Each commit touching a high-risk path carries this plan file in the same
  commit, per `scripts/check_harness.py`.
- No part of this is back-ported to `maint/2.9.x`.

## Touched Surfaces

This commit: `forward_netbox/utilities/forward_api_impl.py`,
`forward_netbox/tests/test_forward_api.py`, `forward_netbox/tests/test_health.py`.

Later commits in the sequence: new modules under `forward_netbox/utilities/`
(`forward_client_config.py`, `forward_usage.py`, `forward_read_cache.py`,
`forward_throttle.py`, `forward_nqe.py`, `forward_snapshots.py`,
`forward_query_index.py`, `forward_device_tags.py`, `forward_permissions.py`,
`forward_shapes.py`); then `models.py`, `diagnostics.py`,
`query_fetch_execution.py`, `forms.py`, `api/views.py`, `api_usage.py`; then the
dependency machinery (`pyproject.toml`, `poetry.lock`, `constraints.txt`,
`development/constraints-upgrade-from.txt`, `scripts/validate_sbom.py`,
`tasks.py`, the three README compatibility tables).

## Approach

Seven changes, each releasable on its own:

1. **Retire the dead client surface** (this commit).
2. Extract the read cache, throttle, counters and credential decrypt out of
   `ForwardClient` into named modules, still over hand-rolled HTTP, so those
   four concerns gain a test safety net before the transport changes underneath
   them.
3. Land the dependency machinery on its own, so an unrelated packaging failure
   is isolated from the migration.
4. Rewrite the failure classifiers in terms of auth / connectivity / transient /
   license, so the swap only has to re-point them at SDK exception types.
5. Swap the transport: `get_client()` constructs an SDK client, the remaining
   client methods become free functions, `forward_api_impl.py` is deleted.
6. Remove the compatibility passthrough, converting every call site explicitly.
   Its removal is what proves no call site was missed.
7. Deeper type adoption in the query-binding layer, gated separately.

### This commit

Four methods had no production caller anywhere in the tree:

- `trigger_snapshot_reachability` and its private poll loop
  `_wait_for_reachability_completion` -- removed. This also removes
  `reachability_trigger_calls` and `reachability_status_calls`, two counters
  that were recorded but never declared in `_empty_api_usage` and so never
  appeared in a usage summary or survived a reset.
- `resolve_nqe_query_head_commit` -- removed. It had no caller in production or
  in tests; a completed plan from 2026-08-04 records that it was deliberately
  left in place at the time as a mirror of a path that has since gone.
- `get_org_nqe_queries` and `get_nqe_repository_queries` -- these have no
  external caller but are still reached from
  `get_nqe_repository_query_index`, which has eleven. They become private rather
  than being deleted, so the public surface shrinks without moving live code.

The public surface goes from 23 methods to 19. Every method removed here is one
that would otherwise need a mapping decision, a rehoming decision and a test
rewrite later in the sequence.

## Validation

For this commit: the client's own suite and the health suite, plus a tree-wide
search proving no reference to the retired names survives in code, tests or
tooling. Behaviour is unchanged for every path that had a caller.

For the sequence as a whole:

- A request-sequence parity harness. Before the swap, record the exact HTTP
  requests the current client emits for a full smoke sync against a mock
  transport; after the swap, replay and diff. Any change in method, path,
  params, body or order is a regression signal. This is what catches
  throttle-ordering and paging-loop drift, which no unit test will.
- A golden assertion that `evaluate_forward_api_usage` produces a byte-identical
  summary from a scripted request sequence before and after.
- One case per SDK exception type added to the persisted-failure-reason tests
  before the swap, not after.
- `invoke harness-check`, `harness-test`, `check` and `test`, on an idle stack.

## Rollback

Each commit is independently revertible and the tree is releasable at every
step. This commit removes code with no callers, so reverting it restores dead
code and nothing else. The first commit with an irreversible consequence is the
dependency declaration; the first with a behavioural one is the transport swap.

## Decision Log

- **2026-09-07** -- Full replacement chosen over keeping `ForwardClient` as a
  permanent adapter. An adapter would have preserved the ~50 method-level test
  files unchanged, but leaves two Forward clients in the tree indefinitely.
- **2026-09-07** -- `forward-sdk` will be a declared, pinned PyPI dependency
  rather than vendored source. The repo has no vendoring precedent, and a
  vendored copy makes every upstream fix a re-vendor.
- **2026-09-07** -- Type adoption is bounded at the query-index module for the
  main sequence. The committed-query and repository-index metadata is read at
  roughly sixty sites whose dominant shape is a three-way
  `commitId` / `lastCommitId` / `lastCommit.id` fallback, present because
  Forward returns different shapes from different endpoints. Collapsing that
  into a model makes each site a semantic rewrite, in the two modules that
  already carry comments about the last two times commit precedence was gotten
  wrong. Deferred to its own change with its own gate.
- **2026-09-07** -- The SDK's `nqe.where.tag_scope()` will not be adopted, on
  data-loss risk, independent of any fix to it. The plugin's builders are twenty
  lines of pure string composition with no maintenance burden, and the failure
  mode if the probe and the query branch diverge is deletion of customer data.
- **2026-09-07** -- `get_org_nqe_queries` and `get_nqe_repository_queries` were
  made private rather than deleted, because the index method that survives them
  still calls them. Deleting them would have meant inlining live code in a
  commit whose value is that it changes no behaviour.
- **2026-09-17** -- `forward-sdk` is declared at `0.1.16` exactly, not the
  `0.1.3` first drafted here on 2026-09-07: this step landed ten days after
  the plan was written, and `0.1.16` is the actual latest published release as
  of today. Re-verified the reasoning still holds at this version - the SDK's
  own `httpx` range (`>=0.27,<1`) and `pydantic` range (`>=2.6,<3`) are
  unchanged between `0.1.3` and `0.1.16`, so the intersection with this
  project's own `httpx` pin and the base-install cost of `pydantic` becoming a
  required (not optional) dependency are exactly as analyzed below.
- **2026-09-17** -- Step 2's extraction stops at the shared, cross-process
  substrate (`forward_client_config.py`, `forward_usage.py`,
  `forward_read_cache.py`, `forward_throttle.py`). Each per-resource
  in-memory cache dict on `ForwardClient` (`_networks_cache`,
  `_snapshots_cache`, etc.) stays put, shaped and read differently per
  resource inside each `get_*` method body. Extracting those too would mean
  rewriting every cached-fetch method's body in the same change meant to
  prove the extraction changes nothing. Every method name the test suite or
  an internal call site depends on (`_record_api_usage`,
  `_throttle_request`, `_rate_limit_key`, `api_usage_summary`,
  `_shared_read_cache`/`_shared_rate_limit_cache` module functions,
  `_RATE_LIMIT_LAST_REQUEST_AT`) is preserved as a thin delegating wrapper;
  none were renamed or removed. Validated by running the full
  `test_forward_api.py` + `test_health.py` suite (149 tests, all green)
  against the extraction with no test changes.
