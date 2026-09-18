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
- **2026-09-17** -- Step 4's translator (`forward_client_errors.py`) is built
  and tested standalone, not wired into `ForwardClient._request()` yet - that
  is step 5. Every branch dispatches on the SDK exception's own type or, for
  a status-carrying `ForwardAPIError`, its `.status` attribute directly,
  never on message-text guessing, because the SDK already did that guessing
  once inside itself to decide which of its own exception classes to raise.
  The message each branch builds still reproduces the exact wording
  `_request()` builds today for the equivalent httpx failure ("Forward API
  request timed out while connecting to Forward.", "Forward API request
  returned transient HTTP `<code>`; retry attempts were exhausted.", "Forward
  API request failed with HTTP `<code>`: `<body>`"), so `diagnostics.py`'s
  needle-based `failure_reason()` and its `_http_status_slug()` regex
  fallback classify a translated exception identically to today's - neither
  function is touched by this step. `TRANSIENT_FORWARD_HTTP_STATUS_CODES`
  moved from `forward_api_impl.py` into this new module (which
  `forward_api_impl.py` now re-imports) rather than being duplicated, since
  step 5 would otherwise need this module to import back from
  `forward_api_impl.py` - a circular import the move avoids before it can
  ever be introduced. One test case per SDK exception type was added
  (`forward_netbox/tests/test_forward_client_errors.py`), each asserting
  both the translated exception's type and its `failure_reason()` slug,
  per this plan's own Validation section.
- **2026-09-17** -- Step 5 is split into sub-commits rather than landed as
  one, on the same "small, independently releasable" principle every step
  before it has followed. A pre-implementation survey (method-by-method,
  ForwardClient vs the SDK's actual services) found that `forward_api_impl.py`
  is ~19 public methods, each wrapping its own caching logic around a
  `_request()` call, several with genuine plugin-authored fallback policy
  (`get_committed_nqe_query`'s index-then-commits-endpoint fallback for a
  Forward API quirk) that has no 1:1 SDK method. Rewriting all of it, with
  full shape verification against the SDK's pydantic models, in one commit
  risks exactly the kind of untested, hard-to-review change this plan's
  step-by-step structure exists to avoid. **Step 5 preserves `ForwardClient`'s
  external contract (method names, signatures, and return shapes) exactly**
  across every sub-commit - internals swap to the SDK, callers see no
  difference - so none of the ~60+ call sites need to change here. Step 6
  ("remove the compatibility passthrough, converting every call site
  explicitly") is what removes `ForwardClient` as a class and moves callers
  onto the SDK's own shapes directly; that is where the shape-adaptation
  work `ForwardClient`'s wrapper methods do in step 5 gets deleted, which is
  what proves every call site was actually migrated rather than left
  quietly depending on a compatibility shim.
- **2026-09-17** -- Step 5a: `forward_client_factory.py`'s `get_client()`
  builds the SDK client but is not called from `ForwardClient.__init__` yet
  (the next sub-commit wires it in) - same "build and test standalone first"
  pattern as step 4's translator. Two resolved design points:
  - **Rate limiting**: the SDK's `Throttle` protocol (`acquire() -> float`)
    is, by its own docstring, exactly this plugin's existing extension
    point - "implement this to coordinate across processes" is what
    `forward_throttle.py`'s Django-cache-backed `Throttle` already does, and
    the SDK's own built-in limiter is documented as per-client only, which
    is not this plugin's deployment shape (many workers, one Forward
    account). `_CrossProcessThrottleAdapter` wraps the plugin's `Throttle`
    to satisfy the protocol. Passing a custom `throttle=` makes the SDK's
    `Transport` use it EXCLUSIVELY (`self._throttle = throttle or
    self._default_throttle(config)`, `_sync/transport.py:53`), so there is
    no double-throttling and `forward_throttle.py`'s step-2 extraction
    remains the permanent home for pacing, not temporary scaffolding.
  - **Proxy resolution**: `resolve_proxies()` is designed for a per-request
    call keyed on the request URL, but the SDK accepts one static `proxy`
    string for the client's whole lifetime. Resolved once, against
    `base_url`'s own scheme, at client-construction time - every request
    this client makes targets the same `base_url`, so this loses only a
    per-request routing decision no real deployment's `resolve_proxies`
    implementation actually makes today.
- **2026-09-17** -- Step 5b: `get_client()` wired into `ForwardClient.__init__`
  (as `self._sdk_client`), and the first five read-only methods converted -
  `get_networks`, `get_snapshots`, `get_latest_processed_snapshot(_id)`,
  `get_snapshot_metrics`, `get_snapshot_data_file_hashes`. Each keeps its
  exact external return shape; only what fetches the data changed.
  - **Usage counters, resolved before touching any method**: approximating
    `http_attempts`/`http_successes`/etc. per call site would under-count,
    because the SDK retries internally inside one service call and a caller
    cannot see how many actual HTTP attempts that made -
    `evaluate_forward_api_usage`'s accuracy is this plan's own hardest
    constraint, so this could not be hand-waved. `forward_usage_hooks.py`'s
    `UsageTrackingHooks`, wired once at client construction via `get_client`'s
    new `usage=` parameter, hooks the SDK's own `on_request`/`on_response`/
    `on_retry` events (`_sync/transport.py`) directly onto
    `ApiUsageTracker`, so every counter stays accurate regardless of which
    method issued the call or how many times the SDK retried it internally.
    The one thing hooks cannot see is a transport failure (no response was
    ever received): `ForwardClient._call_sdk` records
    `http_transport_failures`/`http_timeout_failures` at the point it
    catches and translates a `ForwardTransportError`, the only place that
    information exists. `on_sleep` is deliberately NOT hooked for
    `throttle_sleep_seconds`, which `Throttle.throttle()` (step 2) already
    records on the same tracker - hooking it too would double-count.
  - **`get_latest_collected_snapshot_id` is explicitly NOT converted to
    `client.snapshots.latest_collected_id()`**, despite it being a close
    match (its `COLLECTED_PROBE` constant is structurally identical to this
    plugin's own hand-built probe query - the SDK credits this plugin by
    name for the pattern). That SDK method builds its scope predicate with
    `forward_sdk.nqe.where.tag_scope()`, which this plan's own Decision Log
    already rejected on data-loss risk ("the failure mode if the probe and
    the query branch diverge is deletion of customer data"). Using this SDK
    convenience method would silently reintroduce exactly that risk. The
    plugin's own hand-rolled version (still calling `self.run_nqe_query`
    with `build_device_tag_scope_where`) is untouched; it will pick up the
    SDK transparently once `run_nqe_query` itself is converted in a later
    sub-commit.
  - **`get_latest_processed_snapshot` behavior change, called out
    deliberately**: the SDK's `latest_processed()` excludes a Predict-created
    snapshot by default; the deprecated Forward endpoint this replaces had
    no such filter. Adopted as a correctness fix, not preserved as a quirk -
    a network using Predict would otherwise very often resolve "latest
    processed" to a simulation rather than a state the network was ever
    actually in.
  - `get_latest_processed_snapshot`'s return shape stays camelCase
    (`{id, state, createdAt, processedAt}`), matching Forward's own raw JSON
    the deprecated endpoint returned - confirmed by reading every caller
    (`sync_execution.py`, `query_fetch_execution.py`, `api/views.py`,
    `models.py`) rather than assuming consistency with `get_snapshots`'s
    own snake_case shape, which is a genuinely different convention used
    nowhere else in this method.
- **2026-09-17** -- Step 5c: NQE library methods converted -
  `has_nqe_library_write_permission`, `add_org_nqe_query`,
  `edit_org_nqe_query`, `get_org_nqe_head_commit_id`,
  `commit_org_nqe_queries`, `get_nqe_query_history`.
  - `has_nqe_library_write_permission` -> `client.user_accounts.
    get_current_user().roles`: the SDK's `CurrentUserRoles` model has
    `org`/`network` fields shaped identically to what this method already
    parsed by hand, and its own docstring says it exists for exactly this
    check ("Integrations read both to decide whether a login may write to
    the shared query library").
  - `add_org_nqe_query`/`edit_org_nqe_query` -> `client.nqe.repo.
    stage_add`/`stage_edit`, which stage a draft against the current user
    without committing - the same two-phase stage-then-commit shape this
    method's own `/users/current/nqe/changes` endpoint already had. These
    stay separate methods, not folded into the SDK's all-in-one `publish()`,
    because their caller (`query_binding_resolution.py`) decides add-vs-edit
    per file across a loop before committing the whole batch once; changing
    that call sequence is step 6's job, not step 5's.
  - `commit_org_nqe_queries` -> `client.nqe.repo.commit(paths, title=,
    body=)`, which turned out to already implement the exact same
    no-staged-changes 409 INVALID_CHANGE_PATH retry this method hand-rolled
    (`_unchanged_paths()` in `nqe_repo.py` parses "User has no changes at
    the following paths: ..." almost verbatim) - so that ~40-line retry
    block is deleted outright, not ported. One real behavior change,
    preserved deliberately rather than left to happen by accident: the old
    code always re-fetched `get_org_nqe_head_commit_id()` after committing;
    `commit()`'s own `CommitReport.commit_id` already carries the fresh
    commit when anything was actually committed, so `commit_org_nqe_queries`
    now writes that value directly into this client's own head-commit cache
    (mirroring exactly what a subsequent `get_org_nqe_head_commit_id()` call
    would have cached) instead of triggering a second, redundant fetch. The
    `CommitReport.commit_id` fallback to `get_org_nqe_head_commit_id()` is
    kept for the one case `commit()` doesn't resolve it itself: every
    requested path turned out to be an unchanged no-op.
  - `get_nqe_query_history` -> `client.nqe.repo.history(query_id)`, an exact
    shape match to what this method already returned.
