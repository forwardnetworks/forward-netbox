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
- **2026-09-17** -- Step 5d: the hardest conversion in the sequence -
  `_get_org_nqe_queries`, `_get_nqe_repository_queries`,
  `get_committed_nqe_query`'s commits-endpoint fallback - the exact
  index-then-commits-endpoint policy this method exists to enforce
  (documented in `test_head_commit_from_listing.py`'s own docstring: a
  commit-less listing row returned directly once caused a customer's whole
  sync to fetch nothing while reporting success) is preserved unchanged;
  only what fetches each side changed.
  - `_get_org_nqe_queries` -> `client.nqe.queries(directory=...)`
    (`NqeService.queries`, backed by the SAME unpublished `getNqeQueries`
    op as the plugin's own `/nqe/queries?dir=` call - confirmed by reading
    `_ops/nqe.py`, not assumed). Its `NqeQuery` model has no commit field at
    all, matching the real endpoint's limitation exactly - this is NOT
    `NqeRepository.queries()` (a different SDK method, on `client.nqe.repo`,
    backed by the commits-endpoint and carrying commit info), which would
    have silently made the whole fallback below load-bearing-but-untested.
  - `_get_nqe_repository_queries` (non-org) and `get_committed_nqe_query`'s
    fallback both use `client.nqe.repo.queries(...)`, whose
    `queries_from_payload` helper already collapses Forward's two response
    shapes (a wrapped `{"queries": [...]}` listing, or a bare single object
    for a specific path lookup) into one uniform list - exactly the
    two-shape branching `get_committed_nqe_query` used to hand-roll.
  - Real bug caught and fixed during conversion, before it shipped:
    `RepositoryQuery` carries its commit under one of two field names
    depending on what was asked for (`last_commit_id` when listing at head,
    a nested `last_commit.id` for a specific commit - its own docstring
    warns "reading only one of them loses the pin"). The commits-endpoint
    fallback now checks both, exactly as the model's own docstring
    prescribes; missing this would have silently reintroduced a narrower
    version of the same commit-loss bug this whole method exists to guard
    against.
  - `get_committed_nqe_query`'s no-longer-needed guard ("response was not a
    dict") is dropped rather than ported: the SDK's own pydantic validation
    already rejects a malformed response with `ForwardResponseError`, which
    `_call_sdk` translates like any other failure.
- **2026-09-17** -- Step 5e: `run_nqe_query`/`run_nqe_diff`, the single
  most-called pair of methods in the client (27+ internal callers of
  `run_nqe_query` alone) and the last piece of the transport swap. The
  plugin's own hand-rolled execute/poll/paginate state machine is deleted
  outright, not ported: `client.nqe.execute()`/`execution.wait()`/
  `execution.result_page()` replace it, and the plugin's OWN pagination
  safety loop (identical-page-streak detection, row/page ceilings) is kept
  completely unchanged, just fed by `execution.result_page()` instead of a
  raw HTTP call - the exact same "swap the leaf fetch, keep the policy"
  pattern every other method in this sequence has followed.
  - **Real, unavoidable behavior change, not glossed over**: the plugin's
    own poll loop had a hard ceiling by POLL COUNT
    (`nqe_async_max_polls`); `NqeExecution.wait(timeout=...)` is purely
    TIME-based, with no poll-count concept at all, and `timeout=None` means
    "wait forever" - a real regression from the old hard stop if used
    naively. `_wait_for_nqe_execution` derives an equivalent duration
    ceiling (`nqe_async_max_polls * nqe_async_poll_interval_seconds`) and
    takes the smaller of that and any remaining time to an explicit
    `deadline`, so a runaway execution still cannot hang a worker
    indefinitely even with no per-workload deadline configured. `wait()`'s
    own `ForwardTimeoutError` on either bound already translates to
    `ForwardFetchBudgetExceededError` via step 4's translator - no new
    mapping needed.
  - **Abbreviated commit hashes**: the SDK's `QueryRef.by_id` sanitizes a
    commit id too, but by *raising* `ForwardConfigurationError` on an
    abbreviated one, on the theory that silently discarding a pin answers a
    different question and reports success. The plugin's own
    `_commit_id_for_nqe_execution` (silently falls back to head) runs
    first, so the SDK never sees an abbreviated hash to reject -
    `run_nqe_query` already had this guard; `run_nqe_diff` never checked
    for one at all, so applying the same guard there is a deliberate
    consistency fix, not a preserved behavior.
  - **NDJSON removed**: `_fetch_nqe_async_result_page`'s content-negotiated
    Accept header (preferring `application/x-ndjson`) has no SDK
    equivalent - `execution.result_page()`'s underlying op always requests
    plain JSON; only `execution.stream()`, a different, non-paginated
    method this plugin cannot use (its own pagination-safety loop needs
    page-at-a-time control), requests ndjson. Accepted as a minor,
    documented transport-efficiency change, not a correctness one: same
    records, plainer wire format.
  - **New deadline pre-flight checks added**: neither `execution.result_page`
    nor `client.nqe.diff_page` takes a per-call deadline (unlike `_request`,
    which checked one on every retry attempt), so an explicit
    `time.monotonic() >= deadline` check was added before the first page
    fetch in both methods - closing a gap the fetch_all loop's own
    per-iteration check didn't cover for page one.
  - Dead code deleted outright (fully superseded, no callers left):
    `_start_nqe_async_execution`, `_get_nqe_async_status`,
    `_wait_for_nqe_async_completion`, `_nqe_async_execution_payload`,
    `_nqe_async_status_state`, `_nqe_async_outcome`,
    `_nqe_async_error_summary`, `_parse_nqe_async_result`, `_parse_nqe_lines`.
  - Tests for internals that no longer exist were deleted, not adapted -
    the SDK's own poll-backoff schedule and its own NDJSON/JSON
    content-negotiation are the SDK's testing responsibility now, not this
    plugin's: `test_run_nqe_query_async_prefers_ndjson_for_results`,
    `test_parse_nqe_async_result_falls_back_to_json`,
    `test_run_nqe_query_async_poll_uses_exponential_backoff`,
    `test_run_nqe_query_async_default_poll_backoff_reaches_five_second_ceiling`,
    `test_run_nqe_query_async_poll_backoff_caps_at_ceiling`. A new test
    (`test_run_nqe_query_derives_a_wait_ceiling_from_poll_config_without_a_deadline`)
    replaces the old poll-limit test, pinning the new duration-ceiling
    arithmetic instead.
- **2026-09-18** -- Step 6 scoping, before any implementation. A tree-wide
  survey found the real size is larger than this plan's original "~60+"
  estimate: roughly 84 literal `.method(` call sites across 23 production
  files (28 files call `source.get_client()`, but 5 of those only acquire a
  client and hand it to another object without calling methods on it
  themselves), plus two duck-typed sites a literal grep would miss
  (`api_usage.py`'s `getattr(client, "api_usage_summary", None)`,
  `sync_orchestration.py`'s `getattr(executor, "client", None)`).
  `run_nqe_query`/`run_nqe_diff` alone account for 29 of the 84.
  **Class-level coupling is smaller than the call-site count suggests**:
  only `models.py` (constructs it) and a dead lazy re-export in
  `utilities/__init__.py` (zero production callers) reference `ForwardClient`
  as a type; no `isinstance` checks anywhere. This means step 6's actual
  work is almost entirely call-site renames, not untangling type
  dependencies - and the plan's flagged "data-loss path" scope predicates
  (`build_device_tag_scope_where` et al.) turn out to already be
  module-level free functions in `forward_api.py`, never `ForwardClient`
  methods, so they need zero changes here.
  **The client is held as long-lived instance state in four classes**
  (`executor_base.py`, `sync.py`, `validation.py`, and - the real hot spot -
  `query_fetch_execution.py`'s `ForwardQueryFetcher`, which alone makes 6 of
  the 29 `run_nqe_query`/`run_nqe_diff` calls against `self.client` across a
  multi-step fetch pipeline). Everywhere else the pattern is
  `client = sync.source.get_client()` once per function, several calls
  against that local variable, not threaded further - mechanical to convert
  to a free-function-plus-explicit-client-argument shape almost everywhere
  except `ForwardQueryFetcher`, whose constructor signature change ripples
  into every place that builds one.
  **Test-mocking finding that sets the real sub-step boundary**: no test
  anywhere uses `Mock(spec=ForwardClient)` - every one of the ~20 test files
  that touch this mocks loosely via
  `patch.object(ForwardSource, "get_client", return_value=Mock())` and then
  stubs individual methods on that Mock. Changing the call shape from
  `client.method(...)` to `module.method(client, ...)` breaks every one of
  those stubs, so the true size of each sub-step is the production renames
  PLUS a parallel, roughly 1:1 test-mock rewrite - not the production
  renames alone.
  **Sub-step breakdown**, grouped by method-family (natural risk boundary
  here tracks call-site concentration, not per-method complexity, so this
  beats either a risk-tier split or one-PR-per-file):
  - **6a** - snapshot/network free functions (`get_networks`,
    `get_snapshots`, `get_latest_processed_snapshot(_id)`,
    `get_latest_collected_snapshot_id`, `get_snapshot_metrics`,
    `get_snapshot_data_file_hashes`, `get_device_mgmt_tags`): ~20 call
    sites, lowest risk, no long-lived-object complications. Goes first to
    prove the free-function shape and its test-mock-rewrite pattern before
    the bigger NQE surface.
  - **6b** - NQE query-index/repository functions
    (`get_nqe_repository_query_index`, `get_committed_nqe_query`,
    `resolve_nqe_query_reference`, `get_nqe_query_history`,
    `has_nqe_library_write_permission`, `add_org_nqe_query`,
    `edit_org_nqe_query`, `commit_org_nqe_queries`,
    `get_org_nqe_head_commit_id`): ~23 call sites. Medium risk - feeds the
    head-commit-resolution logic step 5d already found two real bugs in, so
    conversion here needs the same care as that step, not mechanical
    renaming.
  - **6c** - `run_nqe_query`/`run_nqe_diff` (29 call sites, the largest
    single cluster): split into `ForwardQueryFetcher` (6 sites, the
    long-lived-client class whose constructor signature changes and
    ripples into every caller that builds one) as its own pass, then the
    remaining ~23 scattered one-off callers as a second pass.
  - **6d** - the two duck-typed usage-tracking sites, `ForwardClient` class
    deletion (`forward_api_impl.py`), `models.py`'s construction/import, the
    dead `utilities/__init__.py` re-export, and a final tree-wide
    `grep -rn "ForwardClient"` sweep proving nothing survives outside
    `exceptions.py`'s unrelated `ForwardClientError` family - the sub-step
    that "proves no call site was missed," done last once 6a-6c convert
    every real caller.
- **2026-09-17** -- Step 5f: final step-5 cleanup, deleting the now-fully-dead
  raw-HTTP transport that 5a-5e's method-by-method conversion left behind
  with zero remaining callers. Confirmed via `grep -n "self\._request("`
  that no production method still calls it before deleting anything.
  Removed from `forward_api_impl.py`: `_request` itself, its four private
  helpers (`_api_url`, `_headers`, `_auth`, `_proxy_mounts`),
  `_record_http_status_class` (only ever called from inside `_request`,
  no direct test), the module-level retry-backoff helpers
  (`_parse_retry_after`, `_retry_wait_seconds`) and their two constants
  (`DEFAULT_FORWARD_API_RETRY_BACKOFF_SECONDS`,
  `MAX_FORWARD_API_RETRY_BACKOFF_SECONDS`), and the now-unused `random`,
  `httpx`-adjacent (`resolve_proxies`), `ForwardConnectivityError`,
  `ForwardLicenseTierError`, `is_license_tier_denial`, and
  `license_tier_denial_message` imports - the license-tier classification
  these last two powered already lives in `forward_client_errors.py`'s
  `translate_client_exception`, wired in since step 5b. `_rate_limit_key`/
  `_throttle_request` were kept: they're thin delegating wrappers, not
  `_request` internals, and remain legitimate test seams. `httpx` itself
  stays imported - `_call_sdk` still checks
  `isinstance(exc.__cause__, httpx.TimeoutException)` to classify a
  transport failure the SDK's hooks can't see.
  `forward_api.py`'s facade dropped its now-broken re-export of
  `DEFAULT_FORWARD_API_RETRY_BACKOFF_SECONDS` (grepped repo-wide first to
  confirm nothing outside that one facade line referenced it).
  `TRANSIENT_FORWARD_HTTP_STATUS_CODES` stays imported into
  `forward_api_impl.py` despite no remaining internal use, because
  `forward_api.py` still re-exports it by name; added it to this module's
  own `__all__` list (the file's established convention for a name that's
  only "unused" because another module imports it by attribute) rather than
  a per-line `noqa`.
  In `test_forward_api.py`, deleted the 10 tests that drove `_request`
  directly plus the entire `RetryBackoffHelperTest` class (5 more, testing
  `_parse_retry_after`/`_retry_wait_seconds` directly) - all testing
  internals that no longer exist. Kept
  `test_reset_api_usage_summary_preserves_rate_limit_configuration` and
  `test_api_usage_summary_reports_observed_http_attempt_rate`, since both
  call `_record_api_usage`/`_record_http_attempt_usage` directly and those
  methods are still very much alive (called from `_call_sdk`'s hooks path).
  In `test_license_tier.py`, deleted `LicenseTierClientTest` outright
  (it drove `self.client._request(...)` against a mocked `httpx.Client` to
  prove the classification was actually wired into the client, not just
  correct in isolation) - `LicenseTierDenialTest` (pure
  `is_license_tier_denial`/`license_tier_denial_message` unit tests, no
  `_request` dependency) is untouched, and the "is it actually wired in"
  property this class existed to prove now has its SDK-path equivalent in
  `test_forward_client_errors.py`
  (`test_a_permission_error_with_a_license_denial_body_becomes_license_tier_error`,
  `test_a_permission_error_without_a_license_denial_body_is_generic`).
  Full targeted suite (`test_forward_api`, `test_head_commit_from_listing`,
  `test_forward_client_factory`, `test_forward_usage_hooks`,
  `test_forward_client_errors`, `test_health`, `test_license_tier`,
  `test_execute_by_query_id_without_commit`; 199 tests) green against the
  edited worktree, confirmed by inspecting the running test container's
  bind mount rather than assuming it. This closes out step 5 entirely -
  step 6 removes `ForwardClient` as a class and converts every call site to
  the SDK's own shapes directly.
- **2026-09-18** -- Step 6a: the lowest-risk method family from the scoping
  survey above (`get_networks`, `get_snapshots`, `get_latest_processed_snapshot`,
  `get_latest_processed_snapshot_id`, `get_latest_collected_snapshot_id`,
  `get_device_mgmt_tags`, `get_snapshot_metrics`, `get_snapshot_data_file_hashes`,
  plus the private helper `_processed_snapshots_newest_first` these
  internally depend on) converted from `ForwardClient` instance methods
  (`client.get_networks()`) to module-level free functions in
  `forward_api_impl.py` taking the client as an explicit first argument
  (`get_networks(client)`), re-exported from `forward_api.py`'s facade by
  the same names. **`ForwardClient` itself is NOT deleted or renamed yet** -
  it keeps `__init__` and every private helper (`_call_sdk`,
  `_shared_read_cache_key`, `_read_cache_lock`, `_record_read_cache_hit`,
  etc.) as bound methods; only the public, externally-called surface moves
  to free functions. This is a deliberate, narrower reading of "the
  compatibility passthrough" than a full class deletion in one step: the
  object genuinely still needs somewhere to hold its per-resource caches,
  throttle, usage tracker and SDK client, and untangling that from whether
  callers invoke it via `.method()` or `function(client, ...)` are separable
  concerns. Full class removal (and whatever replaces the state container)
  is deferred to 6d, once every method family has made this same move and
  the actual remaining shape of "what `source.get_client()` returns" is
  fully visible.
  **22 production call sites across 11 files** converted (`models.py`,
  `forms.py`, `api/views.py`, `forward_module_readiness.py`, `primary_ip.py`,
  `primary_ip_audit.py`, `sync_execution.py`, `query_fetch_execution.py`,
  `health.py`, `snapshot_freshness.py`, `sync_facade.py`) - close to the
  scoping survey's ~20 estimate. `_processed_snapshots_newest_first` and
  `get_latest_collected_snapshot_id` still call `client.run_nqe_query(...)`
  as a bound method (untouched, that's step 6c) since a free function can
  call methods still bound to the same object just as freely as it can call
  other free functions - no ordering dependency between sub-steps here.
  **Test-mock rewrite, the real size of this step per the scoping survey**:
  every test that previously did `client.get_snapshots.return_value = X`
  (or similar) on a bare `Mock()` stopped working, since nothing calls that
  attribute as a method anymore - `unittest.mock.patch` must target the free
  function's name in whichever module actually imported and calls it
  (`patch("<caller module>.get_snapshots", ...)`), per the standard
  "patch where it's looked up, not where it's defined" rule. Two patterns
  emerged across ~15 test files: (1) individual `with patch(...)` blocks per
  test, used where a handful of sites needed fixing; (2) for
  `test_sync.py`'s ~50 sites (by far the largest concentration, spread
  across one `TestCase` exercising both `sync_execution.py` and
  `query_fetch_execution.py`), a `setUp`-level patch replacing the free
  function with a forwarding `side_effect` lambda
  (`lambda client, *a, **kw: client.get_snapshots(*a, **kw)`) in both
  modules at once - this meant every existing per-test
  `client.get_X.return_value = ...` / `.assert_called_once_with(...)` line
  needed ZERO changes, since they still configure/assert on the same Mock
  attribute, just reached one level of indirection later. The same
  forwarding-shim pattern fixed `test_primary_ip_audit.py`'s hand-written
  `_FakeClient` fake (not a `Mock`) with a single `side_effect`.
  **Real bug this conversion surfaced, not introduced**: three tests
  (`test_api_views.py`'s `test_available_tags_returns_distinct_tags`, and
  two `snapshot_freshness`-adjacent catch-up-decision tests) had been
  silently relying on a broad `except Exception:` handler in
  `available_tags`/`latest_processed_catchup_decision` swallowing a
  `TypeError` from calling the real (unmocked) snapshot-lookup path against
  a bare `Mock()`/`SimpleNamespace()`, and asserting on the resulting
  fallback response rather than the real success path - the tests passed
  before this conversion for the wrong reason. Fixed by actually mocking
  the snapshot lookup, which is what each test's own name and assertions
  already claimed to be doing.
  Full targeted suite (12 files: `test_forward_api`, `test_api_views`,
  `test_forms`, `test_health`, `test_models`, `test_primary_ip_audit`,
  `test_primary_ip_integration`, `test_query_execution_contract`,
  `test_scheduled_jobs`, `test_sync`, `test_sync_facade`,
  `test_sync_orchestration`; 765 tests) green against the running
  container's bind-mounted worktree.
- **2026-09-18** -- Step 6b: the NQE query-index/repository method family
  (`get_nqe_repository_query_index`, `get_committed_nqe_query`,
  `resolve_nqe_query_reference`, `get_nqe_query_history`,
  `has_nqe_library_write_permission`, `add_org_nqe_query`,
  `edit_org_nqe_query`, `get_org_nqe_head_commit_id`,
  `commit_org_nqe_queries`, plus the two private helpers
  `_get_org_nqe_queries`/`_get_nqe_repository_queries` these internally
  depend on) converted to free functions, same shape as 6a
  (`get_committed_nqe_query(client, ...)` instead of
  `client.get_committed_nqe_query(...)`). 32 production call sites across 4
  files converted (`change_control/criteria.py`, `api/views.py`,
  `query_binding_resolution.py`, `query_registry.py`) - `ForwardClient`'s
  class-level coupling really is as small as the earlier scoping survey
  found: no file needed anything beyond a straight method-call rename, no
  `isinstance` checks, no constructor changes.
  **Real bug caught before merging, not by a test**: the free functions
  were defined and every call site converted, but `forward_api.py`'s
  facade was never updated to re-export them - `query_registry.py`'s own
  `from .forward_api import get_committed_nqe_query` therefore failed at
  Django app-load time, which means EVERY test in the app would have
  failed, not just ones touching NQE queries. Caught when running
  `test_forward_api.py` directly (an `ImportError` at collection, not a
  test failure) before it could reach a push. One of three parallel forks
  fixing this step's test files hit the identical `ImportError` first and
  correctly diagnosed it as a production bug outside its own assigned
  scope rather than trying to work around it - a fresh instance with zero
  context recognized "this failure blocks every test, not mine" as a
  do-not-touch signal, exactly as intended.
  **Test-mock rewrite** (three parallel forks + direct work, ~250 sites
  across 10 files) followed the same "patch where it's looked up" rule as
  6a, with one new wrinkle: `query_fetch_execution.py` doesn't call these
  functions directly, it calls `query_registry.py`'s
  `resolve_query_specs_for_client`, which does - so `test_sync.py`'s
  `ForwardSyncRunnerTest` setUp (already patching 6a's functions in two
  modules) grew two more module targets
  (`query_registry`, `query_binding_resolution`) rather than one, tracing
  the real call chain instead of assuming the nearest importer was the
  right target. Two test files without a shared class-level client fixture
  (`test_builtin_query_id_binding.py`, `test_execute_by_query_id_without_
  commit.py`) used a `setUp`/`setUpModule` teardown-paired
  `patch(...).start()`/`.stop()` instead of a context-manager `with`
  block, since their client fixtures are built by a plain helper function
  shared across many independent test methods rather than one `setUp`.
  Full targeted suite (10 files: `test_forward_api`, `test_query_binding`,
  `test_query_registry`, `test_api_views`,
  `test_validation_org_query_audit_command`,
  `test_builtin_query_id_binding`,
  `test_execute_by_query_id_without_commit`,
  `test_head_commit_from_listing`, `test_health`, `test_sync`; 638 tests)
  plus all 5 `test_change_control_*` files (58 tests, covering
  `criteria.py`'s only other production caller) green.
- **2026-09-18** -- Step 6c: `run_nqe_query`/`run_nqe_diff` - the largest
  cluster (29 production call sites, more than 6a and 6b combined) -
  converted to free functions, along with their private helper chain
  (`_run_nqe_query_async`, `_start_nqe_execution`, `_wait_for_nqe_execution`,
  `_fetch_nqe_async_result_page`, `_page_signature`, `_parse_nqe_records`,
  `_parse_nqe_diff_rows`, `_nqe_query_ref`) that only these two methods
  ever called. This is the LAST method family: after this conversion,
  `ForwardClient` retains only `__init__` and pure internal-state plumbing
  (`_call_sdk`, `_record_*`, `_shared_read_cache_*`, cache-copy helpers) -
  no public method survives on the class, confirming the whole migration's
  "step 5 preserves the contract, step 6 removes it" split is now complete
  in substance; only the formal class deletion (step 6d) remains.
  `_nqe_query_ref` takes no `client` argument at all (its body never used
  `self`), so its two direct test call sites
  (`ForwardClient._nqe_query_ref(Mock(), ...)`) simplified to
  `forward_api_impl._nqe_query_ref(...)` with the now-pointless `Mock()`
  placeholder dropped entirely, rather than gaining one.
  29 production call sites across 16 files converted (15 external plus
  `forward_api_impl.py`'s own two internal callers,
  `get_latest_collected_snapshot_id` and `get_device_mgmt_tags` from step
  6a, which had been calling `client.run_nqe_query(...)` as a bound method
  the whole time). Eight files needed a brand-new `from .forward_api import
  run_nqe_query` where none existed before, matching each file's own
  existing import style exactly (module-level for most; local
  function-body imports for the two files - `apic_cimc_readiness.py`,
  `dlm_notice_audit.py` - that already imported everything else that way).
  **That local-import style produced one real test-patching lesson**: a
  function-body `from .forward_api import run_nqe_query` is not a module
  attribute at patch time, so `patch("...apic_cimc_readiness.run_nqe_query")`
  raised `AttributeError: ... does not have the attribute`. Fixed by
  patching the SOURCE (`forward_api.run_nqe_query`) instead of the
  importing module - correct here specifically because the import
  statement re-executes fresh on every function call, so it picks up
  whatever the patched source currently is. This is the exception to
  "patch where it's looked up," and only applies to function-body imports,
  never module-level ones.
  Test-mock rewrite: ~233 matches across 21 files, split across four
  parallel forks plus direct work on `test_forward_api.py` and eleven
  smaller files. One structural finding the forks and direct work both hit
  independently: several call sites reach `run_nqe_query` through
  MULTIPLE layers of indirection - `query_fetch_execution.py`'s
  `ForwardQueryFetcher._run_nqe_query`/`_run_nqe_diff` wrapper methods call
  the free function via `self.client`, and other modules (`scope_
  reconciliation.py`, `sync_execution.py`) call it directly - so the
  correct patch target had to be traced per call site, not assumed from
  the test file's name. `test_sync.py`'s existing `ForwardSyncRunnerTest.
  setUp` (already patching 6a/6b functions) simply grew two more names in
  its existing per-module tuples, needing zero new structure.
  Full targeted suite (22 files, 1015 tests) green.
