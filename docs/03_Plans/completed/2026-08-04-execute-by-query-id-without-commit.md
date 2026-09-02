# Run A Query ID Without A Commit

## Goal

When a query map is bound directly to a Forward query ID and no commit is stored
on it, execute it by sending `queryId` alone and let Forward resolve the latest
commit. Stop reading a commit on the operator's behalf.

## Contract

- An ID-bound map with no stored commit plans as eligible and executes. The
  outgoing execution body carries `queryId` and no `commitId` key.
- Nothing on that path calls `GET /nqe/queries/{id}/history`, and no commit is
  synthesised onto the spec.
- An explicitly stored `commit_id` is honoured exactly as before, and is sent as
  that exact commit.
- Diff execution is unchanged. A diff names its own `diff_commit_id`; that
  commit is still required, still hydrated, and still verified.
- A path-bound map is unchanged. A path is not an identity, so a commit found
  through a path is still verified against the shipped source before it runs.
- A query whose org copy has drifted from the bundle fails at execution, as one
  named per-model failure, not as a zeroed sync.

## Constraints

- Forward's full-execution endpoint accepts `queryId` without `commitId`;
  `_commit_id_for_nqe_execution` already drops an empty or `"head"` value and
  `_nqe_async_execution_payload` already omits the key. The payload layer needed
  no change - only the layers above it, which insisted on supplying a commit.
- `run_nqe_diff` is a different shape: it posts to
  `/nqe-diffs/{before}/{after}` for two snapshots and carries the query's commit
  in the body. A diff compares two revisions by definition, so it is not
  comparable to a full run and is left alone.
- `_build_workload_jobs` turns a single map that fails preflight into zero jobs
  for the entire sync. Any refusal on this path is therefore not a skipped map,
  it is an empty run.
- Forward has objected to the plugin's NQE call volume, so validation used the
  two cheapest models available and counted every execution it made.

## Touched Surfaces

- `forward_netbox/utilities/query_registry.py` - new
  `runs_at_forward_latest_commit`; `_finalize_resolved_spec` returns early for
  that binding; `_resolve_unpinned_customer_full_revision` removed;
  `_hydrate_diff_contract_sources` split into a full side and a diff side so the
  diff side survives an unpinned full side.
- `forward_netbox/utilities/query_execution_contract.py` -
  `unresolved_full_commit` no longer applies to a direct-ID binding; new
  `full_unpinned_head` field; new `remote_source_only` full reason code.
- `forward_netbox/utilities/query_fetch_execution.py` - `validate_rows` takes the
  contract and names the map on a shape failure; `_failure_message` names the map
  behind a failed ID-bound execution and classifies a parameter rejection;
  `_report_contract_compatibility_issues` says once, as info, which maps run a
  query the plugin holds no copy of.
- `forward_netbox/tests/test_execute_by_query_id_without_commit.py` - new.
- `forward_netbox/tests/test_query_execution_contract.py`,
  `test_query_registry.py`, `test_query_id_binding_end_to_end.py` - four
  assertions pinned the old requirement, including one named `fails_closed`.
  Each is rewritten to assert the new behaviour with the reason written next to
  it, not relaxed until it passed.
- `.pre-commit-config.yaml` - the new test file joins the reorder-python-imports
  exclusion, which exists because the two formatters do not converge on a module
  docstring followed by a stdlib import.
- This plan.

## Approach

The plugin held two beliefs that are not true together: that a query ID is a
complete binding, and that a commit must be resolved before that binding can
run. The second one was doing all the damage.
`_resolve_unpinned_builtin_full_revision` walked `/nqe/queries/{id}/history`
newest-first and accepted the first commit whose committed source hashed to the
bundled `.nqe`. Every way that walk could come back empty - a reorganised
repository, a permissions gap, one lookup that did not answer - produced
`unresolved_full_commit`, and one such map empties the whole sync. A customer
with 32 of 32 maps in exactly that state lost five consecutive syncs.

So the requirement is removed rather than made more forgiving. There is nothing
on this path for us to resolve: Forward resolves the latest commit for a query
ID server-side, and the request never sends a repository path, so the bound ID
always runs the query the operator chose. This was confirmed against the live
API before the change: the head-pinned request and the commit-less request
returned the same execution key.

**What the commit requirement was actually protecting, and what replaces it.**
It was standing in front of a shape check - that the query still declares the
parameters this plugin injects and still returns the fields the model needs. For
a **built-in** map that check does not need Forward at all: we ship the `.nqe`,
so the source is verified against its own bundled hash, its declaration is
parsed, and `unsupported_full_parameters` still closes the contract. That is
kept in full. Fetch-time row-shape validation is kept too, and now names the map
and whether it ran at unpinned head, because a bare "Row for `dcim.device` is
missing required fields" does not say which of several enabled maps produced it.

**What genuinely cannot survive.** Three things, stated plainly:

1. *Pre-execution detection of org drift.* If the org copy of a built-in query
   has diverged from the bundle, we no longer find that out before running. It
   fails at execution with `NQE_RUNTIME_ERROR`. That is the accepted trade, and
   the failure is now a per-model failure that names the map, the query ID, the
   revision it ran at, and what to do.

   **The residual exposure inside that trade is narrower than "drift", and it is
   the one thing worth arguing about.** The old check refused any revision whose
   source did not hash to the bundle, which covered every kind of divergence.
   Three kinds remain, and only the third is dangerous:

   - A head that declares *different parameters* - the parameterless
     diff/provenance revision the old resolver was written for. Forward refuses
     the execution outright. Loud, per-model, named. Safe.
   - A head that returns *different fields*. `validate_rows` refuses it against
     the model's required fields and coalesce sets. Loud, per-model, named. Safe.
   - A head that is **parameter-compatible and shape-compatible but returns a
     different row set** - most plausibly a narrowed `where` clause. Nothing
     catches this. Its rows are ingested as authoritative, and rows that are now
     absent are reconciled as deletions. This is the case the old source-hash
     check did cover and the new design does not.

     It needs the org copy of a bundled query to be edited to something still
     structurally identical, which is a deliberate act on a query the operator
     or we published. The mitigations that exist are pinning a commit (still
     fully supported and now the meaningful reason to pin) and
     `ForwardDriftPolicy.max_deleted_objects` / `max_deleted_percent`, which cap
     delete volume - but that policy is optional and unset by default, so it is
     not a mitigation anyone can assume is in place.
2. *Source verification for a map bound to a query the plugin does not hold.*
   A non-built-in map bound by ID typically stores no query text, so there is no
   local expectation to check the server's copy against. The previous check was
   circular anyway: it hashed the source it had just fetched and compared it to
   itself. Those maps now report `remote_source_only` - eligible, but visibly
   executed on trust - instead of a verification that verified nothing.
3. *The `identical_full_diff_commit` guard.* It compares the pinned full commit
   with the diff commit; with no full commit pinned it cannot fire. The
   substantive diff check, `nonempty_diff_declarations`, is unaffected.

**Diff was the one place that needed care.** `_hydrate_diff_contract_sources`
returned immediately when there was no full commit, which was correct only
because a full commit was previously guaranteed. Left alone, a map with a diff
commit and an unpinned full side would have silently lost its diff verification
and fallen back to a full sync - a quiet regression hiding inside a fix. The
function is now split: the full side is hydrated only when a commit is pinned,
the diff side is hydrated and verified on its own `diff_commit_id` regardless,
and `diff_eligible` accepts `remote_source_only` as a full-side pass so a working
diff is not demoted.

`_resolve_unpinned_customer_full_revision` is deleted rather than left in place.
After the early return it was unreachable, and it was itself a "go and find a
commit for them" lookup, which is the behaviour being removed.

## Validation

Live, against a real Forward instance and a real NetBox, with every outgoing
HTTP request recorded by wrapping `ForwardClient._request`. The map used was a
built-in `dcim.site` map bound to the org copy of `forward_locations` by query ID
with `commit_id` empty - the customer's exact shape.

**(a) It plans and it fetches.** Contract: `full_eligible=True`,
`full_reason_code="eligible"`, `full_unpinned_head=True`,
`full_revision.commit_id=""`, `full_revision.source_verified=True` (against the
bundled `.nqe`, with no Forward call). Fetched 235 rows.

**(b) The request body has no `commitId` key.** Captured verbatim:

    {"parameters": {"forward_netbox_shard_keys": [], "device_tag_include_tags": [],
     "device_tag_include_match": "any", "device_tag_exclude_tags": []},
     "queryId": "Q_e037…"}

**(c) Nothing calls the history endpoint.** The complete recorded request list
for that run, not a filtered claim:

    GET  /networks/{id}/snapshots/latestProcessed
    GET  /snapshots/{id}/metrics
    POST /networks/{id}/nqe-executions
    GET  /networks/{id}/nqe-executions/{key}/result

**(d) A stored commit is still sent, exactly.** With `commit_id` set, the body
gained `"commitId": "b4f21073…"` and nothing else changed;
`full_unpinned_head=False`; the same 235 rows. Forward returned the **same
execution key** for the pinned and the commit-less request, which is Forward
stating the two are the same execution.

**(e) Diff execution: unchanged, and proven by A/B rather than asserted.** The
same `run_nqe_diff` call was made from this branch and from `main`, and both
produced a byte-identical request (`POST /nqe-diffs/{before}/{after}`, body keys
`commitId`/`options`/`queryId`, same commit) and the byte-identical response
(HTTP 400 `Invalid query`). `forward_api_impl.py` is not in this diff at all.

*Stated plainly: a **successful** diff could not be demonstrated here.* Forward
refuses a diff whose query declares parameters, and every query published to
this org repository declares at least one, so no revision in it is diff-eligible
on either tree. The A/B shows the behaviour is identical before and after; it
does not show a diff returning rows. Contract-level diff outcomes -
`missing_diff_commit`, `nonempty_diff_declarations`, `eligible` with an unpinned
full side, and diff-side hydration - are covered offline instead.

**Drift surfaces per model, and does not empty the run.** Two models enabled,
one deliberately bound to an org query that does not declare its parameters.
Preflight planned **2** jobs (the old behaviour planned 0), both executed, and:

- `dcim.manufacturer`: `failure_count=1`, `row_count=0`, diagnostic
  "ForwardClientError. Map `Forward Device Vendors` [2] is bound to Forward query
  ID `Q_e037…` and ran at Forward's latest commit. Forward rejected the
  parameters this map supplies, so the query behind that ID no longer declares
  them. Re-publish the bundled query to that ID, or rebind the map."
- `dcim.site`: 235 rows, `failure_count=0`, in the same run.
- Zero history calls.

**NQE call volume for the whole validation: 11 executions** - 7 full
(`POST /nqe-executions`) and 4 diff (`POST /nqe-diffs/…`), including abandoned
attempts. Repository and snapshot reads are not NQE executions.

Offline: `forward_netbox/tests/test_execute_by_query_id_without_commit.py`
covers resolution, contract, payload, diff, the path-bound binding, and the
failure messages, including every case that must still be refused.

## Rollback

Revert. An ID-bound map again requires a commit the plugin reads for itself, and
a repository reorganisation or a permissions gap again empties a sync rather
than running the query the operator bound.

## Decision Log

- 2026-08-04: Removed the requirement rather than relaxing it. A fallback still
  leaves a lookup that can fail, and that lookup has no reason to exist.
- 2026-08-04: `"head"` is normalised to no commit. It is not a commit, the
  payload layer already discards it, and keeping it would make reporting claim a
  pin that was never sent.
- 2026-08-04: Path bindings deliberately excluded. A path can come to hold a
  different query, so a commit reached through a path still has to be verified.
- 2026-08-04: Added `remote_source_only` as its own reason code rather than
  folding it into `eligible`, and then printed it - a reason code nothing renders
  is not visibility, and every existing renderer only prints codes for maps that
  were refused.
- 2026-08-04: Kept diff hydration alive when the full side is unpinned. Not
  doing so would have turned a working diff into a full-only sync with no
  message anywhere.

## Open

- ~~The silently-narrowed-head case above has no detection.~~ **Closed by
  `row_shrink_findings`** (`utilities/validation.py`), which is exactly the
  cheap version this described: a per-model row count compared against the
  last baseline, no Forward call, wired into validation. A narrowed head is
  not a scope-configuration change, so it is not skipped by that guard's
  exemption. Confirmed 2026-09-02.
- `forward_netbox/utilities/health.py` still lists a map with no stored commit
  as an `unresolved_full_commit` preflight issue. It is already classified
  non-blocking and its message already says an empty commit is normal, so it is
  not misleading - but with this change it is describing the intended state, and
  `fix/contract-preflight-severity` (PR #138) already rewrites exactly that
  block. Left to #138 rather than changed twice.
- ~~`QuerySpec.resolve()` still resolves a head commit for an ID-only
  binding.~~ **Brought in line 2026-09-02.** The ID-only BRANCH is retired -
  it returns the spec unchanged - along with `_resolve_head_commit_for_query_id`
  and `test_query_spec_head_resolution.py`. It had no production caller and
  encoded the behaviour this plan removed the need for; grafting a head commit
  onto an ID-only binding is also the shape that runs a revision the operator
  never pinned.

  The METHOD stays. A first pass removed it wholesale on the grounds that only
  tests called it, which was true and incomplete: a second test in
  `test_query_registry.py` covers its path-resolution branch, which is
  useful behaviour and not what this item objected to. Recorded because the
  first assessment was wrong in the direction that deletes working code.

  The client's `resolve_nqe_query_head_commit` is left in place: it mirrors a
  Forward API endpoint and is part of that surface, not internal plumbing.
- `fix/contract-preflight-severity` (PR #138) is not on `main` and is not
  included here. It made the commit-less case *eligible* as a fallback and
  changed Health reporting. This branch removes the need for that fallback, so
  the two overlap in `query_execution_contract.py` and `query_fetch_execution.py`
  and will conflict; whichever lands second should keep this branch's early
  return in `_finalize_resolved_spec` and #138's Health reporting.
- `fix/expose-failure-reasons` is likewise not on `main`. It restores real reason
  text to `_safe_exception_summary`, which is what makes the drift failure fully
  readable; until it lands, the per-model diagnostic here carries the map
  identity and a fixed classification sentence but not Forward's own words.
