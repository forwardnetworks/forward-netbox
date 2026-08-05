# A Row-Count Floor For A Silently Narrowed Query

## Goal

A query head that returns fewer rows than it used to, while still declaring this
plugin's parameters and still returning the fields the model needs, is refused
before anything is staged - named by model, with both counts and the way through
- instead of ingesting as authoritative and reconciling the missing rows as
deletions.

## Contract

- Each enabled model's fetched row count is compared against the same model's
  count in the last ingestion that promoted a baseline (`baseline_ready=True`).
- A drop of more than the threshold **and** at least `MIN_ROW_SHRINK_ROWS` rows
  blocks the run. Both conditions are required.
- The guard is on by default, including for a sync with no drift policy at all.
- A first run, a sync that has never promoted a baseline, a newly enabled model,
  growth, and a model whose baseline had no rows are never blocked.
- A model is compared only when both runs executed it in full with no reported
  failure. A diff run's `row_count` is a count of changed rows, so it is not
  comparable and is skipped on either side.
- A change to the sync's own scope configuration suspends the comparison for
  that model.
- The block names the model, both counts, the drop, the limit, and the override.
- The operator's way through is `force_allow` on the blocked validation run -
  the existing attributed, audited override - and it is scoped to the baseline,
  so it survives a snapshot roll and lapses on its own once a new baseline is
  promoted.
- Needs no Forward call.

## Constraints

- `fix/execute-by-id-without-commit` (45cf983) is the parent. This guard is the
  precondition for that branch shipping and is stacked on it, not beside it.
- False positives are the governing risk. Legitimate shrinkage happens -
  decommissioning, a retired site, a deliberately narrowed scope, a Forward
  collection gap - and a guard that blocks those routinely gets disabled, which
  is strictly worse than not having one.
- `ForwardDriftPolicy.max_deleted_objects` / `max_deleted_percent` are optional
  and unset by default. The whole failure of that shape as a mitigation is that
  nobody sets it, so the new configuration must not repeat it.
- A migration adds two fields with defaults that apply to existing policies. No
  data migration, no backfill.
- The comparison must run before the branch is provisioned and before any row is
  staged, or refusing costs a branch teardown.

## Touched Surfaces

- `forward_netbox/utilities/validation.py` - `DEFAULT_MAX_ROW_SHRINK_PERCENT`,
  `MIN_ROW_SHRINK_ROWS`, `ROW_SHRINK_REASON_PREFIX`, `_comparable_row_counts`,
  `_scope_configuration_changed`, `row_shrink_findings`, `row_shrink_reason`;
  `ForwardValidationRunner._row_shrink_reasons`,
  `._row_shrink_already_accepted`; `_blocking_reasons` gains a keyword-only
  `validation_run` and calls the new check above the policy early return.
- `forward_netbox/models.py` - `ForwardDriftPolicy.block_on_row_shrink`,
  `.max_row_shrink_percent`.
- `forward_netbox/migrations/0049_drift_policy_row_shrink_floor.py` - new.
- `forward_netbox/forms.py`, `tables.py`, `api/serializers.py`,
  `templates/forward_netbox/forwarddriftpolicy.html` - the two fields on the
  form, the table, the API, and the detail page.
- `forward_netbox/tests/test_row_count_floor.py` - new.
- This plan.

## Approach

**Where it runs.** `ForwardValidationRunner.record_plan_validation` is already
the gate: `ForwardSingleBranchExecutor.run` calls it immediately after
`fetch_workloads`, with every model's fetched rows in hand, before the no-op
check, before the fast-baseline decision, before the ingestion row exists, and
before a branch is provisioned. It already records a `ForwardValidationRun`,
already raises `ForwardSyncError` on a blocking reason, and already renders
those reasons in the UI and the API with a Force-Allow button next to them. The
guard is one more blocking reason in `_blocking_reasons`. No new gate, no new
raise site, no new reporting surface.

**What it compares.** `ForwardIngestion.model_results` is the persisted
per-`(model, map)` fetch record - `model`, `row_count`, `sync_mode`,
`failure_count`, `scope_config_fingerprint` - and
`ForwardSync.latest_baseline_ingestion()` is the canonical "last successful run"
lookup. Counts are summed per model across every map, because a model can be fed
by several maps and one of them dropping out is exactly the thing to catch.

**What makes a comparison legitimate.** Four exclusions, each of which turns a
would-be false positive into silence:

1. *Failures.* A failed fetch returns zero rows. That is already a loud,
   separately blocking failure; counting it here as a 100% collapse would bury
   the real cause under a derived one.
2. *Diff runs.* A diff `row_count` is the number of changed rows, not the size
   of the row set. Comparing a diff against a full run reads a quiet snapshot as
   a collapse. Either side being non-full skips the model.
3. *Scope changes.* `scope_config_fingerprint` covers the sync's declared scope -
   include and exclude tags, the match mode, out-of-scope pruning, the endpoint
   and device-tag toggles - and nothing derived from the snapshot. When it moves,
   a smaller row set is the operator getting what they asked for. Membership
   fingerprints are deliberately not consulted: they move whenever the network
   moves, which is the thing being measured. An empty fingerprint on either side
   is unknown, not evidence of a change, so the comparison still stands.
4. *No baseline for the model.* First run, never-promoted sync, newly enabled
   model, or a baseline of zero rows. Nothing to measure against.

**The threshold, and why it is loose.** `DEFAULT_MAX_ROW_SHRINK_PERCENT = 30`,
with `MIN_ROW_SHRINK_ROWS = 20` as an absolute floor, both required.

The estate this was written for has 3,403 in-scope devices behind 32 org-bound
maps on a 44,517-device snapshot. On an estate that size the routine causes of
shrinkage are small in proportion: a decommissioned rack is around forty devices,
about 1%; the largest Forward collection gap seen on this deployment was 72
devices, about 2%; even a retired site is usually well under a third of a
multi-site estate. The failure being guarded against has a different shape
entirely - a narrowed `where` clause, a query rebound to one site, an emptied
collection region - and lands as a step change of tens of percent up to a total
collapse. 30% sits above the first band and below the second.

It is deliberately not tight. The guard's job is to stop a silent catastrophe,
not to audit ordinary churn; that is what per-model reporting and the staged
branch are for. A tighter default would fire on ordinary weeks, and a guard that
fires on ordinary weeks is a guard that gets switched off.

`MIN_ROW_SHRINK_ROWS` exists because percentages are meaningless on small
reference models. A `dcim.manufacturer` map going from twelve rows to eight is a
33% drop and says nothing. Below twenty rows lost, the blast radius is small
enough that the existing per-model reporting and the pre-merge branch review are
the right instruments.

**Configuration, and why it is not another optional field.** Two fields on
`ForwardDriftPolicy`: `block_on_row_shrink = True` and
`max_row_shrink_percent = 30`. Both are non-null with concrete defaults, so they
match `block_on_query_errors` rather than `max_deleted_objects`, and the
migration turns the guard on for policies that already exist.

The check is called *above* the `policy is None or not policy.enabled` early
return, so a sync with no drift policy is guarded on the module defaults. This is
the point of the exercise: the check replaces a source-hash comparison that ran
on every sync whether or not anyone had configured anything, and it would not be
a replacement if it needed to be turned on first. A policy that exists can widen
the threshold or switch it off; the absence of a policy cannot. A policy that is
explicitly `enabled=False`, or whose `baseline_mode` is `none`, does switch it
off - both are the operator saying so in as many words.

**The override, and why it is scoped to the baseline.** `force_allow` on the
blocked run already exists, is already attributed to a user with a typed reason
and a timestamp, already records what it overrode in
`override_blocking_reasons`, and already has a UI button and an API endpoint. It
is the same posture as `forward_accept_merge_failures` - typed, attributed,
auditable, and never a silent default - so it is reused rather than reinvented.

The acceptance is matched on the **baseline**, not the snapshot. What the
operator accepted is "smaller than baseline N", and that stays true for as long
as N is the baseline. A snapshot-scoped acceptance would lapse the moment
Forward processed the next snapshot, putting the operator in a loop they would
escape by disabling the guard - the exact outcome to avoid. It then clears
itself: once an accepted run gets through and promotes a new, smaller baseline,
the acceptance no longer matches and the floor is live again against the count
the operator signed off. So the steady state after a genuine site retirement is
one force-allow, not a permanently relaxed sync.

`_forced_validation_override_applies` is not reused for this. It reads
`sync.latest_validation_run`, which by the time `_blocking_reasons` runs is the
run currently being recorded, so it cannot see the previous run's override. That
is a separate latent problem, recorded under Open rather than fixed here.

## Validation

- `forward_netbox/tests/test_row_count_floor.py`, three classes:
  - `RowShrinkFindingsTest` - the arithmetic with no database: past the
    threshold, within it, exactly on it, growth, a drop too small in absolute
    terms, the absolute floor at its exact boundary, no baseline for the model,
    a zero baseline, a failed fetch, a diff run on either side, summing across
    maps, a changed scope fingerprint, an unknown scope fingerprint, and a model
    not enabled on this sync.
  - `RowShrinkBlockingTest` - the same through `_blocking_reasons`: blocks and
    names the model with both counts and the override; within threshold
    proceeds; growth proceeds; a first run proceeds; a sync with ingestions but
    no baseline proceeds; on by default with no policy; on by default for a
    freshly created policy; a policy can widen it; a policy can switch it off; a
    disabled policy switches it off; `baseline_mode=none` switches it off.
  - `RowShrinkOverrideTest` - force-allow lets the next run through; the
    acceptance survives a new snapshot; it lapses once a new baseline is
    promoted, and a further collapse below the new count blocks again; an
    override of some other reason does not clear the floor; the run being
    recorded cannot clear itself.
- `pre-commit run --all-files`, twice.
- `python3 -m unittest discover -s scripts/tests`.
- `python3 scripts/check_harness.py` and `--base origin/main`.
- The full Django suite in an isolated Compose project, torn down afterwards.
- No live Forward calls. The comparison is local logic against persisted
  ingestion evidence and cannot reach the API.

## Rollback

Revert the commit and run `migrate forward_netbox 0048`. The two policy fields
are additive with defaults and nothing else reads them, so no state needs
cleaning up. A narrowed query head then goes back to being undetectable, which
is the state `fix/execute-by-id-without-commit` records as its open exposure.

## Decision Log

- 2026-08-04: A row-count floor rather than restoring a source check. The
  parent branch removed the commit lookup because it could fail for reasons
  unrelated to drift and emptied whole syncs when it did. Anything that goes
  back to Forward to ask what the query says can fail the same way. Counting the
  rows we already have cannot.
- 2026-08-04: Built into `ForwardValidationRunner._blocking_reasons` rather than
  as new machinery. That gate already runs at the only point where every model's
  rows are in hand and nothing has been staged, and it already carries the
  reporting, the persistence, and the override this needs. A parallel guard
  would have duplicated all four.
- 2026-08-04: Above the policy early return, not below it. Below it, the guard
  would be off for every sync without a drift policy, which is the same failure
  as `max_deleted_objects` being optional - a mitigation nobody can assume is in
  place.
- 2026-08-04: Two new non-null policy fields rather than one nullable
  "threshold, or off if unset". A nullable field conflates "not configured" with
  "disabled" and makes the default invisible in the UI. A boolean plus a
  concrete percentage shows an operator exactly what is running.
- 2026-08-04: Threshold 30%, with a 20-row absolute floor, both required. The
  percentage catches proportional collapse; the absolute floor stops small
  reference models tripping on arithmetic. Reasoning above under Approach.
- 2026-08-04: Compared per model, not in aggregate. `max_deleted_percent` sums
  every model together, so one model collapsing hides inside the totals of the
  others, and the resulting message can name nothing. The failure this guards
  against is per query, so the measurement is per model and so is the message.
- 2026-08-04: `scope_config_fingerprint` used as the suspension signal;
  `map_set_fingerprint` and `scope_membership_fingerprint` rejected.
  `map_set_fingerprint` embeds each contract's effective parameters, including
  shard keys, so it is not a stable statement about what the operator
  configured. `scope_membership_fingerprint` is the resolved device and site
  list, which changes whenever the network changes - suspending on it would
  suspend the guard exactly when it is needed.
- 2026-08-04: The acceptance is scoped to the baseline rather than to the
  `(policy, snapshot_selector, snapshot_id)` triple the existing override check
  uses. Reasoning above under Approach.
- 2026-08-04: A model that reported any failure is skipped rather than counted
  as a collapse to zero. Failures already block through
  `block_on_query_errors` and `_required_query_failure_reasons`, and a second
  derived reason would compete with the real one for the operator's attention.
- 2026-08-04: `ForwardWorkloadState.row_count` rejected as the comparison basis
  even though it is a typed, indexed, per-`(sync, model)` column promoted at
  baseline. It is written only for parameterized full workloads, so it covers
  some models and not others. `model_results` covers every model that ran.

## Open

- `_forced_validation_override_applies` cannot fire on the sync path.
  `record_plan_validation` creates the new `ForwardValidationRun` before
  `_blocking_reasons` runs, so `sync.latest_validation_run` returns the run being
  recorded, whose `override_applied` is always `False`. Its only coverage calls
  `_blocking_reasons` directly with no current run, so the tests pass and the
  path is dead. Left alone here - the row-count floor does its own previous-run
  lookup and does not depend on it - but every other blocking reason is affected
  and force-allow does not carry forward for any of them.
- A drop that is large in absolute terms but under 30% is not caught. On a
  3,403-device model that is up to a thousand rows. The floor is a backstop
  against collapse, not an audit; `max_deleted_objects` remains the instrument
  for a hard ceiling on delete volume, and the per-model drift summary already
  reports the counts.
- The threshold is a single number for every model. A reference model and
  `dcim.device` have very different natural volatility, and a per-model
  threshold would be more precise. Not built: there is no evidence yet on what
  those numbers should be, and a per-model table is a configuration surface that
  would go unfilled.
- Nothing yet reports how close a run came to the floor. A run at 29% passes
  silently, so an estate drifting toward the limit gets no warning before the
  first block. The counts are in `ForwardValidationRun.drift_summary`; surfacing
  the margin would be a reporting change, not a behavioural one.
