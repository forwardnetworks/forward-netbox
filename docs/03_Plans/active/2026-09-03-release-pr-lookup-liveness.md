# Release PR lookup: two liveness questions, not one

## Goal

Make the release flow ask two distinct questions about a pull request instead
of one, so neither half of a release stops on a false answer: a merge that just
landed must not read as missing, and a same-named merge from an earlier attempt
must not read as this branch having shipped.

## Constraints

- `_merge_is_live`'s dead-history verdict from #351 must survive unchanged; it
  guards a different failure (a pull request merged into rewritten-away
  history) and is pinned by its own test.
- No extra network round trip on the common path.
- Read-only git queries only. The release flow already decides what to push;
  these helpers only answer questions.

## Touched Surfaces

- `scripts/release.py` - `_merge_is_live` (fetch retry), new `_head_is_merged`,
  `_open_release_pull_request` (short-circuit condition).
- `scripts/tests/test_release.py` - four cases in
  `PullRequestLookupIgnoresDeadHistoryTest`.

No plugin code, no migrations, no runtime behaviour.

## Context

Both halves of the v3.0.0 release stopped on the same helper asking the wrong
question about a pull request. Neither was a product defect; both reported
success or refusal on bookkeeping and cost a release stage.

**Stale remote refs read as a missing pull request.** `_merge_is_live`
(added in #351) decides whether a merged pull request is still reachable from
`origin/main`, and it reads local refs. The production merge this very script
queues lands seconds before the check runs, so the remote-tracking ref has not
caught up and the merge looks absent. `stage_finish` then reported

    stopped at merge (production): no pull request exists for release/3.0.0

with the merge already on `main` as `6e6c1fb`. The check that exists to catch a
pull request merged into rewritten-away history was rejecting a live one.

**A same-named pull request from an earlier attempt read as completion.**
`_open_release_pull_request` treated any `MERGED` pull request on the branch
name as proof the work had shipped. After the first v3.0.0 tag was refused and
the authorization regenerated, the evidence branch kept its name, so the lookup
found the *first* attempt's pull request - merged, live, and irrelevant -
printed `release PR already merged` and returned success with the new
authorization commit still unpushed. `main` was left carrying no authorization
section at all, and the next tag attempt would have failed on it.

## Approach

The two are the same conflation: *is this pull request's merge still real* is
not *has the work in hand shipped*.

- `_merge_is_live` keeps its meaning and gains a retry: a negative is only
  trusted after `git fetch origin main`. A dead-history merge stays dead across
  that fetch, which is pinned separately so the retry cannot quietly turn the
  #351 verdict back into a pass.
- `_head_is_merged` is the second question, asked directly: is *this branch's
  current head* an ancestor of `origin/main`. `_open_release_pull_request`
  short-circuits only when it is; otherwise it says which earlier attempt it
  found and opens the pull request this branch needs.

## Validation

`scripts/tests/test_release.py::PullRequestLookupIgnoresDeadHistoryTest`:

- a merge that just landed is live after the retry, and exactly one fetch is
  issued;
- a genuinely dead merge stays dead across that fetch;
- a merged pull request from an earlier attempt opens a new one rather than
  returning early;
- a merged pull request for *this* head still returns early, so a shipped
  release is never re-opened.

Each of the two new failing cases fails against the code as it shipped in
v3.0.0.

## Not here

The release flow's other v3.0.0 costs - the plan file that must ride in
prepare's diff, and stale local and remote release branches - are handled in
the launcher rather than the script, and are not touched by this change.

## Rollback

Revert the commit. Both helpers are read-only git queries used only by the
release flow, so reverting restores the v3.0.0 behaviour exactly: the
production-merge race returns, and a same-named merged pull request again
short-circuits the push. Nothing in the plugin imports either function, and no
data or schema is touched.

## Decision Log

- **Retry rather than always fetch.** `_merge_is_live` runs per candidate pull
  request; fetching unconditionally would add a network round trip to every
  call to fix a race that only shows on a negative. Only a negative is retried.
- **Keep the two questions separate rather than widening `_merge_is_live`.**
  Making it answer "has this branch shipped" would have silently changed the
  #351 dead-history verdict, which exists for a different failure and is
  pinned by its own test.
- **Report the earlier attempt instead of failing.** `_open_release_pull_request`
  names the pull request it found and continues, because an operator resuming a
  refused release needs to know a same-named one exists; refusing there would
  have stopped the v3.0.0 recovery outright.
- **A history rewrite was rejected for the customer-name finding** that refused
  the first tag; see `2026-09-02-release-3.0.0.md`. Not this change's subject,
  recorded because the two failures were interleaved in one session.
