# Stop two checks reporting confidently about things they did not measure

## Goal

Make `release.py --finish` report the release it actually performed, and make
the bulk-merge scale gate refuse to answer instead of answering wrongly.

## Why

Both misreport in the same direction - a confident verdict from something never
validly measured - and both cost time during 2.8.2.

`--finish` exited 1 after a **completely successful** release: tag created,
workflow green, identical PyPI and GitHub artifacts published. The failure came
from `stage_post_release`, which runs `check_harness.py`, which requires the
provenance anchor to name the release the table calls current. The anchor
cannot advance until the bridge commit exists; the bridge commit cannot exist
until the pull request that step is opening has merged. The check is
**unsatisfiable by construction** at the moment it runs, so it has failed on
every release since 2.7.13 and been discarded by hand each time. An exit status
that says "failed" while the artifacts are live teaches people to stop reading
it - and this is the command where reading it matters most.

`test_bulk_merge_scale` asserts a wall-clock projection with no load
normalisation. During the 2.8.2 follow-up work a kernel compile on the host - 32
cores at load 98 - turned a 1662s projection into 4382s and failed the gate.
The same revision on the same runtime passed minutes later. Measured back to
back on a quiet host: 1662s on NetBox 4.6.6 and 1734s on 4.6.8, a 4% difference
against a 3600s budget. The gate reported a regression that did not exist.

## Constraints

- Neither check may become more permissive about what it actually checks. The
  release still fails for a failed release; the scale budget is unchanged.
- The post-release follow-up must not become easy to forget. The harness
  already fails while the anchor is stale, and that stays true.
- The `.dev0` policy question is NOT settled here. Whether `main` should carry
  a `.dev0` marker after a release is a product decision with a customer report
  cited on both sides; this change only stops that question deciding whether a
  release is reported as successful.

## Touched Surfaces

- `scripts/release.py` - the `stage_post_release` call site
- `forward_netbox/tests/test_bulk_merge_scale.py` - `MAX_LOAD_PER_CORE`,
  `_load_per_core`, and the projection assertion

## Approach

The release command wraps post-release staging and reports it as follow-up
work, printing the manual steps. The release either published or it did not,
and staging cannot change which.

The scale test reads one-minute load average per core and skips - not passes,
not fails - above 1.5. Skipping is the correct outcome for an unmeasurable
performance gate: an operator reading "skipped: host load per core is 3.06" is
told exactly what to do, where a failure sends them hunting a regression that
is not there. The threshold allows normal background noise while catching a
parallel build.

## Validation

- `invoke harness-test`, which covers `scripts/release.py`.
- The scale test on an idle host, confirming it still measures and still
  asserts.
- Full suite.

## Rollback

Revert. The release command misreports again and the scale gate flakes on busy
hosts again.

## Decision Log

- **Skip, do not normalise.** Dividing the projection by observed load would
  invent a model of how contention scales and quietly mask real regressions.
  Refusing to answer is honest and needs no model.
- **Skip, do not raise the budget.** The budget is a real constraint about a
  worker timeout. Loosening it to accommodate a busy laptop would weaken the
  gate permanently to fix a measurement problem.
- **Wrap the call site, do not delete the step.** Removing `.dev0` staging
  would decide a policy question that is not mine, and the step is useful on
  the releases where it can run.
- **Do not "fix" the unsatisfiable harness call inside the step.** Making it
  pass would require running it against a tree state that does not exist yet.
  Reporting it as deferred follow-up is the accurate description.

## Open

- The `.dev0` policy: `stage_open_next`'s docstring cites a customer who could
  not tell an install from `main` apart from the published release, while the
  practice since 2.7.13 has been that `main` stays at the released version
  because customers install from source. Both cannot be right. Worth one
  decision, recorded once.
