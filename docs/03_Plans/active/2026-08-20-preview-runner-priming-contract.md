# The preview lost a runner method the priming needed, and said nothing useful

## Goal

Restore the dependency preview for every deployment with an optional plugin
installed, and make this class of failure impossible to ship again.

## Why

2.8.7 made `compare_model_rows` prime its dependency caches. That was the right
fix - it took the comparison from cost-per-row to a constant - and it shipped
with a test that only exercised `dcim.interface`.

`dcim.interface` priming touches only caches `PreviewRunner` already seeded. The
routing primers do not: they ask the runner for `BGPRouter`, `BGPScope`,
`OSPFInstance` and `OSPFArea` through `_optional_model`, which the real runner
defines and `PreviewRunner` never did. Every deployment with `netbox_routing`
installed then lost its ENTIRE dependency preview to:

    AttributeError: 'PreviewRunner' object has no attribute '_optional_model'

The preview is one job over all models, so one missing attribute on one model's
path takes down the measurement for every model, not just that one.

The gate did not catch it and was not weak: 2,276 Django tests, a full upgrade
leg and an artifact test all passed. The test estate has `netbox_routing`
installed, but nothing in the suite ran a PREVIEW over a routing model. The new
code opened a path nothing exercised.

Second defect, found while tracing the first and arguably worse: the preview's
failure handler logged only `SyncError` and `JobTimeoutException` - the two
types it anticipated - and without `exc_info`. So an UNANTICIPATED exception
wrote nothing to the server log at all, and the job carried no traceback and no
frame. The condition was backwards; the failures worth logging are the ones
that were not predicted. A deployment saw a bare `AttributeError` with nowhere
to look, which is the exact opacity the 2.8.6 raise-site work removed from sync
and merge failures. This path was never wired into it.

## Constraints

- The delegate must behave as the real runner does. `optional_model` RAISES
  `ForwardQueryError` for an absent plugin rather than returning `None`, and
  `_prime_optional_dependency_cache` catches exactly that and returns `{}`. A
  preview that were quietly more forgiving would hand a `None` model to a
  primer expecting a class.
- No traceback may reach an ingestion issue's `raw_data`, which exports
  verbatim. `job.data["traceback"]` is redacted wholesale on export by
  `sanitize_job_diagnostics`, so it is readable in NetBox and never travels.
- The new test must not depend on which models the author thought of. That
  dependency is the whole defect.

## Touched Surfaces

- `forward_netbox/utilities/drift_comparison.py` - `_optional_model` on
  `PreviewRunner`
- `forward_netbox/jobs.py` - `_dependency_preview_work` failure handler
- `forward_netbox/tests/test_preview_runner_satisfies_the_priming_contract.py`
  (new)
- `.pre-commit-config.yaml` - reorder-imports exclusion for the new module

## Approach

`PreviewRunner._optional_model` delegates to `sync_primitives.optional_model`,
the same primitive the real runner uses.

The regression test is STRUCTURAL rather than per-model: it scans
`sync_primitives` for every `runner._x` read and asserts `PreviewRunner`
answers all of them. A companion sweep offers every model in
`MODEL_SYNC_CONTRACTS` to `compare_model_rows` and fails on any exception.
Neither needs to know which models exist.

The preview failure handler appends the innermost in-package frame via
`with_raise_site`, stores the traceback under `job.data["traceback"]`, and logs
every failure - with `exc_info` for the unexpected ones.

## Validation

Negative control, run with the method removed at runtime so no tree mutation
could contaminate it:

| | missing attributes | routing preview |
|---|---|---|
| without fix | `['_optional_model']` | `AttributeError: 'PreviewRunner' object has no attribute '_optional_model'` |
| with fix | `[]` | returns cleanly |

That AttributeError string is character-for-character what the deployment's job
reported, so this is the reported failure reproduced and closed rather than a
plausible substitute.

Full Django suite: 2280 tests OK, 4 skipped.

## Rollback

Revert. The preview returns to failing on deployments with optional plugins, so
the safe direction is forward; the diagnostic half is observational and reverting
it changes nothing about what runs.

## Decision Log

- **Structural test, not another per-model one.** A per-model test cannot close
  a gap that consists of the models nobody enumerated.
- **The delegate raises rather than returning None**, matching the real runner.
  My first version of the test asserted `None`; the test failure caught the
  wrong assumption before it became a second defect.
- **Invert the logging condition rather than add a branch.** "Log the ones we
  expected" is precisely backwards.
- **Patch, not minor.** A regression fix and a diagnostic addition; no operator
  control changes.

## Open

- One missing attribute killing the whole preview rather than one model's row
  is a blast-radius question worth revisiting on its own. Per-model isolation
  in the preview loop would have turned this outage into a single "Not
  measured" cell.
- The deployment's comparison-cost after-number for 2.8.7 is still unobserved,
  because this defect is what prevented the preview from running.
