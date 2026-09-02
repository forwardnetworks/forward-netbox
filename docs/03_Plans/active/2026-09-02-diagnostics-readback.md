# Two diagnostics that could not be read back

## Goal

Stop `Job.error` sentences that carry no exception from exporting as
`<redacted diagnostic>`, and stop a reworded message from silently losing its
failure-reason slug.

## Why

Both were recorded as open by `2026-08-04-expose-failure-reasons.md`. The
first is a customer reading their own job page and being told nothing about a
sentence that was safe in full. The second is a slow rot: the catalogue is
matched against message TEXT, so any rewording drops a reason to
`unrecognized-fetch-failure` with nothing to notice it.

## Constraints

- The shape allowlist pins WHOLE wording and constrains its one interpolated
  value to the enum it comes from. A reworded message must stop matching -
  redacted is the safe direction - rather than admit arbitrary text.
- The needle test may only assert against messages the PLUGIN composes.
  Transport needles ("timed out", "connection refused") come from libraries
  whose wording this repository does not control.

## Touched Surfaces

- `forward_netbox/utilities/diagnostics.py` - `_SAFE_JOB_ERROR_SHAPES`,
  `_safe_job_error_shape`, consulted by `safe_job_error_summary` only when no
  classifier was recovered.
- `forward_netbox/tests/test_diagnostics_readback.py`.

## Approach

The needle test reads the plugin's string literals with `ast`, not with a text
search. That is not a detail: its first version reported
`full-execution-not-contractually-safe` and `unsafe-full-contract` as dead
slugs, and both messages were intact - the needles span an implicit string
concatenation (`"... is not " f"contractually safe ({code})."`), which Python
merges into one constant at runtime and a grep does not. A test that reports
two false positives on its first run is a test that would have been silenced.
f-string parts stay separate, because an interpolated value sits between them
at runtime and a needle cannot span one there either.

## Validation

`test_diagnostics_readback.py`: every plugin-authored needle matches a literal
the plugin composes; both readers are asserted non-empty; the implicit-
concatenation case is pinned directly; every sync status survives readback;
the interrupted-merge sentence survives; a reworded sentence and a
customer-shaped value are both redacted; an ordinary classified error is
unchanged. Adjacent: `test_issue_diagnosis`, `test_failed_run_model_evidence`.

## Rollback

Revert. The two sentences redact again and the needles are unasserted.

## Decision Log

- **Shapes, not sentences.** An allowlist of literal sentences would admit
  whatever a future edit put in them; a pattern that constrains the variable
  to `[a-z_]+` cannot carry a device name, an address or a tenant label.
- **The two remaining items in the source plan are left open on purpose.**
  The wording fallback's leading-word risk and the `redacted_message_shape`
  convergence are judgement calls about a masking rule, not gaps in it.

## Open

- Nothing here. Two items remain open in
  `docs/03_Plans/completed/2026-08-04-expose-failure-reasons.md` by decision.
