# Expose Failure Reasons

## Goal

Make a failed sync say what failed and why, at the three levels that currently
destroy it: capture, persistence and render. A support engineer reading a
bundle should be able to name the failing models and the reason for each,
without a server-side log and without a round trip.

## Contract

- Every `_safe_exception_summary` call site is strictly more informative than
  before, and none of them discloses anything it did not disclose before.
- Every failing model records its model string, exception class and a
  non-empty reason slug, and those reach the support bundle.
- A message carrying device-name-shaped, hostname-shaped, address-shaped,
  interface-name-shaped or tag-shaped tokens leaves none of them in any
  persisted or rendered output.
- The `OwnershipConflictError` slug behaviour is unchanged.
- No log message promises a record that is never written.

## Constraints

- Persisted and exported diagnostics stay free of customer data. Forward
  exception messages embed device names, addresses, hostnames and tenant
  labels; that constraint is the reason the redaction exists and it does not
  change here.
- `diagnostics` stays import-free beyond the standard library. It is the module
  every failure path formats through, and a cycle there is unfixable.
- No fourth bespoke exemption. There have been three - `OwnershipConflictError`,
  the ValidationError rule catalogue, and this - and each was written as a
  special case for one exception class.

## Touched Surfaces

- `forward_netbox/utilities/diagnostics.py` - reason catalogue,
  `failure_reason`, `safe_exception_summary`, `redacted_message_prefix`,
  `safe_failure_log_message`, `model_failure_summary`, the
  `safe_job_error_summary` pattern, `structured_failure_diagnosis`
  supplied-diagnosis merge, `describe_failure` failing-model rendering, the
  `SAFE_FAILURE_LOG_MESSAGE` wording.
- `forward_netbox/utilities/query_fetch_execution.py` -
  `_safe_exception_summary` delegates; `ForwardModelResult` gains
  `failure_exception` / `failure_reason`; `_failure_result` populates them.
- `forward_netbox/utilities/single_branch_executor.py` - the wholesale-fetch
  `SyncError` carries a structured `safe_diagnosis`.
- `forward_netbox/views.py` - `model_failures` in the support bundle;
  failure fields in the dependency-preview per-model summary.
- `forward_netbox/tests/test_failed_run_model_evidence.py` - new.
- `forward_netbox/tests/test_sync.py` - one pinned message now carries its
  reason slug.
- `scripts/tests/test_failure_reason_exposure.py` - new.
- This plan.

## Approach

**Capture.** `_safe_exception_summary` returned `f"{exc.__class__.__name__}."`.
That is not redaction, it is deletion: the reason never reached the logger, so
no downstream tool could recover it at any cost. It now delegates to a shared
`safe_exception_summary`, which resolves an allowlisted reason slug and falls
back to leading wording with every value-bearing token dropped.

The rule, not an exemption. A reason is resolved by, in order: the ownership
catalogue (unchanged), the HTTP status recovered structurally from the raised
cause or as an `http-<code>` slug, and a needle-to-slug catalogue covering
timeout / auth / connectivity / parse / shape / budget / license and the
plugin's own contract refusals. Where none matches, `redacted_message_prefix`
keeps the wording *up to the first value-bearing token* and no further. That is
deliberately stricter than the existing `redacted_message_shape`, which keeps
whole wording: the messages that one serves are Django validation strings whose
vocabulary is knowable, and an exception message can be an arbitrary Forward
response body.

The needle catalogue was audited against every exception constructed anywhere
in the plugin (an AST sweep of every `*Error(...)` / `*Exception(...)` call with
a literal or f-string first argument), and the loose needles that produced
wrong or doubled slugs were tightened until no message resolved to a reason
that was not its own.

**Persist.** `safe_save_job_data` writes `logger.log_data` unmodified and
`_build_job_log_entries` copies `entry[4]` verbatim, so text does survive to the
database. Confirmed, unchanged. One latent defect was found and fixed on the way
back out: `safe_job_error_summary`'s pattern accepted only
`... failed (Classifier).` and treated `... failed (Classifier: reason).` as
unparseable, exporting `<redacted diagnostic>` - strictly worse than the bare
classifier. That was already live for `OwnershipConflictError`.

**Per-model evidence.** `ForwardModelResult` recorded *that* a model failed and
never why, so thirty distinct failures presented as thirty copies of one
sentence. It now carries `failure_exception` and `failure_reason`, which reach
the persisted `model_results` and a new `model_failures` block in the support
bundle. The wholesale-fetch `SyncError` additionally carries a structured
`safe_diagnosis`, so the ingestion issue names the failing models: the message
itself is stripped by `safe_operation_failure`, which is right for arbitrary
exception text and wrong for facts the plugin derived itself. Merging a
supplied diagnosis is the general form of what the two earlier exemptions each
got bespoke - and every supplied value is still re-checked against the safe
token pattern here, whatever the raiser believed about it.

**Render.** `_sanitize_log_rows` replaced the message of every failure row with
one fixed sentence, so a run in which thirty models failed for one reason and a
run in which they failed for thirty rendered identically. It now keeps the
classifier and reason the row was given. The recogniser matches only a CamelCase
name ending in a recognised exception suffix, optionally followed by a lowercase
slug of the shape this module's own catalogue emits; a device name, address,
hostname or tenant label matches neither, and everything else in the row is
still dropped.

**The false promise.** `SAFE_FAILURE_LOG_MESSAGE` told operators to "use the job
identifier and exception type for server-side investigation". No such record is
written: the plugin contains no `logger.exception` and passes `exc_info`
nowhere, so every Python-logger call it makes records the exception class and
nothing more - exactly what the row already showed. Adding `exc_info=True` would
make the sentence true on the customer's own server, but a support engineer
reading an exported bundle still could not reach that log, so the advice would
remain misleading in the situation it exists for. The sentence now points at the
ingestion issues and per-model failure evidence, which are written and are
exported.

## Validation

- `scripts/tests/test_failure_reason_exposure.py` - 35 tests, runs without
  Django. Pins the capture summary for a classified failure, an HTTP status
  from both the structured cause and the message, the wording fallback, the
  render-level classifier survival, the persisted job-error readback, the
  per-model summary, the supplied-diagnosis merge, and one sweep asserting that
  no public entry point emits any of six value-bearing token shapes.
- `forward_netbox/tests/test_failed_run_model_evidence.py` - new, pins the
  `ForwardModelResult` failure fields, the differing reasons for differing
  failures, the uncatalogued fallback, and that the bundle summary names every
  failing model. Requires the Django test stack.
- `python3 -m unittest discover -s scripts/tests`: Ran 312 tests, OK.
- `pre-commit run --all-files`: clean on two consecutive passes.
- `python3 scripts/check_harness.py` and `--base origin/main`: pass.
- NOT verified: anything requiring the NetBox test stack, including
  `forward_netbox/tests/test_failed_run_model_evidence.py`, the edited pin in
  `test_sync.py`, and the support-bundle view rendering.

## Rollback

Revert. `_safe_exception_summary` returns to the bare class name, failure rows
return to one fixed sentence, and `ForwardModelResult` loses the two failure
fields. The `safe_job_error_summary` pattern fix should be kept regardless: it
repairs a regression that predates this work.

## Decision Log

- 2026-08-04: Fixed the rule rather than adding a fourth exemption. The three
  existing exemptions each taught one function about one exception class; a
  catalogue plus a bounded wording fallback covers the classes nobody has read
  yet, which is the population that actually matters.
- 2026-08-04: Bounded the wording fallback at the *first* value-bearing token
  rather than masking value-bearing tokens throughout. Masking in place is safe
  for Django validation strings and not safe for a Forward response body, and
  the two share a code path.
- 2026-08-04: Chose to stop making the server-side-investigation promise rather
  than to make it true. See Approach.
- 2026-08-04: Put the failing-model list on the exception as structured data
  rather than trusting its message. The message is stripped for good reason and
  should stay stripped.

## Open

- The wording fallback can still emit a leading alphabetic word that happens to
  be customer data - a single-word tenant label at the very start of an
  otherwise uncatalogued message. The catalogue is the mitigation, and each
  uncatalogued reason seen in the field should become a needle rather than a
  widening of the fallback.
- `redacted_message_shape` (used for unrecognised validation rules) still keeps
  whole wording. It was not changed here because its inputs are a narrower
  population, but the two masking rules should probably converge.
- The reason catalogue is matched against message text. A message reworded by
  a future change silently stops resolving to its slug and falls back to
  `unrecognized-fetch-failure`. That is visible rather than silent, but a test
  that asserts each catalogued raise site still resolves would close it.
