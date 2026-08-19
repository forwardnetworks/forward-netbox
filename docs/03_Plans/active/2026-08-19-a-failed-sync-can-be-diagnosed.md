# Two reasons a failed sync could not be diagnosed

## Goal

Make the next failure of this kind name itself, and stop the bundle inventing
failures that did not happen.

## Why

A deployment's sync failed after 36 minutes of staging. Its support bundle said
this and nothing more:

    phase: sync   model: ""   exception: KeyError
    message: "Forward ingestion failed (KeyError)."
    job error: "<redacted diagnostic>"
    model_failures: []

Nothing narrowed it further. The failure is deterministic, so every retry costs
another 36 minutes and produces the same nothing. 22,026 staged creates were
discarded.

Two separate defects, and the second is why the first was hard to find.

### A KeyError is the one exception whose key IS the diagnostic

`failure_reason` returns allowlisted slugs matched against the exception text.
A `KeyError`'s text is just the key, so no rule matches and the classifier
degrades to the bare class name.

The key is also the token most likely to be customer data - a device name is a
plausible dict key - so redacting it wholesale was defensible. But it threw away
the only fact that identifies the bug.

The distinction needed is not "is this a string" but "did this repository choose
this name". `MODEL_SYNC_CONTRACTS` is exactly that set: its field names and its
model labels are vocabulary this code wrote. A key drawn from it is named; a key
that is not stays redacted, which keeps a device-name key silent even though it
is the one that would be most useful.

### The bundle reported thirty failures that never happened

`_FAILURE_LEVELS` includes `warning`, because a warning body can quote a
customer-named query and must be redacted like any other. But the replacement
sentence asserted `The operation failed.`, which is a different claim from
"this text was redacted".

The bundle showed thirty of them seconds into the run, one per model, each
directing the reader to ingestion issues and per-model failure evidence that
were empty - because nothing had failed. They were this notice:

    Execution contract preflight found N map(s) for <model> that cannot run a
    diff; this model still syncs ...

A row that says the model still syncs was presented as a failed operation. The
run's real defect was 36 minutes later and somewhere else, and thirty invented
failures is what had to be read past to reach it. That is not a cosmetic
complaint: it is the reason this diagnosis took as long as it did.

## Constraints

- The redaction does not relax. A warning body is still replaced wholesale, and
  a key outside the contract vocabulary is still withheld.
- A reason slug must survive `recovered_classifiers`, which accepts lowercase
  hyphenated tokens only. A reason that cannot be read back is discarded by the
  log renderer and the bundle - which is how this class of fix was undone once
  before.
- The warning sentence must not contain `SAFE_FAILURE_LOG_PREFIX`. A support
  engineer greps for that phrase, and a warning that spells it out even to deny
  it is counted again by the search.

## Touched Surfaces

- `forward_netbox/utilities/diagnostics.py` - `missing_key_reason`,
  `_schema_field_names`, `SAFE_WARNING_LOG_MESSAGE`, `safe_log_message`
- `forward_netbox/tests/test_keyerror_names_its_key.py` (new)
- `forward_netbox/tests/test_a_warning_is_not_a_failure.py` (new)

## Approach

`failure_reason` answers from the key for a `KeyError` and from nothing else -
the generic needle rules can only match a key by coincidence. The safe set is
derived from the contracts rather than hand-copied, so it cannot drift from
them. `.` and `_` both normalise to `-` so the slug reads back.

`safe_log_message` picks its sentence from the row's level. `warning` gets a
sentence that says a warning was recorded and its detail redacted;
critical/error/failure are unchanged. `safe_failure_log_message` is kept as-is
for its existing callers.

## Validation

`test_keyerror_names_its_key.py` (13) pins both directions, and the redaction
half harder than the naming half: `status`, `address`, `mac_address` and
`ipam.ipaddress` are named; a hostname, an IP, a tag name, a non-string key, a
multi-argument `KeyError` and an undeclared field are not. One test asserts
every nameable key is declared by some contract.

`test_a_warning_is_not_a_failure.py` (6) pins that a warning does not claim
failure, that an error still does, that the warning body is still redacted, that
a classified warning keeps its classifier, and that both list and dict row
shapes behave the same.

`test_log_export.py` covers the layer the operator actually reads: the row it
writes is a warning, so it now asserts the warning sentence renders AND that
`SAFE_FAILURE_LOG_PREFIX` does not appear. Its two redaction assertions - the
sentinel absent from both the view and the export - are unchanged and still
pass, which is what proves the redaction itself was not relaxed.

Full Django suite: 2248 tests, 1 failure, and that failure was this wording
assertion.

A note on how that run was read. An earlier attempt reported 203 errors and
then 17, and neither was real: PostgreSQL crashed mid-run and recovered, and
`--keepdb` reused the damaged database. The failures it produced looked exactly
like domain regressions - `would_delete_count: 1 != 0`,
`'other' != 'whitespace'` - with nothing anywhere saying the database was the
problem. The modules passed in isolation, and a run against a rebuilt database
left the single genuine failure above. Worth remembering: after an unclean
shutdown, `--keepdb` produces confident wrong answers.

## Rollback

Revert. A `KeyError` returns to naming nothing, and warnings return to being
reported as failures.

## Decision Log

- **Derive the safe key set from the contracts.** A hand-written list of field
  names would drift from the contracts it describes, which is the same defect
  in a different place.
- **Name model labels too.** A dict keyed by `model_string` is as common here as
  one keyed by a field, and thirty caught failures landing one per model is
  exactly the shape where a model-label key would appear.
- **Do not relax the redaction to get a diagnosis.** The device-name case stays
  silent. Being unable to diagnose one class of failure is a smaller cost than
  writing a customer's device names into a support bundle.

## Open

- The actual defect is still unidentified. This makes the next occurrence
  self-describing; it does not fix the sync, and the deployment must run again
  to produce a named key.
- Whether the crash is specific to `ipam.ipaddress` is unknown. Thirty caught
  failures spread across every model suggest the shard where it finally escaped
  may not be where it originates.
