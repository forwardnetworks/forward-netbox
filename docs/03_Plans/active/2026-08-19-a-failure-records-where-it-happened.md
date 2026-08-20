# A failure records where it happened

## Goal

Put the raising location into the record, so the next unhandled exception is
diagnosable from what the deployment already has.

## Why

A deployment hit the same unhandled `KeyError` on two consecutive releases -
`ipam.ipaddress`, plan item 51 of 90, about thirteen seconds in, roughly half an
hour of staging discarded each time. The second failure came AFTER a release
whose stated purpose was making that failure name itself.

That release made the KEY nameable, and drew the safe set from
`MODEL_SYNC_CONTRACTS`. The key turned out not to be a name this repository
chose, so it was correctly withheld and the run was exactly as opaque as before.
The fix was aimed at the wrong artifact.

What identifies a defect is the frame: file, line, function. Those are
identifiers this repository wrote. Unlike an exception message they cannot quote
a device or an address, and unlike a rendered traceback they carry no locals.
They were being dropped only because they arrive attached to a traceback, which
is redacted wholesale and rightly so.

Three places should have carried it and none did. The ingestion issue recorded
`exception_type` alone. The job error is redacted at write time. And the Python
logger was called without `exc_info`, so the server log held the exception class
and nothing more.

The traceback survived at all only because `jobs.py` re-raises anything that is
not an expected failure, so RQ logged it. Answering "where did it fail" meant
walking an operator through reading RQ's failed-job registry out of Redis. That
is luck standing in for design.

## Constraints

- Nothing exported gains a value it did not have. The job record, the ingestion
  issue and the support bundle stay redacted; they gain a code identifier.
- A frame must survive `safe_log_message`, which REBUILDS a failure line from
  what it can recover and discards the rest. A detail that is not recoverable is
  written and then thrown away - which is how an earlier attempt at richer
  failure messages was undone.
- Only frames inside this package. A third-party path can embed a home
  directory, and a Django frame says nothing this repository can act on.

## Touched Surfaces

- `forward_netbox/utilities/diagnostics.py` - `plugin_raise_site`,
  `recovered_raise_site`, `with_raise_site`, `_SAFE_SOURCE_PATH`, and the
  sanitizer
- `forward_netbox/utilities/sync_orchestration.py` - the sync failure recorder,
  and `exc_info=True`
- `forward_netbox/utilities/merge.py` - the merge failure recorder
- `forward_netbox/tests/test_raise_site_is_recorded.py` (new)

## Approach

`structured_failure_diagnosis` gains `raise_site`: the innermost in-package
frames, `path:line:function`, bounded at eight. Both failure recorders append
the innermost one to the operator-facing message, and the log sanitizer recovers
and re-emits it so it reaches the export intact.

The Python logger gets `exc_info=True` at the top-level sync failure, and only
there. That writes to the deployment's own server log, which never leaves the
deployment - the one place a full traceback belongs.

Paths validate against `_SAFE_SOURCE_PATH` rather than `_SAFE_DIAGNOSTIC_TOKEN`.
The token pattern excludes `/`, correctly, because a token is not a path; reusing
it silently rejected every frame and returned an empty list that looked like a
working function. A test caught that, which is the argument for pinning the
positive case and not only the leak.

## Validation

`forward_netbox/tests/test_raise_site_is_recorded.py` pins both directions: the
frame is named, the innermost comes first, it reaches the persisted diagnosis
and survives the sanitizer rebuild; and the key value never appears, a frame
from outside the package is never recorded, a forged `/home/...` frame is not
preserved, and the list is bounded.

Full Django suite.

## Rollback

Revert. A failure returns to naming its exception class and nothing else, and
the next one needs the RQ registry again.

## Decision Log

- **Frames, not the key.** The previous release named the key and could not name
  this one. A frame is available for every exception, not just `KeyError`.
- **`exc_info` on the server log only.** A full traceback belongs where it does
  not travel. Everything exported gains a code identifier and nothing else.
- **Teach the sanitizer rather than lower the log level.** The established
  workaround is logging diagnostics at info so the rebuild leaves them alone;
  that hides a failure from anything filtering on level. Preserving one
  strictly-validated token is narrower.
- **Both recorders together.** The merge recorder was the one left behind the
  last time this file gained a diagnostic.

## Open

- The defect itself is still unidentified. This makes it findable; the frames
  from that deployment's next run, or from its RQ registry, are what name it.
