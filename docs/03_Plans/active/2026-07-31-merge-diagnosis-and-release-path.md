# Merge Diagnosis and Release Path

## Goal

Stop a validation gate from being able to burn a release tag, and finish the
diagnosability work at the one call site it missed.

Two version numbers — `v2.6.7` and `v2.6.8` — are tagged with no artifact,
because a gate that can only run after tagging failed for two one-line reasons.

## Contract

- No long-running validation gate runs in the tag-triggered publish workflow.
  Failure blocks a merge, never a tag.
- The version to upgrade *from* is resolved from PyPI, which is the only source
  that establishes an artifact exists.
- A merge failure names its constraint or invalid fields in the issue message,
  as sync failures already do.
- The one ingestion that legitimately cannot be deleted says so before listing
  its dependants.

## Constraints

- Keep the upgrade path validated somewhere; moving the gate must not drop it.
- Persisted and logged diagnostics stay free of customer data.

## Touched Surfaces

- `.github/workflows/{ci,release}.yml`, `scripts/check_harness.py`
- `tasks.py` — `_previous_released_version`
- `forward_netbox/utilities/merge.py`
- `forward_netbox/views.py`
- `scripts/tests/test_tasks.py`, `forward_netbox/tests/test_ingestion_delete.py`
- This plan.

## Approach

**The placement was the expensive mistake.** `artifact-upgrade-test` was added to
`release.yml`, where the earliest it can run is after the tag is pushed, and the
tag ruleset forbids deleting or moving a `v*` tag. Both failures were trivial —
a tagless checkout, then a version that was tagged but never published — and
both cost a version number purely because of where the gate sat. It now runs in
PR CI, and a harness check fails closed if it ever returns to the publish
workflow **or** disappears from CI. Both directions have negative controls.

**The second failure was a false premise, not an oversight.** The docstring for
`_previous_released_version` claimed "a tag is the thing that was actually
published." This repository already contained a counter-example when that was
written — `v2.6.2` — and the function was fixed once for a symptom
(`fetch-depth`) without re-examining it. Resolution now reads PyPI, skips
file-less and yanked entries, and fails closed when the index is unreachable.
Verified live: `2.6.9` resolves to `2.6.6`, correctly skipping all three
tagged-but-unpublished versions.

**Merge messages.** `merge.py` has recorded `structured_failure_diagnosis` in
`raw_data` since 2.6.6, but its message stayed generic — so a customer's
`ipam.ipaddress` `ValidationError` read only as the exception class in the list
while the field names sat one click away. Sync-phase recorders were given the
constraint in the message; this is the third call site, now matching.

**Delete confirmation.** `ForwardIngestionDeleteView` refused only on `post`, so
NetBox first rendered every dependent object — one `ForwardDeviceIdentity` per
synced device. A customer deleting the baseline ingestion met several hundred
device names and learned the delete was impossible only after confirming it.
`get` now refuses the same way.

**The gate was also unrunnable, which moving it revealed.** Once
`artifact-upgrade-test` ran in PR CI it failed on its first honest attempt:
it built both sides of the upgrade on the target runtime under one constraints
file, and no available input satisfies that. Releases before 2.6.7 declare
`max_version = "4.6.5"` and pin `netboxlabs-netbox-branching==1.1.1` exactly, so
constraining the from side to 1.1.2 is unsatisfiable rather than stale, and the
plugin would refuse to load on 4.6.6 even if it resolved. With 2.6.7 and 2.6.8
tagged but absent from PyPI, 2.6.6 is the only reachable from-version — so the
gate had never passed, in either release it consumed.

An upgrade moves the whole runtime. The from side now seeds on the runtime that
release supported and migrates onto the current one, so the NetBox upgrade is
exercised too: `2.6.6 on v4.6.5 → 2.6.9 on v4.6.6`. The from-side constraints
hold every pin identical to `constraints.txt` except branching, whose resolution
is the point, and a test asserts that invariant so the two cannot drift.

## Validation

- `test_ingestion_delete`, `test_issue_diagnosis`, `test_issue_rendering`,
  `test_api_views`, `test_ingestion_merge`, `test_bulk_merge`: **170 tests, OK.**
- `scripts/tests`: 250 OK. `pre-commit run --all-files` converged.
- Negative controls: the placement check fires when the gate is put back on the
  tag path, fires when it is removed from CI, and is silent on the correct
  configuration.
- The PyPI resolver is tested against a version present as a tag but absent from
  the index — the exact case that burned `v2.6.8`.
- `invoke artifact-upgrade-test` passed locally end to end for the first time
  since it was written: seeded under 2.6.6 on NetBox v4.6.5, migrated to 2.6.9 on
  v4.6.6, with the rows written under the previous release read back after the
  upgrade.
- The two constraints files are asserted to differ only in the branching pin, in
  both directions.

## Rollback

Revert the commit. The gate returns to `release.yml`, resolution returns to
tags, and both messages return to their generic form. No migration or persisted
state.

## Decision Log

- 2026-07-31: The harness check was rewritten rather than extended. Its previous
  form asserted that a job running a tag-dependent task checks out with tags;
  once resolution moved to PyPI that premise was obsolete, and a check whose
  premise no longer holds is worse than none — it reads as protection.
- 2026-07-31: Six version-resolution tests were replaced, not adjusted. They
  encoded the tag-based contract, so they passed throughout both failed
  releases. A test that asserts a wrong assumption is not evidence.
- 2026-07-31: The upgrade gate stays a blocking gate, just earlier. Making it
  non-blocking would have removed the failure without removing the risk.
- 2026-07-31: The from side seeds on its own supported runtime rather than the
  gate being scoped to skip cross-runtime upgrades. Skipping would have kept the
  release moving while leaving the only upgrade anyone will actually perform —
  4.6.5 to 4.6.6, across the branching pin — untested by the gate whose entire
  purpose is to test it.
- 2026-07-31: `UPGRADE_FROM_NETBOX_VER` is a constant with a stated meaning, not
  a lookup. Nothing in packaging metadata exposes a release's supported NetBox
  ceiling, so deriving it is not available; naming it and saying when to raise it
  is honest about that.

## Evidence

- `v2.6.7` failed at `Validate upgrade from the previous release` with "No
  released tag below 2.6.7"; `v2.6.8` failed at the same step with "no version of
  forward-netbox==2.6.7 ... requirements are unsatisfiable". Both after the tag
  was pushed; both with publish and GitHub-release skipped, so no partial
  artifact exists.
- PyPI serves `2.6.6` as the latest release. Neither `2.6.7` nor `2.6.8` is
  present.
