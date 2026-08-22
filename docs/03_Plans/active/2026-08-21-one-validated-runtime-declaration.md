# One declaration of the validated runtime

## Goal

Make adding an optional integration a single edit, so a partly-added one cannot
silently disable the fast paths.

## Why

Three subsystems refuse to run unless the installed runtime matches a validated
set: the COPY/SQL apply engine, the set-based merge, and the fast baseline.
Each carried its own copy, and the fast baseline carried two - the versions it
EXPECTS and, separately, the distributions it actually PROBES. The
version-test fixtures spelled the same facts out a fifth time.

Five hand-maintained copies of one fact, every divergence failing CLOSED and
SILENTLY. A plugin listed in four places and missed in the fifth disables the
fast path with no error anywhere, and for the fast baseline that is a first
sync taking hours instead of minutes.

This is not hypothetical. Registering one optional integration required finding
all five, and each was located only by a different test going red - including
the probe/expected pair, where the failure reports as a version mismatch and is
not one. `#280` makes that condition visible to an operator; this removes the
way it is created.

## Constraints

- No behaviour change. The validated set is byte-identical before and after;
  this moves where it is written, not what it says.
- Each subsystem keeps its own JUDGEMENT - which models it touches, what it
  does on a mismatch, its reason codes. The shared module carries facts about
  the runtime, not policy about it.
- The existing constant names stay, so no call site changes and the diff is
  reviewable as a move.

## Touched Surfaces

- `forward_netbox/utilities/validated_runtime.py` (new) - the declaration
- `forward_netbox/utilities/apply_engine_decision.py` - derives both constants
- `forward_netbox/utilities/merge_set_based.py` - derives both constants
- `forward_netbox/utilities/fast_baseline.py` - derives the expected tuple AND
  the probe list, which is the pair that could disagree
- `forward_netbox/tests/test_validated_runtime_is_declared_once.py` (new)

## Approach

`validated_runtime` declares the NetBox and Branching series, the plugin apps,
and the optional distributions with every validated version. The probe list is
derived from the distribution mapping rather than repeated, so the two halves
of the fast-baseline check cannot diverge - the probe list IS the expected
list. Three small helpers (`validated_plugin_apps_match`,
`unexpected_plugin_apps`, `missing_plugin_apps`) give consumers the comparison
without re-implementing it.

## Validation

`test_validated_runtime_is_declared_once` asserts IDENTITY, not equality:
equality would pass again the moment someone pasted a literal back in. It also
asserts that the fast-baseline probe reports every expected distribution, and
that patching the single declaration moves every consumer - which is the
guarantee the refactor exists to provide.

43 tests across the guard suite, `test_optional_plugin_versions`,
`test_fast_baseline` and `test_copy_sql_apply_engine` pass unchanged.

## Rollback

Revert. The constants return to being written out per subsystem with the same
values; nothing about engine behaviour depends on this change.

## Decision Log

- **Identity assertions over equality.** Equality tolerates exactly the defect
  this prevents.
- **Derive the probe list from the distribution mapping** rather than keeping
  two lists in step. This was the subtlest of the five copies: a distribution
  expected but not probed reports as absent, so the failure looks like a
  version problem and is not one.
- **Keep the old constant names.** A rename would have made the diff a
  rewrite rather than a move, and these names appear in engine reason-code
  context that operators and support bundles already carry.
- **Facts here, policy there.** Tempting to move the whole decision function;
  rejected, because each engine's response to a mismatch is genuinely its own.

## Open

- The `forward_netbox` version pin inside the fast-baseline expected tuple is
  still a literal, because it tracks the release version rather than the
  validated runtime and is bumped by the release script. Folding it in would
  couple this module to the release process.
- `#280` reports this condition to operators; it is the companion to this
  change and is deliberately a separate branch.
