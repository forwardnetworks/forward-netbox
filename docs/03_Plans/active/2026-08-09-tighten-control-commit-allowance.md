# Tighten the direct-control-commit allowance, and a dead throttle disjunct

## Goal

Make the release-provenance allowance say what its name implies at any lineage
length, and remove a condition that reads as the opposite of its intent.

## Contract

- The production and release commits always come through a merged pull request.
- Only control commits AHEAD of that pair may be direct.

## Constraints

- A reviewed lineage may be exactly three commits, and ours routinely is:
  anchor, production, release. Any rule expressed as "the first N positions"
  therefore covers the whole lineage at the minimum size.
- The throttle change must not alter behaviour today. It does not: the single
  non-forced caller sets the dirty flag on the line immediately above the call.

## Touched Surfaces

- `scripts/verify_release_provenance.py`, `scripts/tests/test_verify_release_provenance.py`
- `forward_netbox/utilities/logging.py`

## Approach

Anchor the allowance to the END of the lineage (`index < len(reviewed) - 2`)
rather than to a fixed count from the start, and pin that with a test across
lineage sizes 3, 4 and 7.

Rewrite the persist throttle as `force or (dirty and elapsed)`.

## Validation

- `scripts/tests` - 300 tests, OK
- `invoke test-isolated` - full plugin suite, OK
- Verified against the real 2.7.5 lineage: at three reviewed commits the
  allowance now covers only the anchor, and the anchor is a merged PR anyway

## Rollback

Revert. The allowance returns to `index < 3` and the throttle to its
three-way disjunct.

## Decision Log

- **Not a live hole, fixed anyway.** The release commit touches runtime paths
  so the direct-control check would reject it, and the branch ruleset forbids
  direct pushes. This is defence in depth whose stated rule did not match its
  behaviour - worth correcting precisely because nothing else was relying on it.
- **The throttle is inert today.** Fixed for the tripwire, not for a symptom:
  as written it says "persist when nothing changed", so the first caller that
  left the flag False would write on every call and skip the throttle entirely.

## Open

- Neither of these came from a symptom. Both were found by sweeping for the
  shape behind `#43` and `#61` - an override that cannot fire. That sweep found
  no third instance of the shape itself.
