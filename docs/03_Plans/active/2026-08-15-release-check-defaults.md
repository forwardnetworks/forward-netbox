# Make the release checks say what they measured

## Goal

Stop two release checks from silently reporting on something other than the
release in flight.

## Why

Both misfired during 2.8.1 and cost real time, in the same shape: the tool
answered confidently about the wrong subject.

`invoke release-authorization-check` defaulted `version` to `"2.6.0"`, so a bare
invocation checked a years-old release and refused. Read as a failure of the
release in flight.

`check_release_lineage.py` defaults `--release-commit` to `origin/main`, which
is correct after the release PR merges and wrong before it. Its refusal said
only "lineage has 3 commits and needs at least 4" with no indication of which
commit it had walked, so a pre-merge run reads identically to a genuinely
broken release.

## Constraints

- The lineage default is not itself wrong. Running after the merge and before
  the tag is the documented usage, and removing the default would make the
  common case worse.
- Neither check may become more permissive. These are the checks that stop a
  version number being burned.

## Touched Surfaces

- `tasks.py` - `release_authorization_check`
- `scripts/check_release_lineage.py` - the refusal message

## Approach

The authorization check drops its default and refuses without a version. A
release check that quietly measures the wrong release is worse than one that
declines to guess.

The lineage check keeps its default and names the resolved commit in the
refusal: `REFUSED for origin/main (ecd3216): ...`. That turns "the release is
broken" into "you measured the wrong commit", which is the distinction the
message was missing rather than the default being wrong.

## Validation

Behavioural, both confirmed by hand: a bare `invoke
release-authorization-check` refuses with the corrected invocation, and a
lineage refusal names its target.

## Rollback

Revert. The defaults return and so does the ambiguity.

## Decision Log

- **Remove one default, keep the other.** The authorization default had no
  correct case - no release is meaningfully "2.6.0 by default". The lineage
  default has a correct case and only needed to say what it used.
- **Fix the message rather than the default for lineage.** Requiring the flag
  every time would trade a rare misread for a permanent papercut on the path
  that runs before every tag.

## Open

- Nothing. Both are small and self-contained.
