# Sensitive history rewrite, 2026-08-10

## Goal

Remove a real employee name, taken from the customer's estate and used as a
test fixture, from the public repository's history - and close the gap that let
it get there.

## Contract

- The content delta against the old `main` is exactly the fixture lines. No
  commit message, and no other tree, changes.
- Branch and tag protections are restored to their exact prior state, verified
  field by field against a backup taken beforehand.
- No published tag's ancestry changes.

## Constraints

- The repository is public, and `main` forbids deletion, non-fast-forward,
  non-linear history and direct pushes, with zero bypass actors. The rewrite
  therefore requires disabling protections briefly, which must not be left to
  chance if a step fails.
- Tags matching `v*` forbid deletion for the same reason, and a tag descending
  from the offending commit keeps its blob reachable.
- `refs/pull/N/head` is server-side and cannot be rewritten from a client.

## Touched Surfaces

- `forward_netbox/tests/test_absent_device_does_not_block_tag_domain.py` (the
  fixture)
- `.sensitive-patterns.local.txt` (gitignored, local pattern feed)
- `refs/heads/main`, `refs/tags/v2.7.7`
- Repository rulesets `main-release-integrity`, `version-tag-integrity`

## Approach

## What happened

A test added for the tag-domain fix used a real employee name from the
customer's estate as its tag fixture, in one file, on two lines. It reached
`main` through #172 and was carried forward by #173.

The publish workflow for `v2.7.7` refused it. `Validate tagged release` failed
on the sensitive-content guard; `Build distribution`, `Publish to PyPI` and
`Publish identical GitHub release artifacts` were all skipped. Nothing was built
and nothing was published - PyPI's latest stayed 2.7.6 and no GitHub release
exists for the tag.

## Why the local gates missed it

The guard runs locally through pre-commit, `invoke ci` and `check_harness`, but
it matches against whatever pattern feed it is given. The match here was against
line 3 of `FORWARD_SENSITIVE_PATTERNS`, a repository variable supplied only by
the publish workflow under `--require-env-patterns`. The local
`.sensitive-patterns.local.txt` held three lines and did not include this name,
so no local run could have failed.

That asymmetry was tolerable when CI ran the same guard on every push. It is not
tolerable now that the gates are local-only: the release feed is the only place
some names exist, and the release gate is the last thing standing between them
and a publish.

## What was done

1. The fixture was renamed to a neutral value. The test never depended on the
   string.
2. The name was added to `.sensitive-patterns.local.txt` (gitignored), so a
   local run now catches it.
3. `main` was rewritten to excise it. The two affected commits were rebuilt on
   `9aa1de3` with the fixture corrected, and force-pushed.
4. `v2.7.7` was deleted. It pointed at a descendant of the offending commit, so
   leaving it would have kept the blob reachable and made the rewrite
   pointless. The tag published nothing, so only the version number was lost.
5. Both rulesets were disabled for the push and restored immediately, from a
   backup taken beforehand, inside a trap so that a failure at any step still
   re-armed them.

## Validation

- The content delta between the old and rewritten `main` was confirmed to be
  one file, two insertions, two deletions, before any protection was touched.
- `check_sensitive_content.py --protected-history` and `--git-files` both
  returned 0 on the rewritten branch; the history scan had named two lines at
  `da6dbd8` before it.
- The name resolves nowhere in the rewritten history: neither `git log -S` over
  the branch nor `git grep` over either rebuilt commit finds it.
- The live rulesets were re-read after restore and compared to the backup on
  name, target, enforcement, conditions, rules and bypass actors: identical.
  `deletion`, `non_fast_forward`, `required_linear_history` and `pull_request`
  are active on `main`; `deletion` and `non_fast_forward` on `refs/tags/v*`;
  zero bypass actors on both.
- `v2.7.7` is absent from the remote tag list; `v2.7.1` through `v2.7.6` remain.

## Rollback

None available, and none wanted: the point of the change is that the old objects
stop being reachable. The pre-rewrite `main` was `d1aa6e9` and the ruleset
backups are in `~/src/rulesets-backup-20260810` if the protection state ever
needs to be re-asserted.

## Decision Log

- **Rewrite rather than advance the baseline.** Advancing
  `.sensitive-history-baseline` past the finding would have unblocked releases
  in one commit while leaving the name in a public repository permanently. The
  baseline is a two-key control - the in-repo file must equal the
  `FORWARD_SENSITIVE_HISTORY_BASELINE` repository variable - precisely so that
  suppressing a real finding takes a deliberate act by the owner.
- **Delete `v2.7.7` as part of the rewrite.** Leaving the tag would have kept
  the offending commit reachable through `refs/tags/v2.7.7`, so the rewrite
  would have achieved nothing. The tag had published nothing, so the only cost
  was a version number already spent.
- **Restore protections from a backup, under a trap.** Re-creating rules by hand
  after the push risks a silently weaker ruleset, which would be a worse outcome
  than the leak.

## Open

- `refs/pull/172/head` and `refs/pull/173/head` still resolve to the old commits
  on GitHub, as do the cached PR diff views. Only a GitHub Support request to
  garbage-collect unreachable objects clears those. Not yet raised.
- The local and release-time pattern feeds still differ. Extending the local
  file fixes this one name and nothing else. The durable fix is for the local
  feed to be derived from, or checked against, the release feed - so that a name
  the release gate knows about cannot pass a local gate.

## Provenance

The rewrite touched only commits made after the 2.7.6 provenance anchor
(`9aa1de3`). No published tag's ancestry changed: `v2.7.6` and every earlier
release still resolve to the commits they were verified against, and the
anchor's `PRIOR_POST_RELEASE_DOC_COMMIT` is untouched.
