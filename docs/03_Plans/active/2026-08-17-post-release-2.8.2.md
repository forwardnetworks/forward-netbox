# Post-release note for 2.8.2

## Goal

Occupy the post-release documentation bridge for `v2.8.2`.

## Constraints

- This commit must be the first on `main` after the tag and must touch only
  documentation. The slot cannot be reclaimed.
- It must not change the anchor; that is the next change.

## Touched Surfaces

- This file only.

## Approach

Documentation only, by construction.

## Validation

`scripts/check_harness.py`, which enforces the documentation-only shape of the
post-release bridge.

## Rollback

None available or needed; the bridge is inert.

## Decision Log

- **A database constraint was standing in for a gate.** The Forward-diff delete
  path had no removal allowlist while baseline reconciliation had enforced one
  since 2.7.13, so the models one producer refused by name the other deleted
  unattended. Nothing was lost at the deployment that surfaced it only because
  PROTECT refused all five rows. The lesson is that "no harm observed" and "the
  guard worked" are different statements, and only the second one is a reason
  to leave a path alone.
- **The blanket rule was wrong and the full suite caught it.** Refusing every
  delete for a protected model also refused the delete a rename produces, which
  strands a duplicate forever. The targeted tests passed; an end-to-end test
  that renames a site and asserts the old row is gone is what failed. Scope a
  destructive-path gate to the ambiguous case, not to the model.
- **Two release checks were reporting on the wrong subject.** A default version
  of `2.6.0` and a lineage refusal that never named the commit it walked. Both
  cost real detours during 2.8.1. A check that answers confidently about
  something other than what you asked is worse than one that refuses.
- **`--finish` promotes the release table too early.** It wrote a
  `release: promote` commit flipping the row to "Current release" before the
  tag, and `check_harness.py` refused it. Git history is the authority here:
  at `v2.8.1` the README still read "Release candidate", and promotion lands in
  the anchor-advance commit that follows. The playbook prose disagrees with
  what every shipped release actually did.

## Open

- The release command exited 1 after a completely successful release, because
  the post-release step tried to open `2.8.3.dev0` and `main` deliberately
  stays at the released version. This has now failed on every release since
  2.7.13 and is discarded by hand each time. Either the step or the policy
  should go; a release command reporting failure after success trains people to
  ignore its exit status.
- The adapter-only models still have no drift comparison; `#206` stays open.
- NetBox `4.6.7` and `4.6.8` shipped during this release; the "tested on" claim
  moves in its own change because the version is exact-pinned in three scripts.
- NetBox `4.7.0-beta1` landed the same day. `max_version = "4.6.99"` means the
  plugin refuses to load there rather than half-working. The compatibility work
  should lead with `_is_bulk_safe`, which asks `issubclass(model, MPTTModel)`:
  if mptt leaves the tree the import fails loudly, but if mptt stays installed
  while NetBox models stop inheriting from it, the guard silently returns
  "bulk safe" for every former tree model and `bulk_create` corrupts the
  hierarchy. Make the test assert it recognised the model rather than infer
  from a negative.
- The anchor advance follows this commit.
