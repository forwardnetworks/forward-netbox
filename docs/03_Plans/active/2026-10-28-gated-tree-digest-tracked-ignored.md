# Make the cut's gated-tree digest match the committed tree

## Goal

The tag push should run the quick gate when the tagged tree is the tree the cut gated. In 3.0.9 it ran the full gate (about four extra hours) because the digest recorded at the cut never equalled the digest of the committed tree, even though the two trees were identical.

## Constraints

- Release tooling only: no plugin runtime code, no migration.
- A mismatch must stay fail-safe: when in doubt the tag push runs the full gate.
- The digest must still ignore the release plan and untracked-and-ignored files.

## Touched Surfaces

- `scripts/gated_tree.py`: `working_tree_digest`.
- `scripts/tests/test_gated_tree.py`: regression tests.

## Approach

`working_tree_digest` builds its tree in a throwaway index with `git add -A`. From an empty index that skips a tracked file which `.gitignore` also matches (`development/env/netbox.env` and `postgres.env` match `env/`), so the digest had two fewer entries than the commit. Seed the throwaway index from HEAD first (`git read-tree HEAD`): tracked files then stay in it, deletions and modifications on disk are still picked up by `add -A`, and ignored untracked files stay out.

## Validation

- `python -m unittest scripts.tests.test_gated_tree`: the new tests fail without the change and pass with it.
- On a clean checkout the working-tree digest equals the commit digest of HEAD for this repository.
- `invoke harness-check`, then the quick push gate.

## Rollback

Revert the commit; the tag push falls back to the full gate, which is slower but correct.

## Decision Log

- Seed from HEAD rather than from a copy of the real index: the cut stages its own edits with `git add -A` afterwards, so HEAD plus the working tree is what gets committed. A tracked file removed from the index but left on disk before a cut would make the digests differ, which only costs a full gate.
