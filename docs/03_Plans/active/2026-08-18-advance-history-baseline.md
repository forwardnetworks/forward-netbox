# Advance the reviewed history baseline past the renamed plan file

## Goal

Let the release gate's protected-history scan start after the commit that
renamed a plan file off a customer contact's name, so v2.8.3 can be published
without rewriting public history.

## Why

The v2.8.3 publish workflow refused the tag. Its sensitive-content step runs
the release-only superset feed and reported a redacted PATH,
`path:sha256:e478676137834484`, which is
a plan filename this repository created, which carried a customer contact's
given name. Nothing was published: build and publish were skipped.

`#228` renamed the file, which fixes the working-tree scan. It does not fix the
history scan: `scan_commit_history` calls `scan_name(path, kind="path")` for
every changed path, the offending path was added in `526eda3`, and the rename
commit's own diff names it again as a deletion. Both sit inside the protected
range that starts at `f9a8420`.

## Constraints

- Public history must not be rewritten. That was done for `v2.7.7` and it burned
  `v2.7.8`: force-pushing `main` strips pull-request association by SHA, and the
  provenance verifier then refuses those commits permanently, blocking every
  release. Trading a published filename for a permanently unreleasable
  repository is not a trade.
- The baseline is an external trust anchor. `.sensitive-history-baseline` and
  the `FORWARD_SENSITIVE_HISTORY_BASELINE` repository variable must match
  exactly or the release gate refuses, so both move together and neither is
  edited casually.
- The baseline may only advance to a commit that is an ancestor of `HEAD`, and
  the scanned range is `baseline..HEAD` - exclusive - so the baseline must be
  the rename commit itself for the rename's own diff to fall outside it.

## Touched Surfaces

- `.sensitive-history-baseline` -> `479a6451964969b5e021a6cae1209e5a4493633c`
- the `FORWARD_SENSITIVE_HISTORY_BASELINE` repository variable, set to the same

## Approach

Advance rather than rewrite. What is being accepted is exactly one reviewed
finding: a filename, already public since `#223` merged, whose content was
never affected - the name appears nowhere inside that file or anywhere else in
tracked content. Rewriting history would not unpublish it either; forks,
clones and archives already have it.

Everything else in the superseded range was scanned and passed on every release
through v2.8.2, so advancing forgives one known item rather than a blind span.

## Validation

- `check_sensitive_content.py --git-files` over all tracked content.
- The digest technique that identified the path, re-run over the fixed tree.
- The v2.8.3 publish workflow, which applies the superset feed fail-closed and
  is the only thing that can actually prove this.

## Rollback

Restore the previous baseline in both places. The gate returns to refusing.

## Decision Log

- **Advance, do not rewrite.** The precedent for rewriting is `v2.7.7`, and its
  cost was `v2.7.8`.
- **Baseline set to the rename commit, not to `HEAD`.** The range is exclusive,
  so this is the earliest commit that excludes both the addition and the
  rename, and it forgives the least history that solves the problem.
- **Recorded rather than done quietly.** Moving a trust anchor with no written
  reason is indistinguishable from moving it to make a gate stop complaining.

## Open

- The gate checks paths in the tree, in history, in ref names and in tag names.
  Any pre-tag review that only reads file contents is incomplete, and a review
  that reports "clean" while never looking at a category is worth distrusting.
  Worth a scripted pre-tag check that covers every surface the release gate
  covers, rather than a habit.

## Second advance

The first advance was not enough, and the reason is worth recording because it
is the same mistake one layer up.

The publish refused a second time. Two of the recovery documents - this one and
the release plan - quoted the offending filename verbatim while explaining that
the filename was the problem. Documenting a leak by reproducing it puts the
value back into the scanned surface, and the scanner does not care that the new
copy is an explanation.

So the baseline advances again, past those two commits and past the wording fix
that removed the quotes.

What made the difference this time was not more care. It was
`.sensitive-patterns.local.txt` - gitignored, and named in every failure
message the scanner has ever printed. Nobody had created it. Without it the
local gate ran against a strictly smaller pattern feed than the release gate,
so "I read it carefully and it is clean" was the only check available, and it
was wrong twice. With it, the failure reproduces locally in under a second,
before a tag exists to burn.

Creating it immediately found a surface neither refusal had reached: a stale
remote-tracking ref still carrying the old branch name. Ref names are scanned
too. So are tag names, and paths, and file contents - four surfaces, and the
review that produced the first tag covered one of them while reporting the tree
clean.
