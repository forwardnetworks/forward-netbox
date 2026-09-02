# Release tooling for 2.9.2: one invocation, no hand steps

## Goal

A release is one `scripts/release.py <version> --summary ... --write --publish
--auto-finish` invocation that ends with the tag published, the bridge merged
and the anchor advanced - and every step it takes is also a flag, so a failure
names where to resume.

## Why

Every release since 2.7.13 has needed hand work after the tag, and every one
of them has recorded the same open items:

- The `.dev0` contradiction, undecided in six plans.
- `--finish` committing `release: promote` on the release branch before the
  tag, which the harness refuses by construction (the anchor must name the
  release the table calls current, and cannot move until the bridge exists,
  which cannot exist until the tag does) - and a second copy of that commit
  stranded on local `main`.
- The bridge SHA, the anchor constants and the table promotion all edited by
  hand, from a printed instruction.
- The Release Authorization section typed from memory, including the test
  counts.
- The sensitive-content pre-tag review covering one of the four surfaces the
  gate reads.
- `requirements-release.txt` audited by nothing; `poetry.lock` read by nothing.
- `ui-validation` evidence required naming two variables the Playwright task
  never reads.
- The installed-route probe covering menu lists only.

Three items the survey listed as open were already closed on `main` and are
recorded here so they are not re-opened: `_github_json`'s bounded retry and
keep-alive connection, skipped preflight checks printing `skipped`, and the
release command refusing to report a published release as failed.

## Constraints

- `verify_release_provenance.py` is the trusted scanner and is not edited by
  this change; `release.py` mirrors its documentation-path rule rather than
  importing it.
- Nothing here changes what the gate runs. `verify` runs the same `invoke ci`
  and `invoke artifact-test`; it now also keeps their output.
- The authorization checker is not relaxed to accept the rendered section.
  The renderer is tested against the checker's own predicate.
- No Django suite: `invoke harness-test` covers all of it.

## Touched Surfaces

- `scripts/release.py` - `stage_open_next` and `--open-next` removed;
  `stage_verify` records evidence (`.release-evidence.json`, gitignored) with
  logged gate output; `promote_release_tables` is a file edit; `stage_finish`
  on the production branch no longer promotes; `stage_authorize`,
  `stage_anchor`, `stage_finish_unattended`; `stage_post_release` opens its
  pull request and cleans up on failure; `--authorize`, `--post-release`,
  `--anchor` flags.
- `scripts/check_release_authorization.py` - `ui-validation` accepts what the
  task reads (`FORWARD_NETBOX_PLAYWRIGHT_HOST_PORT`, optional).
- `scripts/check_release_preflight.py` - audits `requirements-release.txt`
  alongside `constraints.txt`; `check_lockfile_consistency` (`poetry check
  --lock`).
- `scripts/check_release_lineage.py` - runs the sensitive gate over tree,
  protected history, ref names and tag names, and reports which.
- `scripts/validate_installed_routes.py` - every registered pk-scoped route,
  enumerated from the resolver.
- `.gitignore`, `docs/03_Plans/technical-debt.md`, `active/README.md`,
  `2026-08-18-no-dev0-on-main.md`.

## Approach

The decision first: `main` carries the released version, and the marker
machinery is deleted rather than left as a trap. Then promotion moves to where
the harness can accept it - the anchor commit, which `stage_anchor` now
generates from the tag: it derives the bridge (first first-parent commit after
the tag, checked documentation-only), advances both constants, promotes the
tables, regenerates the changelog, writes its own plan file, and opens the pull
request.

`stage_verify` tees the two gate commands to log files and records what ran:
the exact `rtk env ... invoke` forms the checker parses, exit statuses, the
`Ran N tests` lines, the runtime versions the tree declares. `stage_authorize`
renders the Release Authorization section from that record after the
production pull request merges - when the evidence base commit is knowable -
and commits it on the evidence branch.

`stage_finish_unattended` sequences the existing single-step functions and
waits for each pull request to merge between them. The bound on the wait
exists so an unattended run ends with an instruction instead of a hang.

## Validation

- `invoke harness-test`: 375 tests OK (was 334), including: the promotion
  helper is gone and `--finish` on the production branch commits nothing; the
  rendered authorization satisfies `_evidence_is_concrete` for every required
  id; the unattended driver's step order and its resume message; the anchor
  stage advances both constants and deletes its branch on failure; the lineage
  check names all four sensitive surfaces; both requirement files are audited
  with the same waivers.
- `_detail_routes` enumerated 57 pk-scoped routes on the development stack,
  every one reversible.
- The 2.9.2 release itself is the integration test.

## Rollback

Revert. The manual flags (`--finish`, `--post-release`, `--anchor`) remain
individually runnable, so a release in flight can be finished by hand at any
step.

## Decision Log

- **`.dev0` is gone, not narrowed.** The incident it documented was real and
  the marker was the wrong remedy: customers install from source, so a dev
  marker on `main` offered a version nobody had gated.
- **Promotion lives in the anchor commit.** Not because that is tidy, but
  because the harness ties the anchor to the current release and refuses
  either moving alone. The release branch flip was refused on every release
  since 2.8.6 and discarded by hand each time.
- **The authorization is rendered, not relaxed.** It would have been simpler
  to loosen the checker. The checker is what a reviewer trusts; the renderer
  is tested against it instead.
- **The bridge SHA is derived from the tag, not typed.** `git rev-list
  --first-parent --reverse <tag>..origin/main | head -1`, then checked
  documentation-only before anything is written.
- **Sensitive feed unification is closed as designed, not built.** The
  release-time feed is a repository secret; a checkout cannot derive it.
  `check_sensitive_pattern_parity` already fails closed unless the operator
  acknowledges the gap in the evidence, which is the honest version of parity.
- **`poetry check --lock` is a preflight check, not a harness check**, for
  the same reason `pip-audit` is: it needs a tool the release host has and a
  contributor's machine may not.

## Open

- Nothing.
