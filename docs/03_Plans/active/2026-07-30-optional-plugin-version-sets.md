# Optional Plugin Version Sets

## Goal

Let an operator upgrade an optional NetBox plugin without being blocked at
install, and without silently losing the fast paths if they get past it.

Reported from the field: netbox-dlm was upgraded to 0.5.0 and the plugin still
required 0.4.1.

## Contract

- `netbox-dlm` 0.4.1 **and** 0.5.0 are accepted, at install and at runtime.
- The fast baseline, set-based merge and COPY/SQL engines still engage on both,
  rather than falling back with no operator-visible reason.
- Any version not listed is still refused. Widening is not "accept anything".
- What the release artifact is built and validated against is unchanged.

## Constraints

- Do not relax the build-reproducibility pins (`constraints.txt`,
  `scripts/validate_sbom.py`, the artifact SBOM in `tasks.py`). Those record
  what we ship, not what we tolerate.
- The runtime gates must keep failing closed for unvalidated versions.

## Touched Surfaces

- `pyproject.toml` — the optional-extra dependency
- `forward_netbox/utilities/fast_baseline.py`
- `forward_netbox/utilities/merge_set_based.py`
- `forward_netbox/utilities/apply_engine_decision.py`
- `forward_netbox/tests/test_optional_plugin_versions.py` (new)
- This plan.

## Approach

Two independent problems, only one of which the operator could see.

**1. The install pin.** `netbox-dlm = {version = "0.4.1"}` is an exact `==` in
Poetry, so `pip install forward-netbox[dlm]` refuses 0.5.0 outright. Relaxed to
`>=0.4.1,<0.6.0`.

**2. The runtime gates, which is the one that would have hurt more.** The fast
baseline, set-based merge and COPY/SQL each pinned *every* optional distribution
to one exact version and compared the whole tuple. On 0.5.0 all three switch off
with `unsupported_runtime_tuple` and no message an operator sees. For the fast
baseline that is a first sync going from minutes to hours. Relaxing only the
install pin would have moved the customer from "blocked" to "silently slow",
which is worse.

Each entry is now a `frozenset` of versions validated against that engine, and
the comparisons became membership tests. `fast_baseline._runtime_decision`
compared whole dicts, so it now compares the scalar keys directly and the
optional-plugin versions by membership; its rejection detail renders the sets as
sorted lists so the persisted job evidence stays JSON-serialisable.

**Why 0.5.0 qualifies.** Both distributions were downloaded and compared. The
delta is a single migration: it adds `SoftwareVersion.release_designation`
(`CharField(blank=True, max_length=10)`) and a unique constraint on
`(platform, release_designation)` **conditioned on that field being non-empty**.
This plugin never sets it, so its rows carry `''` and the constraint cannot
apply to them. The coalesce key it does use, `(platform, version)`, is unchanged,
and no field the plugin writes was altered or removed.

## Validation

- `test_optional_plugin_versions` — 7 tests: both versions accepted by all three
  gates, an unvalidated version still refused by each, the other three
  distributions still single-valued, and the fast-baseline rejection detail
  still JSON-serialisable.
- `test_fast_baseline`, `test_set_based_merge`, `test_copy_sql_apply_engine`,
  `test_plugin_integrations`, `test_apply_engine`: **75 tests, OK.**
- `pre-commit run --all-files` converged over two rounds.

## Rollback

Revert the commit. The sets collapse back to single pins and the install pin
returns to `0.4.1`; no persisted state or migration is involved.

## Decision Log

- 2026-07-30: Widened to a validated **set**, not a range, for the runtime
  gates. These are fail-closed gates in front of engines that bypass ordinary
  ORM paths; a range would silently admit a future version nobody has looked at.
  The install pin is a range because pip needs one, and it is bounded below 0.6.
- 2026-07-30: Build-reproducibility pins deliberately left at 0.4.1. Permitting
  a version at runtime and shipping an artifact validated against it are
  different claims, and conflating them would make the SBOM inaccurate.
- 2026-07-30: This is reasoned from the schema delta between the two
  distributions, **not** from a live sync on 0.5.0 — there is no 0.5.0
  environment here to run against. The gates still fail closed for anything
  else, so the exposure is bounded to a version whose diff was inspected.
