# Published-query signature drift: a Health warning and a release gate

## Goal

v2.9.7 changed two bundled `.nqe` queries' `@query` parameter list from one
parameter to seven. `pip install -U` never rewrites a query already
published into a customer's Forward org, so the stale published copy
rejected every execution with HTTP 400
(`Provided argument, 'x' is not a parameter to the given query`), and the
operator saw only `Dependency preview query validation failed for 2
model(s)` - never why, never that a republish would fix it. The mechanism
(`parameter_signature_drift`) already existed and is used by the on-demand
"Export Live Query Drift Check", but nothing surfaced its result as a
standing warning, and nothing stopped a future release from shipping the
same class of change without saying so in the changelog. Close both gaps:
warn on the Health page from stored state, and refuse a release that
changed a signature without telling the operator to republish.

## Constraints

- The Health check makes no live Forward call - it reads
  `ForwardNQEMap.last_live_drift`, already stored by the on-demand check.
  This item does not add a live check at sync start (deferred; see Decision
  Log - the plan's own "best effort" framing for this item).
- The release-gate check is Django-free: `scripts/check_release_preflight.py`
  runs in an environment with no NetBox/Django installed, so it cannot
  `import forward_netbox` through the normal package (`forward_netbox/__init__.py`
  imports `netbox.plugins` at module level).
- The gate must never invent a false positive from missing context: no
  prior release tag, no `.nqe` files, or no changelog row yet for the
  version under preparation are all `skipped`, not refused.

## Touched Surfaces

- `forward_netbox/utilities/health.py`: `_query_signature_drift_check`
  (new), wired into `_health_checks`.
- `scripts/check_release_preflight.py`: `check_bundled_query_signatures`
  (new), `_declared_query_parameters_module` (loads the parser without the
  plugin package), wired into `main()`/`_report_lines`.
- Tests: `forward_netbox/tests/test_query_signature_drift_check.py` (new),
  `scripts/tests/test_release_preflight.py` (extended).

## Approach

1. **Health check reads stored state only.** For every map
   `sync.get_maps()` returns, `_query_signature_drift_check` looks at
   `last_live_drift.get("status")`. Empty (never checked) is skipped
   entirely from the count; `"live_query_id_parameter_mismatch"` is a
   `danger` naming every affected model and pointing at **Publish Bundled
   Queries**; anything else checked and clean is a `pass`. No map ever
   checked at all returns `None` (no check shown), matching the existing
   `_config_backup_delivery_check`/`_base_variant_conflict_check` pattern
   this mirrors.
2. **The release check loads the real parser, not a second copy of it.**
   `forward_netbox/utilities/query_execution_contract.py` has zero
   relative imports (stdlib only), so `_declared_query_parameters_module`
   loads it directly via `importlib.util.spec_from_file_location`,
   bypassing `forward_netbox/__init__.py` entirely. This avoids maintaining
   a duplicate signature-parsing regex that could silently drift from the
   real one.
3. **`check_bundled_query_signatures(version)`** compares each
   `forward_netbox/queries/*.nqe`'s declared parameter names at
   `verify_release_provenance.PRIOR_RELEASE_TAG` (via `git show`) against
   the working tree. Any change requires the README's `v{version}` row
   (the same row `gen_changelog.py` reads) to contain the literal string
   "Publish Bundled Queries"; otherwise it raises `PreflightError` naming
   every changed file. A file with no prior source (new since the last
   release) is never flagged - there is nothing for it to have drifted
   from.
4. **Wired as an ordinary preflight check**: fast, static, runs before the
   expensive gate stages, in the same `try/except PreflightError` block as
   every other check, reported through the same `_report_lines`/JSON
   shapes.

## Validation

- `test_query_signature_drift_check.py`: no map ever checked returns
  `None`; every checked map matching passes; a mismatched map is a danger
  naming the model and "Publish Bundled Queries"; multiple mismatched maps
  are all named; a matching map alongside a mismatched one still warns and
  names only the mismatched one.
- `scripts/tests/test_release_preflight.py` (`BundledQuerySignaturesTest`):
  no files is skipped; an unchanged signature passes; a new parameter with
  no changelog mention is refused; the same change with the mention passes;
  a file new since the prior release is never flagged; no changelog row yet
  is skipped, not refused; an unavailable prior tag is skipped; and a
  real-tree, unmocked run against the actual shipped queries and the real
  `PRIOR_RELEASE_TAG`.
- Manually confirmed against the live tree: adding a parameter to a real
  `.nqe` file is detected (`"1 signature change(s) since v2.9.8"`), then
  reverted with no diff remaining.
- `invoke harness-test`: 422 tests, green (up from 414).
- Regression: `test_health`, `test_quarantine_cadence`,
  `test_uncovered_absence_and_trend`, `test_config_backup_job_and_health` -
  108 tests, green.
- Full `invoke ci` before push.

## Rollback

Single change set, no migration. Revert removes the Health check and the
release-gate check; nothing else depends on either.

## Decision Log

- **The sync-start live check is deferred, not built here.** It is the
  item's highest-risk piece: a live Forward call added to the sync
  execution path itself, versus this item's other two pieces, which are
  either read-only (Health) or run before any Docker/live infrastructure
  exists at all (the release gate). Given the plan's explicit "best effort,
  no live troubleshooting" framing for the remaining items this session,
  the Health warning plus the release gate cover both real incidents this
  class of bug has caused (an operator finding out only from a failed sync,
  and a release shipping the break silently) without touching the sync
  path. A future item can add it once there is room to validate the
  `forward_api_usage` budget impact properly.
- **`importlib.util.spec_from_file_location`, not a duplicated regex.** The
  research for this item flagged "a Django-free copy of
  `_QUERY_SIGNATURE_RE`, pinned by a parity test" as the fallback; loading
  the real file directly turned out to work cleanly (the module has no
  package-relative imports) and needs no parity test at all, since there is
  only one copy of the parser.
- **A literal string match ("Publish Bundled Queries") for the changelog
  requirement, not a structured field.** The README compatibility table is
  free text by design (`gen_changelog.py` reads it verbatim), and this is
  the same convention the v2.9.5/v2.9.8 rows already used in practice
  ("run Publish Bundled Queries") - the check formalizes an existing
  wording pattern rather than inventing a new one.
