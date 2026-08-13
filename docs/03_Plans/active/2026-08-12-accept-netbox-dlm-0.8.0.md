# Accept netbox-dlm 0.8.0

## Goal

Let the customer run netbox-dlm `0.8.0`, which they have already upgraded to.

## Constraints

- An unmatched optional-plugin version disables that whole integration, so the
  version must be accepted everywhere or the DLM maps silently stop running.
- The upgrade gate installs the PREVIOUS release, whose metadata caps
  `netbox-dlm` below this version. The two constraint files cannot both hold
  `0.8.0`.
- Acceptance is validated by running the suite against the version, not by
  reading its changelog.

## Touched Surfaces

- The four version surfaces, moved together to 2.7.12:
  `pyproject.toml`, `forward_netbox/__init__.py`,
  `forward_netbox/utilities/fast_baseline.py`,
  `forward_netbox/tests/test_runtime_dependency_check.py`. Splitting them
  reverts the fast baseline from ~6 min to ~15 h, and the fast-baseline pin is
  the one that goes unnoticed because nothing fails - the sync just gets slow.
- The three compatibility tables and `CHANGELOG.md`
- `pyproject.toml` (the install-level cap)
- `forward_netbox/utilities/plugin_integrations/registry.py` (two version
  fields), `apply_engine_decision.py`, `merge_set_based.py`, `fast_baseline.py`
- `constraints.txt`, `development/constraints-upgrade-from.txt`
- `scripts/validate_sbom.py` (`REQUIRED_COMPONENTS`), which asserts the version
  actually present in the published SBOM
- `forward_netbox/tests/test_optional_plugin_versions.py`,
  `forward_netbox/tests/test_plugin_integrations.py`,
  `scripts/tests/test_tasks.py`

## Approach

What 0.8.0 actually changes, read from the upstream diff rather than the
release notes: `tables.py`, `views.py`, two templates, and one line of
`choices.py` where the `LOW` CVE severity badge moves from blue to yellow. The
choice VALUES are untouched, `models.py` is untouched, and there are no new
migrations - confirmed against the installed package with
`makemigrations --check`, which reports no changes and leaves the migration set
at `0005`. Nothing the plugin reads or writes is affected.

The version is added to all four runtime version sets. Missing any one of them
does not fail loudly: an unmatched version disables the integration, and the
fast-baseline, set-based-merge and apply-engine sets each independently switch
their engine off, which is how a five-minute first sync silently becomes hours.

`pyproject.toml` capped at `<0.8.0`, so pip would have refused the install
outright. That cap is not in the list of version sites this repository's notes
record, and it is the one an operator hits first.

`scripts/validate_sbom.py` is a second unrecorded site, and it fails in the
least convenient place: `invoke ci` passes, and the artifact stage then refuses
the built wheel with `SBOM required-component mismatch`. It is the correct
behaviour - the SBOM describes what was actually packaged, so a stale
expectation there means the manifest and the artifact disagree - but it means
accepting a plugin version is not done until the SBOM expectation moves with
it. Both sites are now recorded here rather than rediscovered.

## The constraint-file conflict

`development/constraints-upgrade-from.txt` documents that every pin except
branching is held identical to `constraints.txt`, and `test_tasks.py` enforces
it. That invariant cannot hold through this change: 2.7.11 declares
`netbox-dlm >=0.4.1,<0.8.0`, so pinning the from side to `0.8.0` makes the
previous release's install unsatisfiable - exactly the failure mode the file's
header describes for branching, arriving through a different door.

So `netbox-dlm` joins branching as a documented exception, but a narrower one:
branching is absent from the from side entirely, while `netbox-dlm` stays
pinned to the newest version the FROM release supports. The test now asserts
the pin is present and does not run ahead of the current one, which keeps the
resolver from picking anything it likes while allowing the lag.

The upgrade gate therefore exercises the plugin version moving with the
upgrade - 0.7.0 before, 0.8.0 after - which is what a customer actually does.

## Validation

The full Django suite with `netbox-dlm==0.8.0` installed in the runtime, plus
`scripts/tests` and the harness check. `makemigrations --check` against the
installed package for the schema claim.

## Rollback

Revert. 0.8.0 is refused at install again and the integration returns to 0.7.0.

## Decision Log

- **Validate by running, not by reading.** The diff is small and plausible, and
  that is exactly the argument that would have shipped a broken integration if
  the one changed choices line had altered a value rather than a colour.
- **Keep the from-side pin rather than removing it.** Dropping `netbox-dlm`
  from the upgrade-source constraints would let the resolver choose, which is
  the same class of unpinned behaviour those files exist to prevent.

## Open

- Nothing for 0.8.0.
