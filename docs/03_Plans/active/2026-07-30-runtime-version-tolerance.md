# Runtime Version Tolerance

## Goal

Stop refusing to run on a patch release, and move the testbed to NetBox 4.6.6
with netbox-branching 1.1.2.

Reported from the field twice in one morning: the plugin refused NetBox 4.6.6
outright, and separately refused netbox-dlm 0.5.0. A third failure — a branch
that could not be deleted — turned out to be a consequence of the first.

## Contract

- Any NetBox `4.6.x` at or above `4.6.5`, and any `netbox-branching` `1.1.x`.
- The testbed, CI matrix, artifact validation and release evidence all pin the
  newest of each: NetBox `4.6.6`, Branching `1.1.2`.
- Every engine that bypasses ordinary ORM paths keeps a **behavioural** check
  against the live runtime. Version tolerance never becomes "assume it works".
- A different series is still refused.

## Constraints

- Do not weaken a fail-closed gate without replacing it with a stronger check.
- Keep the release-evidence contract exact: it records what actually ran.

## Touched Surfaces

- `forward_netbox/__init__.py` — `max_version`, the Branching requirement
- `forward_netbox/utilities/version_series.py` (new)
- `forward_netbox/utilities/{fast_baseline,merge_set_based,apply_engine_decision}.py`
- `pyproject.toml`, `constraints.txt`
- `.github/workflows/ci.yml`, `development/{docker-compose.yml,Dockerfile}`, `tasks.py`
- `scripts/{validate_sbom,validate_installed_artifact,check_release_authorization,verify_release_provenance}.py`
- tests for all of the above, and the three compatibility tables
- This plan.

## Approach

**Two hard blocks stopped the plugin loading at all.** `max_version = "4.6.5"`,
and a `!= "1.1.1"` check on Branching that raised `ImproperlyConfigured`. Either
one turns a patch upgrade into an outage. Both are now series-based: `4.6.99`
and `1.1.x`.

**Three soft blocks were worse, because they were silent.** The fast baseline,
set-based merge and COPY/SQL engines each compared an exact version tuple. Past
the load block, all three would switch off with no operator-visible reason — for
the fast baseline that is a first sync taking hours instead of minutes. They now
match on release series.

**Version equality was never the protection it looked like.** It fired on every
patch and identified nothing, while the one genuinely breaking change this
morning — NetBox 4.6.6 adding `('comments', 5000)` to `MACAddressIndex.fields` —
was caught by the set-based engine's *search-index contract check*, reading the
live indexer. So the pins were simultaneously too strict and not protective.

The replacement is a behavioural check per engine:

- **set-based merge** — already verified the live MAC indexer. It declines on
  4.6.6 with `search_index_contract_mismatch` because it writes those index rows
  in SQL and does not emit `comments` yet. Correct, and left that way.
- **fast baseline** — had *only* a version tuple, and it is the engine that
  writes direct-to-main bypassing branch audit. It now records the required
  fields of all 29 models it loads and checks them live
  (`model_field_contract_mismatch`). Its search-index handling was already safe:
  it reads `get_indexer(model).fields`, which is why it passed on 4.6.6.
- **COPY/SQL** — inherits the runtime tuple plus its own per-model spec version.

The field contract is scoped to **required** fields deliberately. A new optional
column is filled correctly by `bulk_create` without help; a whole-field-set
fingerprint would reject netbox-dlm 0.5.0 over `release_designation`, which is
the very upgrade this release exists to allow.

## Validation

- Full plugin suite on the new baseline (NetBox 4.6.6): see the release evidence.
- `scripts/tests`: 247 OK.
- 4.6.6 was proven before the cap was raised: an earlier run on 4.6.6 produced
  18 failures, all in the two version-pinned engines; widening the gates cleared
  13, and the remaining 5 resolved into 4 stale fixtures plus the one genuine
  `search_index_contract_mismatch`.
- Both directions of the set-based skip were checked: its tests still run on
  4.6.5 (37/37) and skip on 4.6.6, so the guard did not disable them globally.

## Rollback

Revert the commit. The series helper and the field contract are additive; the
pins return to exact values and the testbed to 4.6.5/1.1.1.

## Decision Log

- 2026-07-30: Series tolerance, not an open range. `4.6.x` and `1.1.x` are
  accepted; a different series is refused. The behavioural checks carry the
  safety, so the version test only has to establish "same series".
- 2026-07-30: Testbed moved to the newest supported versions rather than testing
  the whole matrix. Testing both was considered and dropped: the tolerance is
  enforced by contract checks that run on whatever the runtime is, so a second
  matrix leg costs 45 minutes per run to re-prove what the checks already
  assert.
- 2026-07-30: `min_version` stays `4.6.5`. Tolerance extends forward within the
  series, not backward past what was ever tested.
- 2026-07-30: netbox-dlm remains an explicit validated set (`0.4.1`, `0.5.0`)
  rather than a series. Optional plugins are third-party and their patch
  releases are not covered by any contract check here, so each is inspected.
- 2026-07-30: Set-based merge is left declining on 4.6.6. Emitting the
  `comments` index row means proving search parity, which deserves its own
  change; the engine is opt-in and off by default.

## Evidence

- Branching 1.1.1 → 1.1.2 was diffed: `merge_strategies/squash.py` — the module
  this plugin depends on most — is **byte-identical**, the only code change is
  `jobs.py` adding `AutoArchiveBranchJob`, and no definition was removed.
- NetBox 4.6.5 → 4.6.6 adds nine migrations. The four touching models this
  plugin writes are all additive or denormalized: `_abs_length` widened, a
  data migration nullifying empty `cable_end` (this plugin always writes "A"/"B"),
  `prefix._region`/`_site_group` moved to `SET_NULL`, and a new `portmapping`
  model.
- The branch-deletion failure reported alongside these was **not** a separate
  defect: `ForwardIngestion.branch` is already `SET_NULL`, but Django enforces
  that in the ORM and the database FK carries no delete rule
  (`confdeltype = 'a'`). With the plugin unloaded — which is what the version
  block forced — the collector never runs and Postgres refuses the delete.
  Restoring the ability to load fixes it.
