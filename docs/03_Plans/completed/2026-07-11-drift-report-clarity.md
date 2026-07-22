# 2.5.3 — core/integrations editions + routing crash fix + drift clarity

Status: implemented on `fix/2.5.3-drift-report-clarity`.

## Goal

Four things (three Python/packaging; one opt-in query variant needing a validation-org
republish):

1. **Editions split** — make `forward-netbox` a single package with two install
   profiles: **core** (`pip install forward-netbox`, NetBox-builtin models only,
   no optional-plugin dependencies) and **integrations**
   (`pip install forward-netbox[integrations]`, or per-plugin `[dlm]`/`[routing]`/
   `[aci]`/`[peering]`). Users who don't run the optional plugins pull nothing
   extra; the integration maps stay dormant until the plugin is installed +
   enabled.
2. **Routing crash fix** — enabling the netbox-routing models crashed the sync
   with `TypeError: '<' not supported between instances of 'NoneType' and 'int'`
   (blank model, phase Sync). Latent bug surfaced on first enablement.
3. **Drift Report clarity** — a stale/empty-baseline cached preview read as real
   drift (field report: 18/19 models 100% pending).

## Constraints

- Additive / defensive only — no NQE query change, no data/schema change, no
  migration, no validation-org republish.
- The editions extra must not break existing installs: `pip install forward-netbox`
  stays functionally identical; the optional plugins are pulled only via extras.
- The routing fix must not change lookup results — only the sort order used for
  chunk batching.

## Touched Surfaces

- `pyproject.toml` — optional `netbox-dlm` / `netbox-routing` / `netbox-cisco-aci`
  / `netbox-peering-manager` deps + `[tool.poetry.extras]` (`dlm`/`routing`/`aci`/
  `peering`/`integrations`).
- `forward_netbox/utilities/sync_primitives.py` — `_sorted_dependency_scope_keys`
  helper; both `_prime_routing_bgp_identity_cache` and
  `_prime_routing_ospf_identity_cache` use it.
- `forward_netbox/utilities/drift_report.py` — `looks_like_full_create`.
- `forward_netbox/views.py` — absolute-age staleness on the drift view.
- `forward_netbox/templates/.../forwardsync_drift_report.html` — reason-aware
  banner, empty-baseline hint, Preview Dependencies button.
- Docs: README + docs/README + User Guide (Editions section + compat tables);
  `forward_netbox/tests/{test_health,test_sync}.py`.

## Approach

- **Editions:** declare the four plugins as `optional = true` Poetry deps grouped
  into extras. Core install resolves none of them; `[integrations]` (or a single
  `[dlm]` etc.) pulls them. Runtime already gates each integration on
  `apps.is_installed(<plugin>)` + a disabled-by-default map, so a core install
  never surfaces the optional maps (no ContentType → `seed_builtin_nqe_maps`
  skips them).
- **Routing crash:** the BGP/OSPF dependency-lookup caches sorted scope keys
  `(router_pk, vrf_pk | None [, process_id])`; a global-table peer's VRF pk is
  None and collides under the same router/device pk with a VRF peer, so
  `sorted()` compares `None < int`. A shared None-safe key (`-1` sentinel; pks
  are always >= 1) orders deterministically without raising. Order is irrelevant
  to the `__in` lookup results.
- **Drift:** empty-baseline fingerprint (>=3 models, every one fully pending, 0
  removals) + absolute-age staleness (>24h) + a re-run button.

## Validation

- New `DependencyScopeKeySortTest` (test_sync.py): plain sorted raises on the
  mixed None/int keys; the None-safe helper orders BGP (2-tuple) and OSPF
  (3-tuple) shapes deterministically.
- 10 drift tests (test_health.py) incl. empty-baseline detection + absolute-age.
- `pyproject.toml` validated: extras reference only declared optional deps.
- Full suite + lint + harness gates.

## Rollback

Single squashable branch; revert the merge commit. Everything is defensive /
packaging metadata — no data, query, or schema is touched, so rollback cannot
affect any sync. Reverting the extras just removes the convenience install path.

## Decision Log

- **One package + extras** (not two PyPI packages): existing `forward-netbox`
  installs are unchanged; the extras add the optional plugins without a second
  distribution or a breaking rename.
- **Routing fix via a None-safe sort, not filtering None keys:** global-table
  (None VRF) scope keys are legitimate lookups and must stay in the batch; only
  their sort position needed guarding.
- **Hardware-notice alias fix included** (rolled into 2.5.3 per maintainer):
  operators running the alias-aware device query saw netbox-dlm hardware notices
  skip (`device type X is not in NetBox yet`) because the notice looked up the
  raw Forward model while the aliased device query created the DeviceType under
  its NetBox-library name. New opt-in **forward_dlm_hardware_notices_with_netbox_aliases**
  variant applies the same alias mapping (live-verified in the validation org: 24 device types
  that skipped now resolve, 0 misses). It is the only query change in the release,
  so operators on pinned org queries need **Publish Bundled Queries** after
  upgrading.
