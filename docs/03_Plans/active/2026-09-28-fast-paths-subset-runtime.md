# The fast paths run on any subset of the validated plugins

## Goal

A customer's Health tab reads "Fast apply paths: Disabled ... validated
plugins that are not installed: `netbox_cisco_aci`, `netbox_peering_manager`".
They use neither plugin. The validated-runtime rule was exact equality with
every validated plugin, so an optional plugin's *absence* turned off the fast
baseline, COPY/SQL and the set-based merge, and every first sync took hours
instead of minutes. Make the rule a subset: no plugin outside the validated
set, the required ones present, and each installed optional plugin at a
validated version.

## Constraints

- **An extra plugin still refuses.** COPY/SQL has no signal-receiver or
  trigger check of its own; the plugin set is its only defence against an
  unknown plugin's receivers or triggers.
- **An installed optional plugin at an unvalidated or unreadable version
  still refuses.**
- **The subset rule ships only with the absent-plugin hazards it would expose
  fixed:**
  - `apps.get_model` raises `LookupError` (it never returns None), in the
    locked load and in the target and side model lookups;
  - `fast_baseline_models` imported `netbox_routing` unconditionally.
- **One declaration.** All three gates and the Health check use the same
  helper.

## Touched Surfaces

- `forward_netbox/utilities/validated_runtime.py`: `REQUIRED_PLUGIN_APPS`,
  `OPTIONAL_PLUGIN_APP_DISTRIBUTIONS`, `plugin_runtime_mismatch`, and
  subset `validated_plugin_apps_match`.
- The three gates: `apply_engine_decision.py`, `merge_set_based.py`,
  `fast_baseline.py`.
- `forward_netbox/utilities/fast_baseline.py`: `_installed_model` in
  `_target_models`, `_lock_target_tables` and `_side_models`.
- `forward_netbox/utilities/fast_baseline_models.py`: the routing import
  guard, the same one `main` has carried since #350.
- `forward_netbox/utilities/health.py`: `_fast_path_runtime_check` reads the
  gates' own decisions.
- `forward_netbox/views.py`: the bundle `environment` section.
- Tests:
  - `test_fast_paths_subset_runtime.py` (new);
  - `test_fast_paths_report_when_disabled.py`,
    `test_validated_runtime_is_declared_once.py` and `test_log_export.py`
    (updated).

## Approach

1. **The rule, as `plugin_runtime_mismatch(apps, versions)`.** It returns
   `None` when supported. Otherwise it returns one of the gates' existing
   reason codes:
   - `unsupported_plugin_app_tuple` for an unexpected app or a missing
     required one;
   - `unsupported_optional_plugin_version` for an installed optional plugin
     whose version is not validated.
2. **The gates.** COPY/SQL and the set-based merge replace their two loops
   with the helper. The fast baseline's `mismatched` expression does the same.
3. **The hazards.** `_installed_model` returns None for an uninstalled app, and
   the existing `if model is None` guards finally mean something. The routing
   contract imports `netbox_routing` only when there are routing rows, and
   fails closed if rows exist without the plugin.
4. **Health.** The check asks each gate for its decision and names the actual
   cause: an unexpected app, a missing required one, or an unvalidated
   version with the validated list. It never names an absent optional plugin
   as a cause.
5. **Bundle.**
   - `environment` is computed from `settings.PLUGINS`. `INSTALLED_APPS`
     listed every Django app as unexpected and every plugin as missing.
   - `optional_plugin_versions_validated_against` reports the full validated
     set per app.
   - New keys: `plugin_apps`, `missing_required_plugin_apps` and
     `validated_optional_plugins_not_installed`.

## Validation

- `test_fast_paths_subset_runtime.py`:
  - **The rule:** every optional plugin absent, and the customer's exact
    runtime, are supported; an absent plugin's version is never judged; an
    extra plugin, a missing required one, an unvalidated version and
    unreadable metadata all refuse; the app→distribution map covers the
    validated set exactly.
  - **The gates:** COPY/SQL and the set-based merge enable on a subset and
    refuse an extra plugin; the fast baseline decision enables on a subset.
  - **The hazards:** an uninstalled model resolves to None, not
    `LookupError`; side models skip an uninstalled one; no routing rows needs
    no routing plugin.
- `test_fast_paths_report_when_disabled.py`: a missing optional plugin gives
  no warning; a missing required one is named.
- `test_log_export.py`: the environment lists come from `settings.PLUGINS`.
- Regression: `test_fast_baseline`, `test_copy_sql_apply_engine`,
  `test_set_based_merge`, `test_optional_plugin_versions`, then the full
  `invoke ci` (the paired-branch equivalence tests run with every plugin
  installed).

## Rollback

No migration. Reverting restores the exact-set rule, and with it the slow
first sync on any deployment missing an optional plugin.

## Decision Log

- **Absent optional plugins are allowed; extra plugins are not.** Every raw
  SQL path discovers its related objects live from model metadata. An absent
  plugin only removes relations, receivers and tables. An extra plugin adds
  them where nothing checks, which is the case the rule was always for.
- **CI still validates with every plugin installed.** A subset runtime has
  not been exercised end to end in CI. What is claimed and tested: the gates'
  logic, and that no fast-path code assumes an optional plugin is importable.
