# A silently disabled fast path must say so

## Goal

Tell an operator when the fast apply paths are switched off, and by what,
instead of leaving them to notice a slow sync.

## Why

Three subsystems refuse to run unless the installed plugin set exactly matches
a validated tuple:

| subsystem | gate | cost when unmatched |
|---|---|---|
| COPY/SQL apply engine | `COPY_SQL_SUPPORTED_PLUGIN_APPS` | falls back to bulk ORM |
| set-based merge | `SET_BASED_MERGE_SUPPORTED_PLUGIN_APPS` | falls back |
| fast baseline | `fast_baseline._runtime_decision` | **hours instead of minutes** on a first sync |

Failing closed is correct: their SQL is generated against a known schema, and
`fast_baseline`'s own comment records a customer who lost the fast baseline by
upgrading one optional plugin - "no error, just a first sync that takes hours
instead of minutes". What is missing is any way for that customer to find out.
The decision objects carry `unsupported_plugin_app_tuple` and
`unsupported_runtime_tuple`; nothing displays them, so the only route to the
answer is noticing the slowness and reading engine internals.

This surfaced while registering an unrelated optional integration: adding one
plugin to the runtime turned six fast-baseline tests red. A deployment gets no
equivalent signal. Any customer installing any NetBox plugin this release has
not validated is one `pip install` from the same silent degradation.

## Constraints

- **Silence on a healthy runtime.** A check that fires routinely is one
  operators learn to scroll past, which would cost more than it buys.
- Name the unexpected plugin, not the two tuples. The actionable fact is which
  plugin, and both tuples are long.
- Never break the page: a diagnostic that raises is worse than the condition it
  reports.
- Warn, never fail. Syncs still succeed; they are only slower.

## Touched Surfaces

- `forward_netbox/utilities/health.py` - `_fast_path_runtime_check`, appended
  to the sync health checks
- `forward_netbox/tests/test_fast_paths_report_when_disabled.py` (new)
- `.pre-commit-config.yaml` - reorder-imports exclusion

## Approach

Ask each subsystem its own question - the fast baseline through its runtime
decision, the other two by comparing the installed plugin set to their
allowlists - and report the union. The message names the plugins present but
unvalidated, or validated but absent, states the consequence in words rather
than a reason code, and says explicitly that syncs still succeed.

Returns `None` when every fast path is available, so the row exists only when
it means something.

## Validation

`test_fast_paths_report_when_disabled`: a validated runtime produces no check
at all; an extra plugin is named along with the cost and the reassurance; a
missing validated plugin is named; and a fast-baseline probe that raises still
yields a usable warning rather than an exception.

## Rollback

Revert. The fast paths behave exactly as before either way - this change only
reports.

## Decision Log

- **Warn, not fail.** The runtime is degraded, not broken, and a check that
  blocked anything here would be worse than the silence it replaces.
- **Silent when healthy**, asserted first in the tests, because that property
  is what keeps the warning worth reading.
- **Union of three subsystems in one row** rather than three rows: they share
  one cause, and an operator fixes them together.
- **Reuse each subsystem's own gate** rather than reimplementing the comparison,
  so this cannot drift into disagreeing with the engines it reports on.

## Open

- The validated plugin set is currently spelled out in five places (two engine
  allowlists, the fast-baseline expected tuple, its separate actual probe list,
  and the version-test fixtures). Each has to be updated by hand when an
  integration is added, and a miss fails closed silently - which is the same
  class of defect this check reports. Deriving them from one source is the
  real fix and is not attempted here.
