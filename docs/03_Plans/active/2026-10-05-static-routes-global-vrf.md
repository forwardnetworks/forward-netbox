# Fix: global static routes were never applied, and a scrapli release broke fresh builds

## Goal

Make configured static routes import. On a customer's real estate 6,738 of
9,206 configured routes sit in the global table (no VRF), and the first one
failed row validation, so `netbox_routing.staticroute` was skipped wholesale
on every run: zero routes applied. Also close the two latent breaks found on
the way: an unpinned `scrapli` that stops NetBox booting on a fresh image
build, and a readiness check that reports working adapters as missing.

## Constraints

- A null or empty `vrf` stays an INCOMPLETE identity for every model except
  `ipam.prefix` and `netbox_routing.staticroute`. Widening it further would
  let real malformed rows through for models with no global table.
- A row with the `vrf` key absent, or with `device`, `family` or `args` empty,
  is still refused.
- No change to the bundled query or any published query: the query already
  emits `vrf: null` for the global table, which is correct.

## Touched Surfaces

- `forward_netbox/utilities/sync_contracts.py`
  (`row_coalesce_field_is_complete`)
- `forward_netbox/utilities/diagnostics.py` (one allowlisted reason slug)
- `forward_netbox/utilities/plugin_integrations/registry.py` (adapter
  readiness looks in the routing policy and static submodules)
- `constraints.txt`, `development/constraints-upgrade-from.txt`
  (`scrapli==2026.2.20`)
- `forward_netbox/tests/test_null_vrf_identity.py`
- `tasks.py` and `scripts/tests/test_tasks.py` (main lane only): the
  upgrade-gate from side seeds on NetBox 4.7.0 for every 3.x release

## Approach

`row_coalesce_field_is_complete` answered "is this identity field present" and
special-cased a null `vrf` for `ipam.prefix` only. A configured static route
outside any VRF is an ordinary row, so the same allowance now applies to
`netbox_routing.staticroute`. Existing adapter tests fed rows straight to the
adapter and never went through fetch-time validation, which is why this was
never caught.

The failure was also unreadable: the refusal sentence matched no reason slug,
so the support bundle recorded `unrecognized-fetch-failure` and the cause had
to be recovered by reading source. It now maps to `row-identity-incomplete`.

The readiness check looked for `apply_netbox_routing_staticroute` and the
route-map functions in `sync_routing_impl`, where they do not live, so every
bundle reported `missing_apply_adapter` for working code. It now also searches
the declared sibling modules.

`scrapli` 2026.10.3 (2026-10-03) removed `AsyncDriver`; `scrapli_netconf`, which
`netbox-validity` pulls in, has no ceiling, so a fresh image build failed to
boot NetBox with an `ImportError`. Pin the version `poetry.lock` already holds.

## Validation

- Reproduced against the affected customer's own network: the bundled
  static-route query returns 9,206 rows, all 9,206 parse, and 6,738 have
  `vrf: null`.
- New tests fail before the change and pass after it. 349 related tests pass.
- Full `invoke ci` pre-push gate.

## Rollback

Revert this branch. No migration and no persisted state. A previously skipped
model starts applying, so the first sync after upgrade creates the static
routes; that is the intended behavior, and removal remains limited to routes
carrying this sync's comment marker.

## Decision Log

- **Replaced the per-release upgrade-seed entries with a rule** for every 3.x
  from side. Each new 3.x release failed its first upgrade gate until an entry
  was added by hand (3.0.1 did, and 3.0.2 would have); the rule removes the
  class instead of adding a fourth entry.
- **Fixed the allowance, not the query.** Emitting a placeholder VRF name for
  the global table would have created a VRF that does not exist in NetBox.
- **Fixed the scrapli pin and the readiness check in the same change** rather
  than leaving them for a later release: both were live on this lane.
