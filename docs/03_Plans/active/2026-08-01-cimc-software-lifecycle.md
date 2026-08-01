# CIMC Software Lifecycle

## Goal

Report the firmware running on each Cisco CIMC management controller into
`netbox-dlm` 0.6.0's `InventoryItemSoftware`, so a management controller gets
the same lifecycle and vulnerability treatment a device's OS already gets.

## Contract

- A CIMC inventory item we created gains an `InventoryItemSoftware` row naming
  its firmware version.
- An endpoint whose firmware version cannot be read produces no row. A version
  is never inferred, defaulted, or carried over from another endpoint.
- The retained `supported_package_versions` mechanism is not changed by this
  work. This delivery targets the customer's `netbox-dlm` 0.6.0 deployment;
  no additional behavior or validation is required for older DLM releases.

## Constraints

- Persisted diagnostics stay free of customer data.
- No new NQE call volume per sync beyond the one added map. Forward engineering
  has already objected to unnecessary NQE runs; the firmware rides on SNMP
  output Forward already collects, so no new collection is requested.
- The map is opt-in, matching every other lifecycle map.

## Touched Surfaces

- `forward_netbox/queries/forward_dlm_inventory_item_software.nqe` - new
- `forward_netbox/utilities/query_registry.py` - register the map, disabled
- `forward_netbox/utilities/sync_dlm.py` - apply adapter
- `forward_netbox/utilities/sync_contracts.py` - model contract
- `forward_netbox/utilities/plugin_integrations/registry.py` - supported models
  and query maps, *not* required models
- `forward_netbox/utilities/branch_budget.py` - per-row budget
- `forward_netbox/utilities/fast_baseline.py` - inspect the required-field
  contract; add entries only if this path writes either new model
- Tests.
- This plan.

## Approach

**Where the version comes from.** Probed live against the customer network before
designing anything. Forward collects sysDescr (`1.3.6.1.2.1.1.1`) on CIMC SNMP
endpoints, and it carries the firmware verbatim:

    Cisco Integrated Management Controller(Cisco IMC) <model>, Firmware Version
    <version>, Copyright (c) <years>, Cisco Systems, Inc.

`Firmware Version ([^,]+)` extracted all five distinct values present, across
two hardware models, in both the short (`4.1(3c)`) and long (`4.3(2.230270)`)
forms. One of the nine endpoints observed had no sysDescr at all, which is why
absence has to be a skip rather than a fallback - the existing inventory map can
fall back to the literal string "Cisco CIMC" for a *label*, but there is no
honest fallback for a *version*, and writing one would report a controller as
running firmware it may not be running.

**How it attaches.** `InventoryItem` has no platform field, so 0.6.0 added
`InventoryItemRolePlatform` to declare which `Platform`'s versions apply to a
given `InventoryItemRole`. `InventoryItemSoftware.clean()` enforces that
mapping. We already create the CIMC inventory item with role
`management-controller` via `Forward CIMC Endpoint Inventory`, so the adapter
creates `Platform` "Cisco CIMC", the role mapping to it, the `SoftwareVersion`,
and the `InventoryItemSoftware` link - all four consistently, or the plugin's
own validation rejects the write.

**The compatibility trap.** `DLM_INTEGRATION.required_models` gates whether the
whole DLM integration engages. Adding the two new models there would mean every
customer still on 0.4.1 or 0.5.0 silently loses OS lifecycle, hardware notices,
CVEs and vulnerabilities the moment they upgrade us. The new models therefore go
in `supported_models` only, and the adapter skips its map when they are absent.
This is the same failure shape as the fast-baseline version pin: a tightening
that reads as correctness but degrades a working install with nothing visible.

The same trap turned out to be live already, one layer down.
`required_package_version` is compared with strict equality, and
`_drop_unavailable_integration_models` skips *every* model of an integration
whose version does not match. Pinned at `0.4.1`, it stranded anyone on `0.5.0`
even though the fast baseline explicitly validated `0.5.0`; the two surfaces
disagreed. Simply moving the pin to `0.6.0` to accept the new models would have
stranded everyone below it. That field is being widened to a supported-version
set, which is a prerequisite for this work rather than part of it.

**Dependency on the inventory map.** Software rows can only attach to inventory
items the CIMC endpoint map created. That map is opt-in and off by default, so
this one is too, and a row whose inventory item does not exist is skipped and
counted rather than raising.

## Validation

- Extraction is unit-tested against the five real sysDescr shapes observed
  (recorded as fixtures with customer names removed), plus a missing-sysDescr
  case and a sysDescr with no firmware clause, both of which must yield no row.
- A test asserts the role mapping and software version agree, so
  `InventoryItemSoftware.clean()` passes.
- Full local gate.

## Rollback

Revert. The map is opt-in and additive; no existing row is modified, so
disabling it leaves the CIMC inventory items exactly as they are today.

## Decision Log

- 2026-08-01: Probed the live network for the sysDescr format rather than
  regexing against a remembered Cisco format. The long/short version forms
  differ enough that a guessed pattern would have parsed some and silently
  dropped others.
- 2026-08-01: Used `regexMatches` with a named capture rather than
  `patternMatch`, against the general preference for pattern matching over
  regex. Both were run live against the same 46 controllers. `patternMatch`
  matched all 46, but a pattern is a whitespace-tokenised sequence and
  `{version:string}` matches "any sequence of characters up to the next
  whitespace", so it returned `4.1(3f),` — the delimiter included — for every
  row. sysDescr is comma-delimited prose rather than a whitespace-tokenised
  config line, which is the case patterns are built for; there is no pattern
  expression that stops at a comma, so the pattern form needs a second
  strip step and still cannot be expressed without a regex. `regexMatches`
  with `(?<version>[^,]+)` returns the exact token in one step.
- 2026-08-01: New models are `supported`, not `required`. Requiring them would
  break every customer below 0.6.0.
- 2026-08-01: Missing firmware is a skip. There is no safe default for a version
  that feeds vulnerability matching.
- 2026-08-01: The 0.6.0 customer deployment is the acceptance target. Keep the
  existing supported-version follow-up, but do not add old-version compatibility
  branches or tests for this CIMC capability.

## Open

- Whether the same treatment should extend to the ACI APIC CIMC inventory map,
  which sources from a custom command rather than SNMP and so has a different
  version source. Not designed.
- The customer reports SNMP endpoints not covered by an import Forward tag. The CIMC
  endpoints here are SNMP endpoints; the two may be the same gap. Not
  investigated.
