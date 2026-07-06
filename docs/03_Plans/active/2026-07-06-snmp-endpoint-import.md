# Import Forward SNMP endpoints as NetBox devices

**Date:** 2026-07-06

## Goal
Let operators import Forward **endpoints** — generic SSH/SNMP devices Forward
collects but does not model as first-class `network.devices` (e.g. Avocent
console servers, which carry the same device tags) — into NetBox as devices.
Reported by the design partner: N.Patel-tagged Avocents were not syncing.

## Constraints
- Opt-in (default off); zero change to existing device sync when off.
- Reuse the existing dcim.device pipeline + device-tag scope (endpoints carry
  `tagNames`, so the same include/exclude/match applies).
- No new customer data committed; validated against the ADP demo org only.

## Touched Surfaces
- `forward_netbox/queries/forward_devices.nqe` — union a `network.endpoints`
  branch onto the device query, gated by a `sync_endpoints` param + the standard
  `device_tag_*` scope params. Generic identity from MIB-2 (sysName/sysDescr/
  sysObjectId); per-vendor overlay (Avocent, enterprise OID 10418). Device
  branch output is byte-identical (verified live: `model` is already a String,
  `deviceType` serializes without the enum prefix).
- `forward_netbox/utilities/query_registry.py` — `SYNC_ENDPOINTS_PARAMETER_*`;
  `forward_devices.nqe` added to `DEVICE_TAG_PARAMETER_QUERY_FILES`; seed the
  `sync_endpoints` default (False).
- `forward_netbox/utilities/query_fetch_execution.py` — `ForwardQueryContext`
  `sync_endpoints` field; read from source parameters; inject into any query
  declaring `sync_endpoints` (mirrors `sync_device_tags`).
- `forward_netbox/forms.py` — `sync_endpoints` BooleanField + persistence.
- `forward_netbox/utilities/model_validation.py` — allowlist + bool type check.
- `forward_netbox/tests/test_endpoints_import.py` — new.
- `pyproject.toml`, `forward_netbox/__init__.py`, the three README compatibility
  tables, `CHANGELOG.md` — version bump to 2.3.2 (shipped as a patch: additive,
  off by default).

## Approach
The endpoint branch is a second `foreach` unioned into the device query with
`+`; `where sync_endpoints` makes it emit nothing when off (NQE has no empty-list
literal). SNMP scalars are read with `max(foreach o … where requestedOid == …
select max(foreach e in o.rawOidEntries select e.rawValue))`. The whole query
was validated live against the ADP org (155541): 5123 rows = all devices + 121
Avocent endpoints (manufacturer "Avocent", model "Avocent ACS 8000"/"Cyclades
ACS 6000", role "Console Server", tag-scoped).

## Validation
Live NQE run against ADP; `test_endpoints_import`; `test_query_registry`,
`test_query_binding`, `test_forms` green; full Django suite; harness + sensitive.

## Rollback
Revert the query + wiring. When off (default) behaviour is unchanged, so the
risk to existing syncs is nil.

## Decision Log
- Union into the device query rather than a separate model: endpoints ARE
  devices in NetBox, and this reuses types/roles/scoping with no new contract.
- MIB-2 generic core + per-vendor overlay: works for any SNMP endpoint out of
  the box; Avocent is the first overlay, Infoblox/Palo are a few lines later.
- Opt-in: endpoints are noisy and not every deployment wants them.

## Bundled changes
Optional import of Forward SNMP endpoints (e.g. Avocent console servers) as
NetBox devices, off by default, enabled per source and scoped by device tags.
