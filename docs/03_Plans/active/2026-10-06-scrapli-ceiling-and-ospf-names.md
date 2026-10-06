# Fix: scrapli startup failure and duplicate OSPF instance names

## Goal

Stop two failures a customer hit upgrading to NetBox 4.7.2: NetBox failing to
start because `scrapli` moved past an API `netbox-validity` needs, and an OSPF
unique-name collision between instances this sync created.

## Constraints

- The `validity` and `integrations` extras gain a `scrapli` ceiling only; no
  plugin version range changes on this lane (it validates one release of each
  optional plugin on NetBox 4.7).
- OSPF instances in the global table keep the name they always had. Instances
  are still matched on (device, vrf, process id), never on the name.
- No migration and no data rewrite in the plugin.

## Touched Surfaces

- `pyproject.toml`, `poetry.lock` (`scrapli>=2022.7.30,<2026.10` in the
  `validity` and `integrations` extras)
- `forward_netbox/utilities/sync_routing_impl.py` (`ospf_instance_name`)
- `forward_netbox/tests/test_ospf_instance_names.py` (new),
  `forward_netbox/tests/test_ospf_drift_comparison.py`

## Approach

`scrapli` 2026.10.3 (2026-10-03) removed `AsyncDriver`. `scrapli_netconf`, which
`netbox-validity` imports, requires only `scrapli>=2022.07.30`, so a fresh
install or an upgrade that picks up the new release stops NetBox during
`manage.py migrate`. The development images already pin `scrapli==2026.2.20`;
the customer-facing extras did not.

`netbox-routing` 0.5.0 enforces unique (device, name) on OSPF instances. The sync
named every instance `<device> OSPF <process>`, so a device running the same
process in two VRFs held two instances with one name, and the plugin's migration
failed on every install already holding such a pair. The VRF is now part of the
name for instances outside the global table (`<device> OSPF <process> (<vrf>)`);
truncation cuts the base, never the VRF suffix. The next sync renames existing
instances in place because they are matched on device, VRF and process id.

## Validation

- New and updated OSPF tests pass; the old unsuffixed name is renamed, not
  duplicated.
- `poetry lock` resolves `scrapli` 2026.2.20.
- Full `invoke ci` pre-push gate.

## Rollback

Revert this branch. No migration and no persisted state.

## Decision Log

- **Name fixed in the sync, not by a migration.** An install already holding the
  duplicate pair fails inside the routing plugin's own migration before this
  plugin runs, so those installs need a one-time rename in the database; it is
  recorded in the release notes.
- **Ceiling, not a pin.** Newer releases below 2026.10 stay allowed.
