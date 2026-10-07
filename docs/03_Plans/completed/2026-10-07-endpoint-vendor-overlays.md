# Name Infoblox and Palo Alto endpoints by vendor, not sysDescr

## Goal

Infoblox appliances were imported as "Linux SNMP Endpoint" and Panorama
appliances under the manufacturer "Palo". The endpoint branch took the first
token of the system description as the manufacturer, and those devices report
`Linux <hostname> ...` and `Palo Alto Networks ...`. Name them by what they are.

## Constraints

- Only the manufacturer, platform and DeviceType of these endpoints change; the
  set of rows the query returns must not.
- Both device queries (`forward_devices.nqe` and the aliases variant) stay in
  step; the query signature does not change.

## Touched Surfaces

- `forward_netbox/queries/forward_devices.nqe`,
  `forward_netbox/queries/forward_devices_with_netbox_aliases.nqe`.
- `forward_netbox/tests/test_endpoints_import.py`.
- `docs/01_User_Guide/operations.md`.

## Approach

`isInfoblox` and `isPaloAlto` now also match the vendor's enterprise OID
(`1.3.6.1.4.1.7779.*`, `1.3.6.1.4.1.25461.*`) and the system description, and the
manufacturer chain tests them before it falls back to the first sysDescr token.

## Validation

- Run live against a customer network with the old and new query: the same 11,288
  rows come back, and only 47 Infoblox endpoints (Linux to Infoblox) and 4
  Panorama endpoints (Palo to Palo Alto Networks) change.
- A source-text test pins the OIDs and the order in both files; 51 endpoint tests
  pass in the isolated stack.

## Rollback

Revert the commit. Devices already moved keep their new manufacturer until the
next sync with the old query.

## Decision Log

- **Match the enterprise OID, not only the profile name.** The profile fallback
  only applied when SNMP returned no system description, which is not the case
  for these appliances.
- **Left the generic-endpoint gate alone.** These endpoints are still imported
  only when generic endpoints are on, as before.
