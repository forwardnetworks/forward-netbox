# Explain the counts that look wrong

## Goal

A customer's sync leaves counts nobody can read at a glance: 844 devices with no primary IP, 259 uncovered devices, 14,988 addresses assigned to nothing, 283 devices with no address at all. Each took an investigation to say whether it mattered. Every such count should carry a reason in plain words, whether it can be fixed, and the next action, where the count appears.

## Constraints

- Counts only; no device names, addresses or ids in the rows or the bundle.
- Cheap: built only for the Health page and the support bundle, never on every sync page load, and never able to break Health.
- No change to what a sync writes.

## Touched Surfaces

- `forward_netbox/utilities/primary_ip_reasons.py` (new): one reason per device without a primary IP.
- `forward_netbox/utilities/explained_findings.py` (new): the rows for primary IPs, uncovered devices and unassigned addresses.
- `forward_netbox/utilities/health.py`: `explained_findings` in the Health summary (evidence only), and so in the bundle.
- `forward_netbox/views.py`: `without_primary_ip_reasons` in the primary-IP bundle block.
- `forward_netbox/templates/forward_netbox/forwardsync_health.html`: a "Counts explained" table with Finding, Count, Reason, Fixable and Suggested action columns.
- `forward_netbox/tests/test_explained_findings.py`.

## Approach

Devices without a primary IP are given exactly one reason, in order: no longer returned by Forward (tagged uncovered), shares an address with another device's primary IP (by design), holds an address but none was chosen (the only one the settings fix), interfaces carry no address, no interfaces synced. The first three come from what NetBox holds; the last two are what the customer's 283 devices with no address at all will fall under. Uncovered devices and unassigned addresses (BGP neighbour versus possible orphan) get rows in the same shape.

## Validation

- Tests: each device lands in exactly one reason and the reasons add up to the count; only the unchosen-address reason is fixable; every reason has a why and a what-to-do; no devices gives no rows; no device names appear in the rows; the combined rows carry all five fields.
- Not measured against the customer's NetBox; their next bundle will show how their 283 split.

## Rollback

Revert. The Health page and bundle go back to the bare counts.

## Decision Log

- The shared-address test compares host addresses with other devices' primary IPs, ignoring VRF and mask, which can call a device "shares" where the same address exists in two VRFs. Counts only, and the wording says "an address it holds", so the cost is a mislabelled reason, not a wrong action.
- A reason table in code, not a free-text field, so the same words appear in the Health page, the bundle and the docs.
