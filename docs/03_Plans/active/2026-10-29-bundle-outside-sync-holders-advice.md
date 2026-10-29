# Bundle: devices outside the sync, every shared-address owner, advice per recorded outcome

## Goal

A customer's 845 devices without a primary IP against the sync's 702 left 143 with no explanation in the bundle, the bundle named ten of the devices that lost an address to another device's, and 37 "recorded nothing" and 20 "address on no synced interface" devices were told nothing to do. Close all three so the bundle answers them without a question.

## Constraints

- Counts and NetBox primary keys only; no names, no tag names.
- No migration, no Forward call, no change to which address becomes primary.

## Touched Surfaces

- `forward_netbox/views.py`: `_primary_ip_bundle_payload` gains `outside_this_sync` (role, status, scope tags, managed by another sync or none, device keys), `without_primary_ip_device_keys` (every device by reason) and `management_step_outcomes.shared_holders` (each owner with its device count, and every device/owner pair).
- `forward_netbox/utilities/primary_ip_reasons.py`: `outcome_advice` appends what to do for the recorded outcomes a person can act on.
- `forward_netbox/tests/test_explained_findings.py`.

## Approach

The management-address step only handles devices Forward gives a management address or a `Mgmt_` tag, within the sync's tag scope, so "recorded nothing" means Forward gave it neither; the advice says to tag the interface. "No interface" means Forward's address is on an interface the sync did not import. Lists are capped at 5,000 keys and say when they are cut.

## Validation

- `invoke test-isolated` on the explained-findings, log-export and redaction suites.
- The full gate at the 3.0.10 cut.

## Rollback

Revert; the bundle loses the added keys and the advice returns to its previous text.

## Decision Log

- Outside-sync devices are split by another sync versus none, not named: a device another sync manages is not this sync's to reason about, and the split is what tells the reader which it is.
- Advice is written only for outcomes with a known cause; `shared` stays by design.
