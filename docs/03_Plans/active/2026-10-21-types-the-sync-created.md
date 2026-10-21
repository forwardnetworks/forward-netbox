# Show the device and module types the sync created, and what to do with them

## Goal

A customer found 39 used and 201 unused device types, and 45 used and 20 unused module types, in NetBox that are not in the NetBox device-type library. Matching existing types stops new duplicates (separate change), but the ones already created stay, and nothing tells an operator which they are, which have a library equivalent, or how to move off them. List them, say why, and link straight to the devices so the operator can fix them in NetBox.

## Constraints

- Counts and one list page only; type and device names appear on the list page, behind the viewer's permissions, never in the bundle.
- No Forward call and no writes. NetBox's own bulk edit does the moving; this only finds the types and the devices.
- Honest about what is inferred: a type the sync created has no component templates, as does a hand-made one and some library types (virtual appliances). The wording says "looks sync-created".

## Touched Surfaces

- `forward_netbox/utilities/sync_created_types.py` (new): finds device and module types with no component templates, in use or not, and the equivalent type each would match.
- `forward_netbox/utilities/explained_findings.py`: Counts explained rows for them.
- `forward_netbox/views.py`, `forward_netbox/templates/forward_netbox/forwardsync_types.html` (new): the list page.
- `forward_netbox/tests/test_sync_created_types.py`, `docs/01_User_Guide/operations.md`.

## Approach

A type counts as looking sync-created when it has no component template of any kind (NetBox's own twelve template relations on a device type, ten on a module type). Of those, ones in use by this sync's devices are split by whether an equivalent type that does have templates exists, found with the same matching the sync uses (part number, or name ignoring case, punctuation and a vendor name in front, unique): "has an equivalent in NetBox" (the operator can move the devices) and "no equivalent" (not in the library, by design). Types with no templates and no devices or modules anywhere are listed as unused and safe to review for deletion. Each row on the list page links to the devices using the type (`?device_type_id=` and `?module_type_id=`) and to the equivalent, so the move is NetBox's bulk edit of the device type.

## Validation

- `invoke test-isolated --test-label "forward_netbox.tests.test_sync_created_types forward_netbox.tests.test_explained_findings"`, then `pre-commit run --all-files`, `invoke harness-check`, `scripts/check_operator_text.py` and the script tests.
- Tests: a type with templates is never listed; a template-less type in use is split by equivalent or none; an equivalent is only suggested if it has templates and is unique; unused means no devices or modules anywhere; module types behave the same way; every template-less type used by this sync's devices or modules is in exactly one group (one used only by another source's devices is in none, by design); a suggestion is only offered if the sync itself would match it once the old type is gone (another template-less look-alike makes it ambiguous); links go to the device list filtered by the type; a user who may open the sync but not the types sees none; no type names in the explained rows.

## Rollback

Revert. The Counts explained table loses the type rows.

## Decision Log

- "No component templates" is a heuristic, not provenance: NetBox does not record who created a type, and the change log may have been pruned. It is stated as "looks sync-created".
- No move action. Reassigning many devices is NetBox's bulk edit; a guarded move with a preview can follow if the manual route proves too slow.
- Unused types are counted across all of NetBox, since nothing ties an unused type to one sync, and the row says so.
- Moving devices is only half the job: while the old type exists, Forward's model name matches it exactly and the next sync moves the devices back. The advice says to delete the old type straight away, between syncs and not while a branch is waiting to merge.
