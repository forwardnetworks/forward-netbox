# Match existing NetBox device and module types instead of creating duplicates

## Goal

A customer found 39 used and 201 unused device types, and 45 used and 20 unused module types, in NetBox that are not in the NetBox device-type library, created by the sync. Against the public library, roughly a third of the device types are naming mismatches: Forward reports `Opengear CM7148-2-DAC` where the library has `CM7148-2-DAC`, `FPR-2140` where it has `FPR2140-NGFW-K9`, and so on. The sync matches only an exact manufacturer plus model or slug, so any difference creates a new type. Match the types NetBox already holds when the difference is only punctuation, case, a vendor prefix or a part number.

## Constraints

- Exact matches are untouched, so nothing changes for a type that already matches today, including the types the sync created earlier. This prevents new duplicates; it does not merge existing ones.
- A match must be unambiguous: exactly one existing type under that manufacturer. Two candidates, or none, leaves the row as it is and a type is created as today. No fuzzy "contains" matching.
- A matched type is never written. Today a match overwrites the type's model, slug and part number with Forward's values, which would rename a library type, so a matched device type's own row is dropped from the fetch (the type exists, so there is nothing to apply) and a matched module type's description and comments are carried over from the type itself.
- Delete rows are never rewritten: a rewrite there could point a delete at a library type.
- No Forward calls, no new sync setting, no migration.

## Touched Surfaces

- `forward_netbox/utilities/netbox_type_matching.py` (new): the index of existing types, the match rules, and the row rewrite for `dcim.devicetype`, `dcim.device`, `dcim.module` and `netbox_dlm.hardwarenotice`.
- `forward_netbox/utilities/query_fetch_execution.py`: rows are matched right after the NQE fetch, before scope filtering and removal computation, and on each diff and shard exit before that exit's filters, never on `delete_rows`. A skipped device-type row goes through the same scope and shard filters as the rest and is counted only if it would have survived them; it is never part of a removal.
- `forward_netbox/utilities/dlm_notice_audit.py`: the audit's rows are matched like the sync's.
- `forward_netbox/tests/test_netbox_type_matching.py`, `docs/01_User_Guide/configuration.md`.

## Approach

Point the fetched row's type fields at the existing type's own model, slug and part number before anything else sees the row, so a device's type lookup (including the bulk path), module types and hardware notices resolve to the existing type through code that already works. A device type's own row is dropped rather than rewritten: rewriting it would store a library type's identity in the durable workload state, and a later run that stopped matching would turn that identity into a delete of a library type. Order: exact (leave as is), then a unique part-number match, then a unique match on the normalized name (lowercase, letters and digits only, with and without a leading vendor name). Hardware-notice rows carry no manufacturer, so they match across all manufacturers and only when the answer is unique, and a vendor name written in front of the model only matches a type of that vendor ("Dell 3048" never lands on another vendor's "3048"). Matching is deterministic: it does not depend on set order. The notice audit, prune and report apply the same matching so they see the rows the sync writes. The matching is deterministic given NetBox's state, so a run is stable.

## Validation

- `invoke test-isolated --test-label "forward_netbox.tests.test_netbox_type_matching"` plus the apply, device-type and drift suites that exercise `_ensure_device_type`, then `pre-commit run --all-files`, `invoke harness-check`, `scripts/check_operator_text.py` and the script tests.
- Tests: an exact type is untouched; a library type under a differently written name is reused and not renamed or overwritten; ambiguity and a missing manufacturer leave the row alone; the match is idempotent and does not mutate its input; each of the four models is handled (device types dropped, device, module and notice rows pointed at the existing type); a matched module type keeps its description and comments; a vendor in front of a model matches only that vendor's type; two vendor names that both prefix a model never change the answer; the notice audit reads matched rows; a short key is never matched. Delete rows are never passed to the matcher, by the call sites; the full Django suite is the regression check for the fetch seam.

## Rollback

Revert. Rows go back to Forward's strings and the sync creates a type for any name it does not already hold.

## Decision Log

- Prevention only. Moving devices off the duplicate types the sync already created is a data change to many devices, so it comes later as a guarded action with a preview.
- Hardware notices match globally, only when unique, because their rows carry no manufacturer by design.
- The part-number rule uses the library's own `part_number` field, which is how Forward's part-number model strings line up with library models.
- Matching does not change how components are created. Hardware-notice rows are matched on their own (they carry no manufacturer), so an ambiguous notice is skipped even when its device type matched; documented rather than coupled to the device-type match.
- Skipped device-type rows are reported in the fetch metadata and counted in the model's row total, so the shrink and zero-row guards still see Forward's whole result.
- The part-number rule ignores values that are placeholders or as long as the module query's truncation (a truncated description, not a part number).
