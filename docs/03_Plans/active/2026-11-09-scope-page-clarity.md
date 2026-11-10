# Scope Page Clarity

## Goal

An operator reading Scope Reconciliation cannot mistake the devices this sync never created for uncovered devices. The page shows the split as two rows and the sum only as explanatory text, and the cleanup checklist covers two cases it was silent on.

## Constraints

- No data model, query or migration change; the stored report keys (`unmanaged.untagged_total`, `owned_untagged`, `unclaimed`) are unchanged.
- The support bundle must carry every figure the page shows, under the page's label, plus the total.
- No customer identifiers in code, tests or docs.

## Touched Surfaces

- `forward_netbox/templates/forward_netbox/forwardsync_scope_reconciliation.html`: replace the summed row with `Uncovered: created by this sync` and `Not managed by this sync`; the total becomes a sentence.
- `forward_netbox/utilities/reconciliation_strip.py`: parent label `No include tag (all)`, text that says it is a total.
- `forward_netbox/utilities/bundle_label_index.py`, `bundle_ui_figures.py`: new labels and values; the total is kept as `No include tag (all)` with its parts.
- `forward_netbox/utilities/health.py`: the uncovered message names the new row.
- `forward_netbox/utilities/cleanup_checklist.py`, `forward_netbox/views.py`: items for generic SNMP endpoints this sync created and for devices whose primary IP the management step recorded nothing for.
- `docs/01_User_Guide/troubleshooting.md`.

## Approach

Rows follow `owned_untagged` (needs attention, existing links and trend) and `unclaimed` (informational). The generic-endpoint item counts `endpoint_generic` in `unmanaged.owned_absence.endpoint_detail` and is subtracted from the add-a-tag item so no device is counted twice. The unrecorded primary-IP item counts bare devices classified `has_an_address_but_none_was_chosen` with no recorded outcome.

## Validation

- `invoke test-isolated --test-label forward_netbox.tests.<module>` for `test_scope_page_clarity`, `test_reconciliation_strip`, `test_log_export`, `test_bundle_label_index`, `test_cleanup_checklist`, `test_scope_reconciliation_view`.
- `invoke harness-check`, `invoke lint`.
- Live: open Scope Reconciliation on the local stack and confirm the two rows and the sentence.

## Rollback

Revert the commit; no state to clean up.

## Decision Log

- Kept the total in the bundle (as `No include tag (all)`) so a reader searching by number still finds it.
- Left the strip's child labels as they were; only the parent was renamed.
