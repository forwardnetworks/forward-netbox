# Show how the device numbers fit together

## Goal

A customer compared 3,862 device rows in the drift table with the number of devices in NetBox, in Forward's whole network, and with the 844 without a primary IP, and could not tell why they differ. Each number is right for what it counts, but nothing says what it counts. Show, at the top of the Health page, how the device numbers fit together: Forward's tag scope, the devices in it, what NetBox holds, and where the rest went.

## Constraints

- No new Forward call. Everything comes from the stored scope report, which Scope Reconciliation already produces, so the strip is free to render. It says when the report was made and links to refresh it.
- No contradiction of the Scope Reconciliation page: the labels are that page's own (In scope (collected), In scope (SNMP endpoints), In scope, missing from NetBox, Tagged but backfilled, Carry no include tag, created by this sync, not claimed by this sync).
- Counts only; no device names.
- Only identities that hold by definition are stated as arithmetic: the devices carrying no include tag are the ones this sync created plus the ones it never owned, and the named NetBox devices minus the ones carrying no include tag are the ones matched to a tagged Forward device. The report counts some things by device and some by distinct name, so it now also records the named-device total (`netbox_named_device_total`), the count the subtraction is valid over; an older report without it shows no matched count. Everything else is described, not added up.

## Touched Surfaces

- `forward_netbox/utilities/reconciliation_strip.py` (new): turns the stored scope report into the steps.
- `forward_netbox/utilities/health.py`: `reconciliation` in the Health summary (evidence only).
- `forward_netbox/templates/forward_netbox/forwardsync_health.html`: a "How the device numbers fit together" card above Counts explained.
- `forward_netbox/tests/test_reconciliation_strip.py`, `docs/01_User_Guide/operations.md`.

## Approach

The strip makes no statement about what happens to an uncovered device, because the sync's own run removes devices Forward has dropped after a quarantine and a prune option removes devices that left the tag scope; it points to Scope Reconciliation for that. Two groups, in the Scope Reconciliation page's own labels. Forward: devices in scope (collected), SNMP endpoints in scope, of the collected ones the ones missing from NetBox, and a separate group, tagged but backfilled, which are not fetched and not in the numbers above. NetBox: the named devices, the ones matched to a tagged Forward device, and the ones that carry no include tag, which split into created by this sync (the uncovered devices) and not claimed by this sync. The out-of-scope orphans are shown as their own count under "created by this sync", not as the same set: they are devices the sync previously managed whose name is not in Forward's tagged set. After a sync's tag step the two normally match, and they can differ for devices that became uncovered since. Each line has a plain meaning; the tag names are kept in their own field rather than in the text. A report that is missing, produced nothing or is old is said so, with a link to run Scope Reconciliation.

## Validation

- `invoke test-isolated --test-label "forward_netbox.tests.test_reconciliation_strip forward_netbox.tests.test_health forward_netbox.tests.test_explained_findings"`, then `pre-commit run --all-files`, `invoke harness-check`, `scripts/check_operator_text.py` and the script tests.
- Tests: the steps carry the report's own numbers under the page's labels and are not summed; matched uses the named-device total (not the distinct-name count) and is withheld for an older report; backfilled is a separate group; orphans are their own count and can be 0; every report key the strip reads is still produced by the report; a missing report, an error and an old report are each reported honestly; the light summary does not build it; a failure building it gives an unavailable strip instead of breaking Health; the Health page renders it; no device names are in the payload.

## Rollback

Revert. Health goes back to the table of checks and Counts explained.

## Decision Log

- Orphans (devices this sync previously managed, from a scope claim or a recorded absence) and "created by this sync, no include tag" (devices it holds an identity for) overlap heavily but are defined differently, so they are separate lines. Earlier drafts treated them as one, then claimed the orphan count could read 0 while hundreds are uncovered; both were wrong once a post-sync tag step has recorded absences, and were corrected after review.
- Backfilled devices are not part of the in-scope count (that is collected devices and endpoints only), so they are a sibling line, not a child.
- No total for Forward's whole network: the report does not carry it and a fetch of every device just to count them is not worth a Forward call. The strip says the scope is why the in-scope number is smaller than the network.
- The in-scope number is described as matching the drift table's Forward rows for the same snapshot, not asserted to: that table counts rows, and other maps can feed the same model.
