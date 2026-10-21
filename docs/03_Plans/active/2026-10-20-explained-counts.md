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
- `forward_netbox/utilities/skip_reason_catalogue.py` (new): a sentence, a fixable label and a next step for each ingestion skip reason, including the open-ended `missing-X` and `still-referenced-by-X`.
- `forward_netbox/views.py`: a drill-down page (`ForwardSyncExplainedView`) listing the devices behind a no-primary-IP or uncovered reason, restricted to the devices the user's own permissions reach, with the scope report's rule for each (in words, for the devices it classifies) and how many runs it has been uncovered; and `forwardsync_explained.html`.
- `forward_netbox/tests/test_explained_findings.py`.

## Approach

Devices without a primary IP are given exactly one reason, in order: tagged uncovered (Forward no longer returns it, or it left the sync's scope), shares an address with another device's primary IP (by design), holds an address but none was chosen (the only one the settings fix), interfaces carry no address, no interfaces synced. The first three come from what NetBox holds; the last two are what the customer's 283 devices with no address at all will fall under. Uncovered devices are split by the sync's stored scope report, which is the authoritative source: devices Forward no longer returns, devices Forward still returns under different tags or as a custom-command source (each with the report's rule in words where it has one), and devices the report does not cover (another sync owns them, or no report yet), which are stated as unknown rather than guessed. A device having an absence record is not used as evidence, because reconciliation writes one for every device it tags uncovered. Unassigned addresses (BGP neighbour versus possible orphan) get rows in the same shape.

The operator has to be able to act, not just read: every row carries a link to a list already filtered to the things to fix (the drill-down page for primary-IP and uncovered reasons, IPAM filtered to addresses with no interface or to the BGP neighbour tag, the latest ingestion's issue list for a model), and the skipped rows in the latest ingestion are grouped by why with the catalogue's explanation. Names appear only on the drill-down page, behind the view permission, never in the bundle.

## Validation

Verification command: `invoke test-isolated --test-label "forward_netbox.tests.test_explained_findings forward_netbox.tests.test_unassigned_ip_bundle forward_netbox.tests.test_bgp_neighbour_tag forward_netbox.tests.test_subinterface_skip_reasons forward_netbox.tests.test_health"` (run on the final tree; the count is in the commit message), then `pre-commit run --all-files`, `invoke harness-check` and the script tests.

Measured against Forward (read-only): of about 10.7k devices with a completed collection, roughly 350 have addresses that every one belongs to another device (firewall virtual systems and HA peers, chiefly) and roughly 50 FortiGate devices have no interface address at all, which is why a sync leaves a few hundred devices with no address in NetBox. The "interfaces carry no address" reason says both.

- Tests: each device lands in exactly one reason and the reasons add up to the count; only the unchosen-address reason is fixable; every reason has a why and a what-to-do; no devices gives no rows; no device names appear in the exported bundle; uncovered devices split into absent, still-in-Forward, unclassified and unrecorded from the stored scope report; a failed absence check or an older report is unclassified rather than "still in Forward"; the Rule column shows words, not codes; a user without device permissions sees no names and a zero count; the combined rows carry all five fields.
- Not measured against the customer's NetBox; their next bundle will show how their 283 split.

## Rollback

Revert. The Health page and bundle go back to the bare counts.

## Decision Log

- The shared-address test compares host addresses with other devices' primary IPs, ignoring VRF and mask, which can call a device "shares" where the same address exists in two VRFs. Counts only, and the wording says "an address it holds", so the cost is a mislabelled reason, not a wrong action.
- A reason table in code, not a free-text field, so the same words appear in the Health page, the bundle and the docs.
- The stored scope report is the only source for the uncovered split; absence records are not evidence, because reconciliation writes one for every device it tags. When the report's absence check failed or predates that check, devices this sync owns are `unclassified` rather than described as still in Forward.
