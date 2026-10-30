# Failed syncs explain themselves, NQE failures retry, device lists paginate and act

## Goal

One customer's sync failed and nothing on screen said why: four NQE queries finished with a non-success outcome inside Forward, the ingestion issue named only the line that raised, the blocked validation run was not linked from the ingestion, and the bundle carried a count of reasons but not the reasons. The same customer then hit a page that showed 25 of 190 devices with no paginator, offered no bulk action, could not delete a device because of netbox-routing, and read the Scope page's counts as faults. Fix each, so the screen says what is wrong and what to do.

## Constraints

- No customer names, network ids, hostnames or addresses in the repo, plans, tests or docs. Tests use synthetic names.
- No migration. No change to what the sync writes. Forward is still reached only through the SDK, and forward-sdk 0.1.29 is the new pin.
- A delete path removes only what an explicit rule names: routing rows (the `netbox_routing` app, plus the rows the duplicate repair already releases). Anything else that protects a device refuses the delete and rolls everything back.
- The support bundle stays name-free: model labels, counts, outcome slugs and device primary keys only.
- NetBox's "select all N matching" must never reach devices outside the list the user was looking at.

## Touched Surfaces

- `forward_netbox/utilities/forward_api_impl.py`, `forward_client_errors.py`, `diagnostics.py`, `forward_usage.py`: resubmit a query that finished with a system-error outcome (2 retries, backoff, within the per-workload deadline) and carry the outcome as a reason slug.
- `single_branch_executor.py`, `validation.py`, `validation_guidance.py` (new), `sync_orchestration.py`, `models.py`, the validation run template: hold the blocked run so the failed ingestion links it, say what to do for each blocking reason, and point the issue at the run.
- `health_summary_blocks.py`, `ingestion_presentation.py`: the blocking-reason sentences in the bundle.
- `views.py`, `urls.py`, `device_list` and `device_bulk_delete` templates, `device_cascade_delete.py` (new), `scope_reconciliation.py`: the four device lists as children views with pagination, bulk Edit and Delete; select-all held to the stored list; opt-in routing cascade on delete; a derived list of unclaimed devices that share a name with a managed device.
- Scope Reconciliation, Health and Explained wording and reconciliation lines; `bundle_label_index.py`.
- `constraints.txt`, `pyproject.toml`, `poetry.lock`, `scripts/validate_sbom.py`: forward-sdk 0.1.29.

## Approach

1. Retry only the outcome Forward reports as an internal failure (`SYSTEM_ERROR`); a timeout has spent its budget and a user error fails the same way again.
2. Restate the outcome in the plugin's own words so a message, issue and bundle can name it without quoting Forward.
3. Create the validation run before it is judged, so the executor holds it even when a block raises.
4. Release routing rows through the existing `_release_routing_rows_protecting` machinery with a wider predicate, inside the delete's own transaction.
5. Convert `_all` to the stored report's device keys on plugin-owned bulk views; refuse it without a stored list.

## Validation

- Targeted suites for each area, then the full Django suite and `scripts/tests`, `pre-commit`, `invoke harness-check`, and the release gate at the cut.
- Tests derived from the requirements: retry then succeed, no retry for other outcomes, exhaustion names the outcome; blocked run linked from the ingestion; every reason validation writes has a next step; select-all is held to the list; a refused delete explains itself; the cascade removes routing rows then the device, and a non-routing blocker refuses and restores them.
- Not verified: the retry against a live Forward failure (none can be provoked on demand), and the bulk views through a real browser.

## Rollback

Revert the release. Nothing is migrated; the new bundle fields are additive and an older plugin ignores them.

## Decision Log

- The cascade is opt-in on the confirmation page. Unticked, the delete is NetBox's own, so nothing changes for anyone who does not choose it.
- The cascade covers routing rows in general rather than only OSPF, but only a row that hangs off a device being deleted and no other (found by following the row's own links to a device, interface or virtual device context). Review found the first version released any `netbox_routing` row Django named: a BGP peer on another device that uses a deleted device's address as its neighbour protects that address, and is not the deleted device's to remove. Unknown owner refuses. The duplicate-device repair keeps its own narrower list.
- The cascade runs in a transaction on the write connection the active branch uses (`router.db_for_write`), up-front checks that the user may delete every selected device, ignores the background-job box, and clears queued events when it rolls back, so an undone delete sends no webhook.
- A user who may delete a device may remove its routing rows through this action without a separate netbox-routing permission: the rows are the device's own and go with it. Accepted.
- Retries live in the plugin's wait loop, not `nqe.query(outcome_retries=...)`, because the plugin needs its own paging, deadline and usage counters around `execute()`.
