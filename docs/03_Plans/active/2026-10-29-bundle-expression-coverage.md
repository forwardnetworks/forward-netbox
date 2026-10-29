# Bundle: close the figures the pages show that the bundle did not hold

## Goal

The label index covered table rows with fixed labels, and nothing else on the pages. Compare every template expression on the Health, Scope Reconciliation and Drift report pages against a real 3.0.9 bundle, extend the comparison to the ingestion detail and Module Bay Plan pages, and put what is missing into the bundle.

## Constraints

- Counts and paths only; no device, bay or tag names. No migration. No Forward call.

## Touched Surfaces

- `forward_netbox/views.py`: `latest_ingestion.snapshot` and `snapshot_metrics` (the ingestion page's snapshot rows and "Forward Snapshot Metrics" card), `module_readiness` (the Module Bay Plan counts), `scope_reconciliation.catalogue_cleanup` (the "Routing Policy Outside Scope" card).
- `forward_netbox/utilities/bundle_label_index.py`: rows for the Ingestion and Module readiness pages, and `PAGE_CARD_PATHS` (every card heading to the bundle path that holds it), shipped as `ui_card_index`.
- `forward_netbox/tests/test_bundle_label_index.py`, `test_log_export.py`.

## Approach

Of 383 expressions on the three pages, all but the dropped name samples, one dead row (`step_query_parameters`) and one message field resolved in the real bundle. The gaps were whole sections: the ingestion snapshot state and metrics (collection failures per device), the Module Bay Plan card, and the routing policy cleanup count. A card heading is a fixed string, so a guard on cards covers the tables labelled from data and the figures written inside sentences that a row label cannot.

## Validation

- `invoke test-isolated` on the label-index, log-export and redaction suites.
- The guard fails when a card has no entry or an entry has no card.
- Not covered: value equality between page and bundle; the dependency preview, audit, delete and types pages; ingestion pages other than the latest.

## Rollback

Revert; the bundle loses the added keys.

## Decision Log

- Cards are indexed by heading, not by row, because their content is data-labelled.
- Module readiness exports counts only: its device and bay lists are names.
