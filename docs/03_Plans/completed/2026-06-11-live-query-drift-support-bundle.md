# Live Query Drift Support Bundle

## Goal

Surface a live Forward repository check for direct query-ID bindings on the
Forward Sync health page and in the sync support bundle so operators can tell
whether a saved query ID is still resolving in the live org repository.

## Constraints

- Keep the existing local drift summary unchanged so offline diagnostics remain
  available even if Forward lookups fail.
- Do not change sync execution behavior or binding semantics.
- Avoid leaking query text, credentials, network IDs, or other customer data
  into the new diagnostics payload.
- Keep the support-bundle output compact and JSON-safe.

## Touched Surfaces

- `forward_netbox/views.py`
- `forward_netbox/utilities/execution_ledger_serialization.py`
- `forward_netbox/templates/forward_netbox/forwardsync_health.html`
- `forward_netbox/tests/test_health.py`
- `forward_netbox/tests/test_log_export.py`

## Approach

1. Reuse the existing live query-drift lookup helper so the health page and
   support bundle can report live Forward repository resolution results.
2. Add a compact live query-drift summary that highlights direct query-ID
   warnings, lookup errors, and status counts.
3. Render the live summary on the health page alongside the existing local
   query-drift diagnostics.
4. Include the same live diagnostics payload in the sync support bundle so
   customer troubleshooting can use one archive instead of a separate export.
5. Keep all local drift behavior and refresh actions intact.

## Validation

- `invoke harness-check`
- `invoke harness-test`
- focused health and log-export regression tests
- manual render of the Forward Sync health page
- support-bundle JSON inspection for the new live diagnostics payload

## Rollback

Remove the live diagnostics field from the health view/template and sync
support bundle, leaving the existing local query drift flow unchanged.

## Decision Log

- Chosen: add a live Forward repository check instead of reclassifying the
  local query-ID status, because the local status is intentionally unverified
  and still useful when Forward lookups are unavailable.
- Rejected: changing sync execution or binding semantics, because the bug is in
  operator visibility and supportability rather than row application.
