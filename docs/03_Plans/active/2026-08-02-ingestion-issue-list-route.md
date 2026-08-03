# Ingestion Issue List Route

## Goal

Make opening an ingestion issue work instead of returning a 500.

## Contract

- Every model exposing a detail view can reverse its list route.
- The issue list is read-only; issues are written by a sync, never by hand.

## Constraints

- No new navigation entry: an issue is reached from its ingestion.

## Touched Surfaces

- `forward_netbox/views.py` - `ForwardIngestionIssueListView`
- `forward_netbox/urls.py` - the `detail=False` include
- `forward_netbox/tests/test_model_view_routes.py` - new
- This plan.

## Approach

A customer clicked an ingestion issue and got a Server Error:

    NoReverseMatch: Reverse for 'forwardingestionissue_list' not found.

NetBox's object view reverses `<app>:<model>_list` for its breadcrumb, so
registering a detail view without a list view is a 500 rather than a missing
page. It happened on the one page that exists to explain a failed merge.

**Registering the view is not sufficient.** Every other model has two URL
includes - a `detail=False` one for the list and a `<int:pk>` one for the
detail - and `forwardingestionissue` only ever had the detail include, so a
registered list view has no URL to attach to. The first attempt at this fix
registered the view alone and the regression test still failed, which is how
that was caught.

**This class has shipped twice.** In 2.6.3 the Ingestions list raised
`NoReverseMatch` for `forwardingestion_delete`, and the response was to remove
the action pointing at the missing view rather than register it - which left
ingestions undeletable until 2.6.6. Registering the route is the fix; deleting
the caller is not.

**Why the existing probe missed it.** The installed-route probe renders the
plugin's *menu* lists. This model has no menu entry, so it was never in the
population being checked. The guard therefore sweeps every registered model
rather than every menu item.

## Validation

- A test reverses the list route for every plugin model that exposes a detail
  route, and reports the offenders by name.
- A second test pins `forwardingestionissue` explicitly, so the sweep passing
  because the model stopped exposing a detail view would not hide a regression.
- Both fail against the unfixed tree and pass against this one: 2 tests OK.

## Rollback

Revert. Opening an ingestion issue returns a 500 again.

## Decision Log

- 2026-08-02: Swept every registered model rather than adding one route test.
  The defect is a missing pairing, and it has now occurred for two models.
- 2026-08-02: Read-only list. Issues are diagnostic evidence written by a sync;
  offering edit or delete actions would invite editing the record of a failure.

## Open

- The installed-route probe still only covers menu lists. Extending it to every
  registered detail view would catch this at the artifact rather than in tests.
