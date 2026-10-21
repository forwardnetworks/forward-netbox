# One page for every device without a primary IP, by role and reason

## Goal

A customer saw 844 devices without a primary IP and asked how many were routers and switches (43). The Counts explained table gives a reason per device and the drill-down lists one reason at a time, so nobody could see the devices of one role across all reasons, or read what each reason means in one place. Add that view, with plain help on how to read it.

## Constraints

- Counts and the legend are help text; device names appear only on this page, behind the viewer's permissions, and never in the bundle.
- The role filter must count and cut after the viewer's own device-permission restriction.
- No change to how reasons are decided; this reuses `classify_primary_ip_devices`.

## Touched Surfaces

- `forward_netbox/views.py`: `ForwardSyncExplainedView` gains `reason=all` and a `role` filter with role chips.
- `forward_netbox/utilities/primary_ip_reasons.py`: a short title for each reason.
- `forward_netbox/templates/forward_netbox/forwardsync_explained.html` and `forwardsync_health.html`: the role chips, a Reason column, the "how to read this" legend, and a link from Counts explained.
- `forward_netbox/tests/test_explained_findings.py`, `docs/01_User_Guide/operations.md`.

## Approach

`reason=all` lists every device the sync manages without a primary IP, each with its reason title. Role chips (role name and count) filter by `?role=<pk>`, and the same filter works on every single-reason list. A legend under the header explains how to read the page and lists each reason present with its count, whether it is fixable and what to do, using the same wording as the table.

## Validation

- `invoke test-isolated --test-label "forward_netbox.tests.test_explained_findings forward_netbox.tests.test_unassigned_ip_bundle forward_netbox.tests.test_bgp_neighbour_tag forward_netbox.tests.test_health"`, then `pre-commit run --all-files`, `invoke harness-check`, `scripts/check_operator_text.py` and the script tests.
- Tests: the page lists every reason's devices with the reason; the role filter narrows to that role and the chips count the permitted devices only; an unknown role value is ignored; the legend names each reason present once; a user who may open the sync but not the devices sees none and a zero count.

## Rollback

Revert. The single-reason drill-down pages are unchanged.

## Decision Log

- The role filter is a query parameter on the existing page rather than a new view, so the drill-down and the all-devices page share one template and one permission path.
- The filter is by role because that is how the customer slices the estate (routers and switches against firewalls and console servers); a site or device-type filter can follow.
