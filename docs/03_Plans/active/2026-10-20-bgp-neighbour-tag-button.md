# Tag the BGP neighbour addresses from a button, not a command

## Goal

The Counts explained row for addresses assigned to nothing told the operator to run a shell command to tag the BGP neighbour addresses. Operator-facing text may not tell someone to run a command, and an operator should be able to fix this from the page that shows it. Make it a button.

## Constraints

- Tag-only; nothing deletes. The first run writes one change-logged edit per address, so it stays out of the sync and runs as a job the operator starts.
- The edits are attributed to the person who clicked.
- Only someone who can change IP addresses and the sync sees the button.
- Follows the existing button-job framework, including its parity tests, rather than a new mechanism.

## Touched Surfaces

- `forward_netbox/utilities/sync_facade.py`: the `tag_bgp_neighbour_ips` button-job kind and its permission.
- `forward_netbox/jobs.py`: `_tag_bgp_neighbour_ips_work` and `TagBgpNeighbourIpsJob`.
- `forward_netbox/views.py`: the POST view that queues the job and returns to Health.
- `forward_netbox/api/views.py`: the REST action `tag-bgp-neighbour-ips`.
- `forward_netbox/utilities/explained_findings.py` and `forwardsync_health.html`: the row's `post_url` and the form.
- `docs/01_User_Guide/operations.md`, `forward_netbox/tests/test_bgp_neighbour_tag.py`, `forward_netbox/tests/test_button_jobs.py`.

## Approach

A button job named after its kind runs `tag_bgp_neighbour_ips` as the job's user (the invoking user, else the sync's owner, else a visible failure). The Health row carries a form that posts to the view; the form is shown only when the viewer has `ipam.change_ipaddress` and can change the sync, because NetBox reaches the sync by the same action and the click would otherwise 404. The management command stays for scripting.

## Validation

- `invoke test-isolated --test-label "forward_netbox.tests.test_bgp_neighbour_tag forward_netbox.tests.test_button_jobs forward_netbox.tests.test_explained_findings forward_netbox.tests.test_unassigned_ip_bundle"` (77 tests, OK): the job tags as the clicking user and change-logs to them; a POST queues the job for someone permitted and nothing for someone not; the row offers a button and no `manage.py`; the form shows only with both permissions; the button-job parity tests cover the new kind's API action, permission and runner.
- `scripts/check_operator_text.py`, `pre-commit run --all-files`, `invoke harness-check` and the script tests.

## Rollback

Revert. The row goes back to its text; the management command is untouched.

## Decision Log

- A button job rather than a request-time write: 15,000 edits take about twenty seconds and must not hold a web request.
- The shell command stays in the user guide as the scripting path; the guide is not operator-facing text in the gate's sense and the button is named first.
