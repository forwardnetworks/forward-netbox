# Show a Site's Forward Network Map on its page

## Goal

A team keeps a live Forward Network Map per branch in place of an offline drawing. Let an operator reach the map for a NetBox Site from the Site page, without storing or syncing anything, now that forward-sdk 0.1.27 can list maps.

## Constraints

- Forward is reached only through the SDK. The maps routes are unpublished in the SDK, so a refusal, a missing route and a changed response shape must degrade to a note and never break the page.
- Read-only: no Forward write, no migration, no model, no job.
- A page view must not wait on Forward: 5-second timeout, no retries, one list call (plus the folder list) per source per 60-second cache window, failures cached too.
- Only sources the viewing user may see are asked.
- The list has no site field, so the match is by exact case-insensitive name and the panel says so.
- No customer names, network ids, hostnames or screenshots in the repo, tests or docs; tests use synthetic names.

## Touched Surfaces

- `forward_netbox/utilities/forward_api_impl.py` and the `forward_api.py` shim: `get_network_maps`, a `ForwardClient(source, timeout=, retries=)` override.
- `forward_netbox/utilities/forward_client_errors.py`: the translated API error carries `status` so a refusal is told from a failure without parsing text.
- `forward_netbox/utilities/network_maps.py`, `forward_netbox/template_content.py` (`ForwardSiteNetworkMapsPanel`), `templates/forward_netbox/inc/site_network_maps_panel.html`.
- `forward-sdk` 0.1.22 to 0.1.27 (`pyproject.toml`, `constraints.txt`, `scripts/validate_sbom.py`, `poetry.lock`).
- `docs/01_User_Guide/operations.md`.

## Approach

1. Bump forward-sdk to 0.1.27, which carries `client.network_maps`.
2. One wrapper lists maps and folders and returns `state` (`ok`, `refused`, `error`) with the rows; an empty list from `ok` means no maps are saved.
3. The Site extension asks each viewable source with a network, matches by name, and renders matches or a note; a Site with no match and a healthy answer renders nothing.

## Validation

- `invoke test-isolated` on `forward_netbox.tests.test_network_maps` plus the existing `test_forward_api`, `test_forward_client_errors`, `test_forward_client_factory`, then the full gate.
- 0.1.27 also changes `snapshots.latest_processed` to pick the newest processed snapshot by creation rather than the last one reprocessed. The plugin resolves snapshots through it (`get_latest_processed_snapshot`), so a sync can now resolve a different snapshot than under 0.1.22 when an older snapshot was reprocessed after a newer one. That matches Forward's own behaviour; the existing tests mock the SDK, so none proves the new ordering. Not verified here.
- Not verified: the deep link against a live Forward UI (taken from the app's route definition); the panel against a live Forward from this repo (the SDK session live-checked the list calls).

## Rollback

Revert; nothing is stored, so nothing needs undoing. The panel can also be removed by dropping `ForwardSiteNetworkMapsPanel` from `template_extensions`.

## Decision Log

- No model or migration (unlike the device analysis panel): map data does not need persisting and a migration adds upgrade-test surface to a release we are trying to shorten.
- Match by name only; matching by the devices a map holds would need a call per map on every render.
- A refusal and a failure stay visible on the Site page, so an operator is told why a map they expect is missing; a healthy answer with no match shows nothing so 200 other Sites are not cluttered.
