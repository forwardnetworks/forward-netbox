# Device Type Library NQE Data File

## Goal

Add a documented, repeatable path for using a Forward NQE data file to map Forward device model strings to NetBox Device Type Library model and slug values, with manufacturer overrides kept in data where the alias-aware maps can consume them.

## Constraints

- Keep default bundled NQE maps runnable when the data file has not been uploaded.
- Keep NetBox-ready device type shaping in NQE, not Python.
- Do not commit customer identifiers, network IDs, snapshot IDs, credentials, or exported customer data.
- Provide a user-facing script to build the data file from a local checkout of the device type library.
- Do not rely on `useLatestDataFiles` in the public `/api/nqe` plugin path; the selected snapshot must expose the data file value for aliases to affect plugin sync output.

## Touched Surfaces

- `forward_netbox/queries/`: add alias-aware optional device model and device query variants.
- `forward_netbox/utilities/query_registry.py`: seed optional built-in maps while keeping the default non-data-file maps as the active path.
- `forward_netbox/signals.py`: seed optional maps without overriding a user's later enabled state.
- `scripts/`: add the Device Type Library to Forward NQE data-file converter and tests, including manufacturer override rows.
- `docs/`: document the default path, optional data-file path, and query-map workflow.

## Approach

- Add alias-aware NQE variants for `dcim.devicetype` and `dcim.device`.
- Store alias rows and manufacturer override rows in one stable-schema JSON data file, distinguished by `record_type`.
- Keep the existing built-in files as the non-failing default path.
- Document the required Forward data file name and NQE name.
- Add script coverage for JSON generation and alias precedence.
- Validate alias-aware NQE against the Forward instance, including the safe fallback when the selected snapshot has no data file value.

## Validation

- Unit test the converter script.
- Run query registry tests.
- Validate alias-aware NQE through the public `/api/nqe` path used by the plugin.
- Confirm the live data file upload and attachment separately from snapshot-bound query execution.

## Rollback

- Disable the two alias-aware NQE maps in the UI and re-enable the default device model and device maps.
- Remove the uploaded Forward data file from the target network if it is no longer desired.
- Revert the optional query files, converter script, registry entries, and documentation additions.

## Decision Log

- Default maps remain the non-data-file path so an installation without the Forward data file cannot fail during query execution.
- Alias-aware maps are seeded disabled so operators can opt into Device Type Library matching without changing custom maps or query IDs.
- Alias shaping stays in NQE because the exported rows should already contain NetBox-ready model and slug values before Python sync validation.
- The alias-aware maps preserve the static NQE manufacturer helper as a fallback, but prefer `manufacturer_override` rows from the data file when the selected snapshot exposes them.
