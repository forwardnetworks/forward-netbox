# Accept netbox-dlm 0.10.1 and 0.10.2

## Goal

`netbox-dlm` 0.10.1 and 0.10.2 were published on 2026-10-07 and a customer is
moving onto them. This lane validates one set of optional-plugin versions, and an
installed version outside the set silently turns off the DLM integration and the
fast paths. Accept both, alongside `0.10.0`, before anyone upgrades into that.

## Constraints

- Add the new versions to the accepted sets; never replace `0.10.0` (replacing
  broke a customer pinned to an earlier plugin release on 2.9.14).
- The tested pin follows the newest release the gate can install. `0.10.2` is
  tagged on GitHub but not yet on PyPI, so the pin stays at `0.10.1` and `0.10.2`
  is accepted without being installed by the gate.
- `development/constraints-upgrade-from.txt` stays in lockstep with
  `constraints.txt`.

## Touched Surfaces

- `forward_netbox/utilities/validated_runtime.py`,
  `forward_netbox/utilities/plugin_integrations/registry.py`.
- `constraints.txt`, `development/constraints-upgrade-from.txt`, `poetry.lock`,
  `scripts/validate_sbom.py`, `tasks.py`.
- `forward_netbox/tests/test_optional_plugin_versions.py`,
  `forward_netbox/tests/test_plugin_integrations.py`.

## Approach

Diffed each release against its predecessor. `0.10.1` changes the software-version
page only (one extra related query in the view and a template layout); `0.10.2`
sets the plugin author. Neither touches models or migrations, so the adapters are
unaffected. Move the accepted sets and the tested pin as above.

## Validation

- The wheel diff above and the GitHub compare for `0.10.1...0.10.2`.
- The optional-plugin and integration test suites, then the full gate on the push.
- Follow-up: move the tested pin to `0.10.2` once it is on PyPI.

## Rollback

Revert the commit. Installs on `0.10.1` or `0.10.2` would then fall outside the
validated set and the integration would report an unsupported version.

## Decision Log

- **Accept before the gate can install it.** `0.10.2` changes only metadata, and
  waiting for PyPI would leave an upgrading customer on a silently reduced runtime.
- **Pin at `0.10.1` for now.** The gate installs from PyPI, so the tested pin cannot
  name a version that is not published.
