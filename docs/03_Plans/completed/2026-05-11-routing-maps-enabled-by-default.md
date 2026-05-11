# Enable beta routing/module maps by default

## Goal

Expose the beta routing and module import surface by default so operators can use the built-in `netbox-routing`, `netbox-peering-manager`, and `dcim.module` maps without first flipping the plugin-wide routing gate.

## Constraints

- Keep the core NetBox-native import shape intact.
- Preserve the existing strict handling for data-file-aware alias and feature-tag rule maps.
- Existing custom or query-bound maps must not be rewritten.
- The change must remain reversible with a config flag.

## Touched Surfaces

- `forward_netbox/choices.py`
- `forward_netbox/utilities/query_registry.py`
- `forward_netbox/signals.py`
- `forward_netbox/tests/test_query_registry.py`
- `forward_netbox/tests/test_forms.py`
- `forward_netbox/tests/test_models.py`
- `forward_netbox/tests/test_sync.py`
- `docs/01_User_Guide/configuration.md`
- `docs/01_User_Guide/usage.md`
- `docs/01_User_Guide/troubleshooting.md`
- `docs/02_Reference/model-mapping-matrix.md`
- `docs/02_Reference/built-in-nqe-maps.md`

## Approach

- Default `enable_bgp_sync` to `True` so the routing/module beta surface is visible unless explicitly hidden.
- Seed built-in optional routing/module maps as enabled by default.
- On reseed, re-enable untouched built-in optional maps that still match the shipped raw query text so existing installs pick up the new default.
- Leave alias-aware and feature-tag-rules maps disabled because they require extra Forward data files.
- Update docs and tests to match the new default surface.

## Validation

- Ran focused unit tests for query registry, forms, models, and sync preflight.
- Ran the repo CI-equivalent gate with `python -m invoke ci`.
- Built release artifacts with `python -m build`.
- Verified release artifacts with `python -m twine check dist/*`.

## Rollback

- Set `enable_bgp_sync` back to `False` in plugin settings.
- Revert the routing/module default flags and reseed logic.
- Restore the prior docs text and tests if the default surface needs to be hidden again.

## Decision Log

- Chosen: enable the shared beta routing/module gate by default so the common native routing surface is broadly available.
- Rejected: keep the gate hidden and ask operators to opt in manually, because that preserves the old default but does not match the requested broader availability.
- Rejected: auto-enable the data-file-aware alias/rules maps, because those still depend on separate Forward data file setup.
