# Versioning and Deprecation Policy

## Versioning

Releases follow **semantic versioning** (`MAJOR.MINOR.PATCH`):

- **MAJOR** — backwards-incompatible changes (removed features, changed defaults
  that require operator action, or a raised minimum NetBox/`netbox-branching`
  baseline that drops a previously supported version).
- **MINOR** — backwards-compatible features and non-breaking behavior changes.
- **PATCH** — backwards-compatible bug fixes.

Every release records the exact required NetBox and `netbox-branching` versions in
the compatibility matrix in the top-level `README.md`, and the changes in
`CHANGELOG.md` (generated from that matrix). Stored Forward source parameters are
forward/backward tolerant across recent releases — unknown keys are accepted and
ignored — so a minor up/downgrade does not invalidate existing sources.

## Deprecation process

When a feature, configuration key, NQE map, or API behavior is to be removed:

1. **Announce** it in the `CHANGELOG.md` / compatibility-table entry for the
   release that first deprecates it, describing the replacement and the migration
   path.
2. **Keep it working** for at least one subsequent **minor** release after the
   announcement, emitting a deprecation warning (log or UI) where practical.
3. **Remove** it no earlier than the next **major** release, referencing the prior
   deprecation notice in that release's notes.

Security-critical changes may move faster; those are called out explicitly in the
release notes and `SECURITY.md`.

## Support scope

Security fixes target the latest released minor version (see `SECURITY.md`). The
project's overall support scope is stated in the `README.md` Support section;
consult it and the `SECURITY.md` deployment notes before deploying in production.
