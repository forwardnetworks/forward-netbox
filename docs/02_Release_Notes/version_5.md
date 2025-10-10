---
description: v5 Release Notes
---

# v5 Release Notes

## v5.0.0 (Unreleased)

#### Major Changes
- Renamed the plugin namespace from `ipfabric_netbox` to `forward_netbox` to match the new Forward branding. This affects import paths, template locations, permissions, and navigation slugs.
- Replaced the legacy SDK integration with a REST client that talks directly to Forward, keeping the SDK optional for deployments that still rely on it.
- Updated runtime ingest and topology helpers to use the new REST client for snapshots, inventory collection, and diagrams.
- Updated developer tooling and docs to reference Forward Networks resources, streamlining installation and support workflows.

#### Upgrade Notes
- Update `PLUGINS` entries, import statements, and any custom templates or scripts to reference `forward_netbox` instead of `ipfabric_netbox`.
- If you rely on automation that expects the Forward SDK, ensure the correct `forward` package version is installed alongside the plugin.
- Regenerate any container images or virtual environments so build steps no longer pin the Forward SDK version.
