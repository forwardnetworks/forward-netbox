---
description: v5 Release Notes
---

# v5 Release Notes

## v5.0.0 (Unreleased)

#### Major Changes
- Renamed the plugin namespace from `ipfabric_netbox` to `forward_netbox` to match the new Forward branding. This affects import paths, template locations, permissions, and navigation slugs.
- Made the Forward SDK an optional dependency. The plugin can be installed without pulling the proprietary SDK; Forward-powered features now surface helpful guidance when the SDK is absent.
- Updated developer tooling and docs to reference Forward Networks resources, streamlining installation and support workflows.

#### Upgrade Notes
- Update `PLUGINS` entries, import statements, and any custom templates or scripts to reference `forward_netbox` instead of `ipfabric_netbox`.
- If you rely on automation that expects the Forward SDK, ensure the correct `forward` package version is installed alongside the plugin.
- Regenerate any container images or virtual environments so build steps no longer pin the Forward SDK version.
