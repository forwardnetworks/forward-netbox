# Apply Device Scope Tags to Synced NetBox Devices

## Goal

Let operators see and filter in-scope devices in NetBox. The Forward device
scope tag (e.g. `Prod_Core`) lives only in Forward and is used to pick which
devices to sync; it never appears in NetBox, so operators cannot filter the
NetBox device list by scope or visually spot out-of-scope leftovers. Add an
opt-in that tags each synced device in NetBox with its scope include tag(s).

## Constraints

- Opt-in, default off (`apply_device_scope_tags` source parameter) — existing
  syncs are unchanged.
- No tag churn: only add the tag when the device does not already carry it.
- Reuse the existing NetBox Tag + device-tag machinery (same as the feature-tag
  map) rather than a new mechanism.

## Touched Surfaces

- `forward_netbox/utilities/sync_device.py` — `_scope_tags` helper (ensures and
  caches the scope Tag objects) and tag application in `apply_dcim_device`.
- `forward_netbox/forms.py` — `apply_device_scope_tags` BooleanField, initial,
  both fieldsets, both cleaned-parameter dicts.
- `forward_netbox/utilities/model_validation.py` — allowlist the new key.
- `forward_netbox/tests/test_device_scope_tagging.py` — enabled/disabled/no-dup
  tests.
- `docs/01_User_Guide/configuration.md` — document the option.

## Approach

`_scope_tags(runner)` reads the opt-in flag; when set, it resolves the sync's
device-scope include tags, ensures a NetBox `Tag` per name (slug via
`slugify`, default colour), and caches the resulting Tag list on the runner.
`apply_dcim_device` captures the upserted device and calls the existing no-churn
`_device_add_tag` for each scope tag. Tags reuse the per-device tag cache so a
steady-state re-sync adds nothing.

## Validation

- `forward_netbox.tests.test_device_scope_tagging` (applied when enabled; absent
  when disabled; no duplicate on re-apply).
- Full `forward_netbox.tests` suite.
- `invoke harness-check`, lint.

## Rollback

Remove the helper + call, the form field, the allowlist entry, and the tests.
Default-off opt-in means no production impact until enabled.

## Decision Log

- Multi-tag "any" guard: tagging applies only with a single include tag or
  "all" match mode (where every in-scope device carries every include tag). In
  "any" mode with multiple tags a device may match just one, and the device row
  does not carry its Forward tag names, so tagging is skipped with a warning
  rather than risk applying a tag the device does not have.

- Tag during `apply_dcim_device` (single adapter path; device is always
  adapter-required) rather than a separate post-sync pass — keeps the tag write
  next to the device write and reuses the device object already in hand.
- Scope Tag objects cached on the runner so the Tag upsert runs once per sync,
  not once per device.
- Opt-in because auto-tagging mutates NetBox data; operators who do not want
  scope tags in NetBox are unaffected.
