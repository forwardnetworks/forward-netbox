# Match devices the way NetBox identifies them: case-insensitively

## Goal

A customer's sync failed on every run after upgrading, with
`IntegrityError on constraint dcim_device_unique_name_site at
apply_engine_bulk.py:bulk_orm_apply_device`. The Forward data was clean: all
device rows for the network were pulled and contained zero duplicate
`(name, site)` pairs, including under case-folding. So the new row collided
with a device already in NetBox.

NetBox's `dcim_device_unique_name_site` is
`UniqueConstraint(Lower("name"), "site", condition=Q(tenant__isnull=True))`,
which is case-insensitive. Both device apply paths matched existing devices by
EXACT name. A device Forward reports as `CORE-SW-01` against a stored
`core-sw-01` in the same site was classified as new, and the database refused
the create. On the bulk path, inside a branch (every real sync), that refusal
re-raises and fails the whole sync. A hostname whose case changed on the device
is enough to trigger it.

## Constraints

- Nothing that matched before may match differently: an exact-name match is
  always preferred.
- No additional queries on the bulk path.
- The dependency preview must stay read-only.
- No customer names or identifiers in this plan, the commits or the PR.

## Touched Surfaces

- `forward_netbox/utilities/apply_engine_bulk.py` - `bulk_orm_apply_device`
  existing-device index and match; `name` added to the bulk update fields.
- `forward_netbox/utilities/sync_device.py` - `apply_dcim_device` and new
  `_case_variant_device`.
- `forward_netbox/tests/test_device_name_case_identity.py` (new).

## Approach

- **Bulk path.** The existing-device index is fetched by `Lower("name")` and
  keyed by lowercased name: one query per chunk, the same count as before.
  Within a site, exact-name matches are preferred; only when none exists does a
  case-only difference count as the same device. Two case variants in one site
  (possible when tenants differ) still raise the existing ambiguity error
  rather than guessing.
- **`name` is now a bulk update field.** An exact match never changes it, so it
  was never needed. With a case-only match, leaving it out would match the
  device, set Forward's spelling in memory and never save it, which is a drift
  that reappears on every run.
- **Adapter path.** `_case_variant_device` resolves a same-site case variant
  only after the exact lookup misses, through `runner._get_unique_or_raise`,
  which the preview runner shims read-only. When found, the upsert matches it
  by primary key and renames it to Forward's spelling. Unfixed, this path
  failed the row on `full_clean` rather than the whole sync, but it never
  converged either.
- The downstream device-by-name lookups (interfaces, MAC and IP addresses,
  virtual chassis, primary IP) resolve a parent device and never create one.
  They find the renamed device because devices are applied first in the same
  branch, so they are unchanged.

## Validation

- New `test_device_name_case_identity`: bulk and row paths update the case
  variant instead of colliding; an exact match still wins over a case variant;
  unrelated devices are still created. Run against the unfixed code, the bulk
  test reproduces the customer's error verbatim
  (`duplicate key value violates unique constraint
  "dcim_device_unique_name_site"`).
- Device suites (`test_apply_engine`, `test_bulk_adapter_parity`,
  `test_cross_site_vlan_revalidation`, `test_device_scope_tagging`): 85/85.
- Full `invoke ci` before push.

## Rollback

Single commit; revert restores exact-name matching.

## Decision Log

- **Rename to Forward's spelling rather than keep NetBox's.** Forward is the
  source of truth for device attributes, and `name` is already in the upsert
  values. Keeping NetBox's spelling would need a special case in both paths
  and would leave a permanent drift between the two.
- **Exact first, case-insensitive second**, rather than case-insensitive only,
  so that an estate holding both spellings in one site under different tenants
  resolves exactly as it did before.
