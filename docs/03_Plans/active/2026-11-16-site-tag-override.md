# A Device Tag Can Place Devices at a NetBox Site

## Goal

Several Forward locations can become one NetBox site, without a customer-specific
rule in the plugin. A device tagged `NetBox_Site_<name>` is placed at the site
`<name>`; an untagged device keeps its Forward location, so nothing changes until a
customer opts in.

A customer's data centre is dozens of Forward locations (halls, fabrics, zones) and
their inventory system has one site for it. The sync produced a site per location,
so the two never lined up. A first attempt grouped by name inside that customer's
own org copy of the queries, and it only reached the site list: the device and VLAN
queries the sync is actually bound to still used the location, so the result was an
empty grouped site while devices stayed at their per-location sites. Per-customer
rules in copies of the queries cannot be kept consistent; this makes one rule that
every query shares.

## Constraints

- No customer names, network ids, snapshot ids or hostnames in the repo.
- Untagged devices keep exactly the site they have today (verified on live data).
- One rule everywhere a site is named: the shared NQE helper, the maps that use it,
  and the ad-hoc queries the plugin builds in Python.
- Cisco ACI fabric names are not sites and are not changed.

## Touched Surfaces

- `queries/netbox_utilities.nqe`: `siteTagName`, `siteNameFor`, `deviceSiteName`.
- `queries/forward_devices.nqe`, `forward_devices_with_netbox_aliases.nqe`
  (devices and SNMP endpoints), `forward_vlans.nqe`, `forward_locations.nqe`
  (now one row per site, address from the same-named or first location).
- `utilities/site_tag.py`: the prefix, a plain-Python reference of the rule, and the
  NQE fragments the inline queries embed.
- `utilities/scope_reconciliation.py`, `utilities/query_fetch_execution.py`: their
  inline queries use the shared rule, so the orphan-site and scope checks agree with
  the maps.
- `docs/02_Reference/site-tag-override.md` and the built-in maps and mapping matrix.
- `tests/test_site_tag_override.py`.

## Approach

1. The helper returns the first (alphabetical) `NetBox_Site_` tag value, lowercased,
   else the lowercased location, else `unknown`.
2. The locations map groups devices by that name and takes the address from the
   location with the same name, else from the first location by name.
3. The Python inline queries embed the same rule from `site_tag.py`; a test keeps the
   two spellings of the tag lookup identical.

## Validation

- Live, read-only, on a customer-shaped Forward network: with no tags, the locations
  (241 rows), devices (10,662) and VLANs (9,196) queries return exactly the rows the
  previous versions returned.
- Live, with an existing tag standing in for the prefix in a test copy: the tagged
  devices became one site with one slug and an address from its first location, and
  the locations list shrank by the grouped sites.
- Unit tests: the rule (override, ordering, empty tag, case, unknown), the helper's
  exports and prefix, every site-naming map using the helper, ACI left alone, the
  inline queries embedding the shared rule, and the documentation.

## Rollback

Revert the commit and republish the queries. No tag means no change in behaviour, so
reverting is safe at any time.

## Decision Log

- A device tag, not a data file: a data file would need a missing-file variant of
  every query that names a site, where tags are always present on a device.
- Namespaced prefix `NetBox_Site_`, not `Site_`: an existing tag that happens to start
  `Site_` must not move devices when the queries are republished.
- Names stay lowercased like every other site name the sync creates.
- Not done: keeping the original Forward location as a NetBox location under the
  site. The plugin has no location model today.
