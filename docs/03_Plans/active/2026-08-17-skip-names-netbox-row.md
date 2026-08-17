# Name the NetBox row a protected-delete skip is about

## Goal

Make a dependency skip that was raised with a NetBox object in hand say which
row it was about, so a blocked delete is actionable from the ingestion issue
list alone.

## Why

A deployment on 2.8.1 reported six ingestion issues. Five were dependency
skips on the delete path:

    ipam.vrf         skipped; still referenced by ipam.ipaddress, ipam.prefix,
                     netbox_routing.bgpscope
    dcim.devicetype  skipped; still referenced by dcim.device
    dcim.site        skipped; still referenced by dcim.device
    dcim.site        skipped; still referenced by dcim.device
    dcim.site        skipped; still referenced by dcim.device, ipam.vlan

Every sentence is correct and none of them is actionable. The model is named,
the direction is named, and the one fact needed to go look - *which* site - is
missing, because everything that would identify it is a name or a slug and
`diagnostic_shape` reduces `context` to its key names before anything persists.
Two of the three `dcim.site` rows are byte-identical, so the panel cannot even
confirm they are different sites.

This is the same gap 2.8.1 closed on the merge recorder, which now appends
`Affected NetBox row: pk N.` A pk is an internal identifier rather than
customer data, so it is the one thing that survives redaction. The sync
recorder never got the same treatment.

## Constraints

- Customer data stays out of persisted diagnostics. The pk is added *because*
  the slug cannot be; this must not become an excuse to widen `context`.
- A raiser with no row in hand must record exactly the message it recorded
  before, byte for byte. Most dependency skips are a missing parent - an
  absence, with nothing to name - and they are the common case.
- Not more permissive and not less: nothing about which rows are deleted, or
  refused, changes.

## Touched Surfaces

- `forward_netbox/exceptions.py` - `ForwardDataError.netbox_pk`
- `forward_netbox/utilities/sync_primitives.py` - `delete_by_coalesce` passes
  the object it could not delete
- `forward_netbox/utilities/sync_reporting.py` - `record_issue` appends the
  sentence and stores the pk in `raw_data`

## Approach

`ForwardDataError` gains an optional `netbox_pk`, normalised to text so an
integer and a UUID pk are handled the same way and a blank is treated as
absent. `delete_by_coalesce` is the only raiser that sets it today: it is
holding the object whose `delete()` the database refused, so it is the only
place that can.

`record_issue` appends `Affected NetBox row: pk N.` to the message when the
exception carries one, and mirrors it into `raw_data["netbox_pk"]` so anything
reading issues over the API does not have to parse English. Appended, never
substituted - that is what keeps the missing-parent case unchanged.

The pk lands inside the message, and the message is already part of the issue
dedup key, so distinct blocked rows now persist as distinct issues instead of
collapsing. The per-model `DEPENDENCY_SKIP_ISSUE_DETAIL_LIMIT` still caps the
flood.

## Validation

`forward_netbox/tests/test_skip_names_netbox_row.py`, covering the exception
(integer, UUID, blank, and pk `0`, which a falsiness guard would drop), the
recorder (message, `raw_data`, redaction still holding, two blocked sites no
longer identical, and both unchanged cases), and `delete_by_coalesce` itself
raising with the pk on `ProtectedError`.

Also re-ran the recorder-adjacent suites, since the message shape is asserted
in several: health, DLM integration, ingestion merge, merge rule rejection,
protecting-reference deletes, bulk merge, issue diagnosis, skip direction and
skip raisers.

## Rollback

Revert. The sentence disappears and the skips read as they did on 2.8.1.

## Decision Log

- **The pk, not the protecting rows' pks.** The message already names the
  models holding the row; with the blocked row's own pk an operator can open it
  in NetBox and see them. Listing the holders would be unbounded on a site with
  thousands of devices.
- **Only `delete_by_coalesce` sets it.** Teaching every raiser to name a row
  would mean inventing one where there is none. A skip waiting on an absent
  parent has no NetBox row to name, and saying nothing is correct there.
- **Message and `raw_data`, not `context`.** `context` is the redaction
  boundary and stays where it is.

## Open

- Separate, and not addressed here: the diff-driven delete path
  (`_split_diff_rows`) has no model allowlist, so it can delete
  `dcim.site`, `dcim.devicetype`, `dcim.platform`, `ipam.vrf`, `ipam.prefix`
  and `dcim.device` - every one of which `BASELINE_REMOVAL_MODELS` refuses by
  name, with the reasons written out. These five skips are that path (or
  operator-enabled orphan pruning) being caught by PROTECT rather than by a
  gate. Worth a decision on its own; a reporting change should not carry it.
