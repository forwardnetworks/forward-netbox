# Uncovered devices Forward's UI still shows as tagged

## Goal

When a device this sync created is uncovered because the snapshot no longer
carries it, the scope reconciliation page and the device page say whether
Forward's device *configuration* still lists it under an include tag - and
which configuration fact keeps it out of the snapshot: collection disabled, a
configured device the snapshot does not carry, or a tag entry that outlived
the device (vsys/vdom child, controller-managed, removed). Today both pages
say only "gone from Forward", and an operator who then opens Forward's Device
Tags page sees the device tagged and reports the sync as dropping tags.

## Constraints

- The census vocabulary stays three-valued (`absent`, `untagged`,
  `vendor_excluded`); both prunes gate on `absent` and that must not move.
  The new information is a `details` refinement of `absent`, exactly as the
  endpoint-scope rules refine `untagged`.
- Advisory only. The two configuration reads are REST GETs that a read-only
  or older Forward may refuse; a failure leaves the census verdicts and
  availability untouched.
- No extra calls on a converged sync: the reads run only when absent names
  exist, and the second (classic-devices) only when the first found at least
  one absent name still under an include tag.
- Device names stay out of persisted payloads beyond the existing 25-name
  samples; the per-device map is keyed by pk.
- `maint/2.9.x` lane; no migration.

## Touched Surfaces

- `forward_netbox/utilities/forward_api_impl.py` -
  `ForwardClient.get_configured_device_tags` (`GET
  /networks/{id}/device-tags?with=devices`) and
  `get_classic_device_collection` (`GET /networks/{id}/classic-devices`,
  `collect: false` = disabled).
- `forward_netbox/utilities/scope_reconciliation.py` - `ABSENT_DETAILS`,
  `_configured_absence_details`, `_absence_census(include_tags=)`,
  `_absence_summary` (`absent_detail`, `absent_still_tagged`),
  `unmanaged.owned_detail_by_id` (renamed from `owned_endpoint_detail_by_id`;
  now carries both refinements).
- `forward_netbox/template_content.py` - the device-page offer carries
  `absent_detail` / `absent_detail_label` for an absent device.
- `forward_netbox/templates/forward_netbox/forwardsync_scope_reconciliation.html`,
  `forward_netbox/templates/forward_netbox/inc/device_ownership_panel.html`.
- `forward_netbox/management/commands/forward_device_scope_reconciliation_audit.py`
  - the remediation text names the still-tagged count.
- `forward_netbox/tests/test_absent_configuration_detail.py` (new).

## Approach

Live probe of the validation org (latest processed snapshot, include tags matched `any`)
established the shape before any code moved: the plugin's scope result equals
the snapshot's tagged set exactly (0 tagged devices in `network.devices` fall
outside the predicate; no name, case or whitespace mismatches; no
`FORWARD_CUSTOM` devices under the include tags). Forward's configuration-side
tag list held 4334 names under the two tags against the snapshot's 3908; the
gap was 709 names absent from the snapshot (445 vsys/vdom children, 164 not
in the classic config, 63 `collect: false`, 36 configured but not carried), 90
generic SNMP endpoints (generic import off) and 84 CIMC controllers. Of the
709, ~119 had appeared in some snapshot since January; those plus the 174
endpoints account for the customer's 321.

Implementation: after the census has classified names, hand the `absent` ones
to `_configured_absence_details`. It reads the configuration tag map, keeps
the names under any include tag, and only then reads the classic-device
collection flags to split them into the three details. `_absence_summary`
adds an `absent_detail` breakdown (reason, label, count, sample) and an
`absent_still_tagged` total; the panel renders both the way it renders the
endpoint breakdown, and the device page names the fact beside the remove
button with the remedy per detail.

## Validation

- `forward_netbox.tests.test_absent_configuration_detail` - census
  refinement per fact, include-tag gating, no reads when nothing is absent,
  advisory failure modes (first read fails: census intact; second read fails:
  `absent_tag_only`), summary breakdown, client parsing of both REST shapes,
  report by pk, device-page offer.
- Existing `test_endpoint_absence_detail`, `test_uncovered_absence_and_trend`,
  `test_device_scope_reconciliation_audit_command`,
  `test_uncovered_device_cleanup` unchanged and green (their `Mock()` clients
  return non-dicts from the new methods, which the code treats as "no
  configuration detail").
- Full `invoke ci` before push.
- Live: the validation-org probe above is the ground truth the details were designed
  from; the customer's own panel is the confirmation once released.

## Rollback

Revert the commit. No schema. A report stored by the new code carries
`owned_detail_by_id`, which the old `template_content` reads under the old
key and simply does not find; a report stored by the old code lacks the new
summary keys, which the templates guard with `{% if %}`.

## Decision Log

- Refine `absent` rather than add a fourth kind: the prunes and their tests
  pin the three-valued vocabulary, and a disabled device IS absent from the
  snapshot - the operator needs to know Forward still has it, not a different
  prune gate.
- Configuration reads via REST rather than NQE: no NQE table exposes the
  configuration, and the Device Tags page the customer compared against is
  backed by exactly these endpoints.
- The generic-endpoint and CIMC halves of the customer's 321 already had
  their explanation (`ENDPOINT_ABSENCE_DETAILS`); no change there.
