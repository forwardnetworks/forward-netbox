# Primary IP from `Mgmt_<iface>` device tag (2.0 feature, 2026-06-22)

## Goal

Set a NetBox device's `primary_ip4`/`primary_ip6` from a Forward device tag that
names the management interface. Blake's spec: a device tag `Mgmt_<iface>` (e.g.
`Mgmt_Vl211`) → primary IP = the address on that interface (`Vlan211`). Blake
confirmed all such interface IPs are in the Forward collection.

## Constraints

- Must not change behavior for syncs that don't opt in (default off).
- Must never break the ingest: tag-fetch / resolution failures log + skip.
- Single-branch only (the one execution path); update merges atomically.
- No customer identifiers in code/tests/docs.

## Approach

Opt-in per sync (`set_primary_ip_from_mgmt_tag`, default off). The work runs in
the single sync branch **after** all workloads stage (so interfaces + IPs exist)
and **before** merge, so the primary-IP update merges atomically with the sync.

- `utilities/interface_naming.py` — abbreviation-tolerant interface matching.
  `canonical_interface_key` reduces a name to `(type, number)` so `Vl211` and
  `Vlan211` compare equal; `resolve_mgmt_interface_name` maps a `Mgmt_<iface>`
  tag to the real interface name. Standard Cisco abbreviations (Vl→Vlan,
  Gi→GigabitEthernet, Lo→Loopback, Po→Port-channel, Te→TenGigE, …).
- `utilities/primary_ip.py` — `resolve_primary_ip_assignments` (pure: tags +
  interface→IPs → chosen v4/v6, lowest address wins on ties) and
  `apply_primary_ip_from_mgmt_tags` (in-branch: reads the device→Mgmt-tag map
  from Forward, resolves interface→IP from the branch ORM, sets primary_ip4/6,
  `snapshot()` + save + `emit_branch_object_changes` so the update is
  merge-eligible). Defensive: tag-fetch failure logs + skips, never breaks ingest.
- `ForwardClient.get_device_mgmt_tags` (forward_api_impl) — ad-hoc NQE over
  `device.tagNames` (Mgmt_ tags are not synced into NetBox), scoped to the sync's
  device-tag scope; returns `{device: [Mgmt_* tag, ...]}`.
- Form: `set_primary_ip_from_mgmt_tag` checkbox; allowlisted + normalized in
  `model_validation`; wired into `single_branch_executor.run`.

## Touched Surfaces

- `utilities/interface_naming.py` (new) — abbreviation-tolerant matching.
- `utilities/primary_ip.py` (new) — pure resolver + in-branch apply step.
- `utilities/forward_api_impl.py` — `ForwardClient.get_device_mgmt_tags`.
- `utilities/single_branch_executor.py` — post-stage, pre-merge hook.
- `forms.py` — `set_primary_ip_from_mgmt_tag` checkbox + clean/save/initial.
- `utilities/model_validation.py` — allowlist + normalize the new param.
- `tests/test_interface_naming.py`, `tests/test_primary_ip.py`,
  `tests/test_primary_ip_integration.py` (new).

## Defaults chosen (flag Blake if different)

- Sets both v4 and v6 when the matched interface has each.
- Overwrites existing primary_ip (Forward is authoritative for mgmt).
- No/multi match → skip + log (lowest address wins when an interface has several).

## Validation

- `test_interface_naming` (14) — abbreviation matching.
- `test_primary_ip` (10) — pure resolver.
- `test_primary_ip_integration` (2) — provisions a real branch, proves the
  device UPDATE ObjectChange carries `primary_ip4` (merge-eligible) + no-op path.
- Full suite 867 OK; lint/harness/sensitive clean.

## Rollback

Feature is opt-in (`set_primary_ip_from_mgmt_tag`, default off) — disabling the
sync flag fully reverts behavior. No migration; revert the commit to remove the
code (`interface_naming.py`, `primary_ip.py`, the client method, the form field,
the executor hook) with no schema impact.

## Decision Log

- **Opt-in, default off** — lands green without changing any existing sync's
  behavior; Blake enables it per sync once validated.
- **In-branch post-step (not an NQE workload)** — `primary_ip4` is a FK to an IP
  that does not exist until `ipam.ipaddress` stages, so the assignment must run
  after staging; doing it in-branch keeps it atomic with the merge.
- **Abbreviation matching in Python, not NQE** — testable canonical `(type,
  number)` key avoids brittle NQE string expansion and tolerates either side
  being abbreviated.
- **Mgmt_ tags read ad-hoc from `device.tagNames`** — they are not synced into
  NetBox, so they are fetched directly (scoped to the sync's device-tag scope).

## Open items for Blake

- Confirm the abbreviation map covers your platforms; flag non-standard short
  names (the canonical-key fallback still matches identical forms).
- Confirm overwrite-vs-only-if-empty preference.

Related: [[primary-ip-from-mgmt-tag]] (memory).
