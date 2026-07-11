# 2.5.4 — endpoint include-tag scoping + missing-interfaces clarity

Status: implemented on `fix/2.5.4-endpoint-scope-and-iface-masking`.

## Goal

Two field reports from the same tag-scoped sync (include tags set, endpoint
import on):

1. SNMP endpoints imported even though they carry none of the include tags
   (the deliberate 2.4.4 behavior, now unwanted by the operator). Live data
   disproved the 2.4.4 premise: 678/685 endpoints carry ≥1 Forward tag and 526
   carry the operator's include tag, so an include gate scopes endpoints rather
   than excluding them all.
2. Interfaces and IP addresses did not import while devices did. Root cause: the
   scoped-device set = {include-tagged **collected** devices} ∪ {**all**
   endpoints}. When the tags resolve to 0 collected devices in the selected
   snapshot (e.g. backfilled), the endpoint union keeps `dcim.device` non-empty
   while interface/IP queries — which endpoints can never satisfy — collapse to
   0: a silent, confusing partial import. Secondary: a duplicate device name
   across sites raised an uncaught `MultipleObjectsReturned` from
   `get_device_by_name`, failing the whole interface apply workload.

## Constraints

- Do NOT regress 2.4.4 operators who rely on "all endpoints import on a scoped
  sync": the include gate must be opt-in, default off.
- The endpoint scope probe and the query branch must apply **byte-identical**
  include logic — a drift makes the local filter drop rows the branch emits,
  and with prune-out-of-scope on those become DELETEs of previously imported
  endpoints.
- The persisted-context artifact cache must not reuse a pre-toggle scoped set
  after the toggle flips.
- No behavior change for sources that never set the new key.

## Touched Surfaces

- Queries (validation-org republish required): `forward_devices.nqe`,
  `forward_devices_with_netbox_aliases.nqe` — new
  `scope_endpoints_by_include_tags: Bool` param + opt-in include gate in the
  endpoint branch (byte-identical in both).
- `forward_netbox/utilities/forward_api_impl.py` — new
  `build_endpoint_tag_scope_where` (sibling of `build_device_tag_scope_where`,
  targeting `endpoint.tagNames`); re-exported via `forward_api.py`.
- `forward_netbox/utilities/query_fetch_execution.py` — probe include gate,
  toggle threading (context dataclass, source-parameter read, cache key,
  artifact descriptor v4, query-param injection), and the 0-collected-devices
  masking warning.
- `forward_netbox/utilities/query_registry.py` — parameter constant + default
  injection.
- `forward_netbox/forms.py` — "Scope SNMP Endpoints by Include Tags" toggle
  (field, initial, both fieldsets, both candidate-parameter dicts).
- `forward_netbox/utilities/model_validation.py` — source-key allowlist + bool
  type check.
- `forward_netbox/utilities/sync_primitives.py` — `get_device_by_name` catches
  `MultipleObjectsReturned`, resolves to the earliest device, warns once per
  name (the by-name cache dedupes).
- `forward_netbox/tests/test_endpoints_import.py` — see Validation.

## Approach

- **Opt-in include gate** in the endpoint branch:
  `!scope_endpoints_by_include_tags || isEmpty(include) || ("all" ? no include
  tag missing : any include tag present)` — pure boolean, mirroring
  `build_device_tag_scope_where` semantics. Default off = today's exclude-only
  behavior, byte-for-byte.
- **Probe parity**: `_resolve_scoped_endpoint_names` builds its where-list from
  `build_endpoint_tag_scope_where`, passing include tags only when the toggle
  is on, so probe == branch by construction.
- **Masking warning**: in `_resolve_scoped_tag_scope`, when the collected-device
  half of the scope is empty and endpoints were unioned in, warn that
  interfaces/IPs require collected devices in scope (points at the snapshot
  selector). Complements the existing all-backfilled warning.
- **Duplicate-name guard**: resolve `MultipleObjectsReturned` deterministically
  (earliest pk) + warning instead of crashing the workload.

## Validation

- Live validation-org oracle (pre-implementation probe, trace D): toggle-on
  include=[scope-a] → 526 endpoints; [scope-b] → 5; toggle-off → 685
  (regression gate). Re-verified live post-implementation.
- CI-safe tests (test_endpoints_import.py): query-shape (toggle param, gated
  include reference, both match modes, both files, byte-identical branch);
  default-off query parameter; probe toggle-off excludes include tags
  (regression guard) / toggle-on applies "any" and "all"; builder golden parity;
  scope-union threading; masking warning fires only on 0 collected devices;
  form fieldset; allowlist bool check; duplicate-name lookup resolves to the
  earliest device and warns.
- Full suite + lint + harness gates.

## Rollback

Single squashable branch; revert the merge commit. The toggle defaults off and
the new source key is additive, so reverting cannot change any existing sync;
republishing the prior bundled queries reverts the query surface. The
duplicate-name guard and masking warning are strictly additive.

## Decision Log

- **Opt-in toggle, not always-apply**: always-apply would silently drop (and,
  with prune enabled, DELETE) every endpoint for 2.4.4 operators whose console
  servers carry no scoping tags. Default-off preserves them; the operator who
  wants scoped endpoints flips it.
- **Include gate in-query + in-probe (lockstep), not post-fetch filtering**:
  the local filter dropping branch-emitted rows is exactly the prune hazard the
  2.4.4 comment documents; keeping both sides identical avoids it.
- **Artifact descriptor bumped to v4**: the scoped set now varies with the
  toggle; a stale pre-toggle cache entry must not be reused.
- **Earliest-pk resolution for duplicate names** (not a hard error): Blake must
  not hit a new sync failure; the warning surfaces the ambiguity without
  failing the workload.
