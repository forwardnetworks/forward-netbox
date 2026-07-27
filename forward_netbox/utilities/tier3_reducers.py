"""Deterministic per-side reducers for Tier 3 contributor relations.

The parameterless Tier 3 NQE revisions deliberately return provenance rows,
not rows that may be applied directly to NetBox.  This module is the only
place that turns one complete contributor side into normalized model rows.
"""

import json
from collections.abc import Iterable
from dataclasses import dataclass
from typing import Any

from ..exceptions import ForwardQueryError
from .sync_contracts import row_coalesce_field_is_complete


TIER3_REDUCER_IDS = frozenset(
    {
        "tier3_locations",
        "tier3_vlans",
        "tier3_vrfs",
        "tier3_prefixes",
        "tier3_hsrp_groups",
        "tier3_mac_addresses",
        "tier3_ip_addresses",
        "tier3_device_feature_tags",
    }
)


@dataclass(frozen=True)
class ScopeSide:
    include_tags: frozenset[str]
    exclude_tags: frozenset[str]
    include_match: str
    scoped_device_names: frozenset[str]
    scoped_site_names: frozenset[str]
    sync_device_tags: frozenset[str]

    @property
    def tag_scope_enabled(self) -> bool:
        return bool(self.include_tags or self.exclude_tags)

    def tags_are_in_scope(self, tags: Iterable[Any] | None) -> bool:
        values = {str(value) for value in tags or ()}
        if self.exclude_tags.intersection(values):
            return False
        if not self.include_tags:
            return True
        if self.include_match == "all":
            return self.include_tags.issubset(values)
        return bool(self.include_tags.intersection(values))

    def device_is_in_scope(
        self,
        device: Any,
        *,
        contributor_tags: Iterable[Any] | None = None,
    ) -> bool:
        if not self.tag_scope_enabled:
            return True
        if contributor_tags is not None:
            return self.tags_are_in_scope(contributor_tags)
        return str(device or "") in self.scoped_device_names


def scope_side_from_context(context) -> ScopeSide:
    return ScopeSide(
        include_tags=frozenset(
            str(value)
            for value in getattr(context, "device_tag_include_tags", ()) or ()
        ),
        exclude_tags=frozenset(
            str(value)
            for value in getattr(context, "device_tag_exclude_tags", ()) or ()
        ),
        include_match=str(getattr(context, "device_tag_include_match", "any") or "any"),
        scoped_device_names=frozenset(
            str(value) for value in getattr(context, "scoped_device_names", ()) or ()
        ),
        scoped_site_names=frozenset(
            str(value) for value in getattr(context, "scoped_site_names", ()) or ()
        ),
        sync_device_tags=frozenset(
            str(value) for value in getattr(context, "sync_device_tags", ()) or ()
        ),
    )


def scope_side_from_payload(payload: dict[str, Any]) -> ScopeSide:
    payload = dict(payload or {})
    return ScopeSide(
        include_tags=frozenset(
            str(value) for value in payload.get("include_tags") or ()
        ),
        exclude_tags=frozenset(
            str(value) for value in payload.get("exclude_tags") or ()
        ),
        include_match=str(payload.get("include_match") or "any"),
        scoped_device_names=frozenset(
            str(value) for value in payload.get("scoped_device_names") or ()
        ),
        scoped_site_names=frozenset(
            str(value) for value in payload.get("scoped_site_names") or ()
        ),
        sync_device_tags=frozenset(
            str(value) for value in payload.get("sync_device_tags") or ()
        ),
    )


def scope_state_from_context(context) -> dict[str, Any]:
    return {
        "include_tags": sorted(
            str(value)
            for value in getattr(context, "device_tag_include_tags", ()) or ()
        ),
        "exclude_tags": sorted(
            str(value)
            for value in getattr(context, "device_tag_exclude_tags", ()) or ()
        ),
        "include_match": str(
            getattr(context, "device_tag_include_match", "any") or "any"
        ),
        "scoped_device_names": sorted(
            str(value) for value in getattr(context, "scoped_device_names", ()) or ()
        ),
        "scoped_site_names": sorted(
            str(value) for value in getattr(context, "scoped_site_names", ()) or ()
        ),
        "scoped_matched_tags": {
            str(name): sorted(str(tag) for tag in tags or ())
            for name, tags in sorted(
                (getattr(context, "scoped_matched_tags", {}) or {}).items(),
                key=lambda pair: str(pair[0]),
            )
        },
        "sync_device_tags": sorted(
            str(value) for value in getattr(context, "sync_device_tags", ()) or ()
        ),
        "sync_endpoints": bool(getattr(context, "sync_endpoints", False)),
        "sync_generic_endpoints": bool(
            getattr(context, "sync_generic_endpoints", False)
        ),
        "scope_endpoints_by_include_tags": bool(
            getattr(context, "scope_endpoints_by_include_tags", False)
        ),
    }


def is_tier3_reducer(reducer_id: str) -> bool:
    return str(reducer_id or "") in TIER3_REDUCER_IDS


def _canonical_json(row: dict[str, Any]) -> str:
    return json.dumps(
        row,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
        default=str,
    )


def _distinct_rows(rows: Iterable[dict[str, Any]]) -> list[dict[str, Any]]:
    distinct = {_canonical_json(row): row for row in rows}
    return [distinct[key] for key in sorted(distinct)]


def _contributor_is_in_scope(
    row: dict[str, Any],
    scope: ScopeSide,
    *,
    device_field: str,
    tag_field: str | None = None,
) -> bool:
    tags = row.get(tag_field) if tag_field and tag_field in row else None
    return scope.device_is_in_scope(row.get(device_field), contributor_tags=tags)


def _reduce_locations(rows, scope: ScopeSide):
    return _distinct_rows(
        {
            "name": row.get("name"),
            "slug": row.get("slug"),
            "status": row.get("status"),
            "physical_address": row.get("physical_address"),
            "comments": row.get("comments"),
        }
        for row in rows
        if _contributor_is_in_scope(
            row,
            scope,
            device_field="contributor_device",
            tag_field="contributor_tags",
        )
    )


def _reduce_vlans(rows, scope: ScopeSide):
    grouped: dict[tuple[Any, Any, Any], list[dict[str, Any]]] = {}
    for row in rows:
        if not _contributor_is_in_scope(
            row,
            scope,
            device_field="contributor_device",
            tag_field="contributor_tags",
        ):
            continue
        key = (row.get("site"), row.get("site_slug"), row.get("vid"))
        grouped.setdefault(key, []).append(row)
    reduced = []
    for (site, site_slug, vid), candidates in grouped.items():
        default_name = f"VLAN {vid}"
        names = [str(candidate.get("name") or "") for candidate in candidates]
        preferred = [
            name for name in names if name != default_name and name.lower() != "default"
        ]
        reduced.append(
            {
                "site": site,
                "site_slug": site_slug,
                "vid": vid,
                "name": min(preferred or names),
                "status": "active",
            }
        )
    return _distinct_rows(reduced)


def _reduce_vrfs(rows, scope: ScopeSide):
    return _distinct_rows(
        {
            "name": row.get("name"),
            "rd": row.get("rd"),
            "description": row.get("description"),
            "enforce_unique": row.get("enforce_unique"),
        }
        for row in rows
        if _contributor_is_in_scope(
            row,
            scope,
            device_field="contributor_device",
            tag_field="contributor_tags",
        )
    )


def _reduce_prefixes(rows, scope: ScopeSide):
    return _distinct_rows(
        {
            "vrf": row.get("vrf"),
            "prefix": row.get("prefix"),
            "status": row.get("status"),
        }
        for row in rows
        if _contributor_is_in_scope(
            row,
            scope,
            device_field="contributor_device",
            tag_field="contributor_tags",
        )
    )


def _reduce_hsrp_groups(rows, scope: ScopeSide):
    fields = (
        "protocol",
        "group_id",
        "name",
        "device",
        "interface",
        "vrf",
        "address",
        "state",
        "priority",
        "status",
    )
    return _distinct_rows(
        {field: row.get(field) for field in fields}
        for row in rows
        if _contributor_is_in_scope(
            row,
            scope,
            device_field="device",
            tag_field="contributor_tags",
        )
    )


def _is_zero_mac(value: Any) -> bool:
    normalized = (
        str(value or "")
        .strip()
        .lower()
        .replace(":", "")
        .replace("-", "")
        .replace(".", "")
    )
    return normalized == "0" * 12


def _reduce_mac_addresses(rows, scope: ScopeSide):
    grouped: dict[str, list[dict[str, Any]]] = {}
    for row in rows:
        if not _contributor_is_in_scope(row, scope, device_field="device"):
            continue
        mac_address = str(row.get("mac_address") or "")
        if _is_zero_mac(mac_address):
            continue
        grouped.setdefault(mac_address, []).append(row)
    reduced = []
    for mac_address, candidates in grouped.items():
        chosen_device = min(str(row.get("device") or "") for row in candidates)
        chosen_interface = min(
            str(row.get("interface") or "")
            for row in candidates
            if str(row.get("device") or "") == chosen_device
        )
        reduced.append(
            {
                "device": chosen_device,
                "interface": chosen_interface,
                "mac": mac_address,
                "mac_address": mac_address,
            }
        )
    return _distinct_rows(reduced)


def _reduce_ip_addresses(rows, scope: ScopeSide):
    global_candidates: dict[str, list[dict[str, Any]]] = {}
    vrf_candidates: dict[tuple[Any, Any], list[dict[str, Any]]] = {}
    for row in rows:
        if not _contributor_is_in_scope(row, scope, device_field="device"):
            continue
        if row.get("vrf") in (None, ""):
            global_candidates.setdefault(str(row.get("host_ip") or ""), []).append(row)
        else:
            vrf_candidates.setdefault(
                (row.get("address"), row.get("vrf")),
                [],
            ).append(row)
    reduced = []
    for host_ip, candidates in global_candidates.items():
        chosen_prefix = max(int(row.get("prefix_length")) for row in candidates)
        prefix_candidates = [
            row for row in candidates if int(row.get("prefix_length")) == chosen_prefix
        ]
        chosen_device = min(str(row.get("device") or "") for row in prefix_candidates)
        chosen_interface = min(
            str(row.get("interface") or "")
            for row in prefix_candidates
            if str(row.get("device") or "") == chosen_device
        )
        chosen_address = min(
            str(row.get("address") or "")
            for row in prefix_candidates
            if str(row.get("device") or "") == chosen_device
            and str(row.get("interface") or "") == chosen_interface
        )
        reduced.append(
            {
                "device": chosen_device,
                "interface": chosen_interface,
                "vrf": None,
                # Preserve Forward's ipSubnet scalar encoding exactly. Python's
                # ip_network() canonicalizes away host bits and is not equal to
                # the NQE model row representation.
                "address": chosen_address,
                "status": "active",
            }
        )
    for (address, vrf), candidates in vrf_candidates.items():
        chosen_device = min(str(row.get("device") or "") for row in candidates)
        chosen_interface = min(
            str(row.get("interface") or "")
            for row in candidates
            if str(row.get("device") or "") == chosen_device
        )
        reduced.append(
            {
                "device": chosen_device,
                "interface": chosen_interface,
                "vrf": vrf,
                "address": address,
                "status": "active",
            }
        )
    return _distinct_rows(reduced)


def _reduce_device_feature_tags(rows, scope: ScopeSide):
    reduced = []
    for row in rows:
        if not _contributor_is_in_scope(row, scope, device_field="device"):
            continue
        candidate_kind = str(row.get("candidate_kind") or "")
        if candidate_kind == "raw_forward_tag":
            if str(row.get("tag") or "") not in scope.sync_device_tags:
                continue
        elif candidate_kind != "structured_rule":
            raise ForwardQueryError(
                "Tier 3 device-tag contributor row had an unsupported candidate kind."
            )
        reduced.append(
            {
                "device": row.get("device"),
                "tag": row.get("tag"),
                "tag_slug": row.get("tag_slug"),
                "tag_color": row.get("tag_color"),
            }
        )
    return _distinct_rows(reduced)


_REDUCERS = {
    "tier3_locations": _reduce_locations,
    "tier3_vlans": _reduce_vlans,
    "tier3_vrfs": _reduce_vrfs,
    "tier3_prefixes": _reduce_prefixes,
    "tier3_hsrp_groups": _reduce_hsrp_groups,
    "tier3_mac_addresses": _reduce_mac_addresses,
    "tier3_ip_addresses": _reduce_ip_addresses,
    "tier3_device_feature_tags": _reduce_device_feature_tags,
}


def reduce_contributor_rows(
    reducer_id: str,
    rows: Iterable[dict[str, Any]],
    scope: ScopeSide,
) -> list[dict[str, Any]]:
    reducer = _REDUCERS.get(str(reducer_id or ""))
    if reducer is None:
        raise ForwardQueryError(
            f"Unsupported Tier 3 contributor reducer `{reducer_id}`."
        )
    return reducer(rows, scope)


def contributor_target_key(reducer_id: str, row: dict[str, Any]) -> Any:
    reducer_id = str(reducer_id or "")
    fields = {
        "tier3_locations": ("slug",),
        "tier3_vlans": ("site", "site_slug", "vid"),
        "tier3_vrfs": ("name",),
        "tier3_prefixes": ("vrf", "prefix"),
        "tier3_hsrp_groups": ("protocol", "group_id", "address", "vrf"),
        "tier3_mac_addresses": ("mac_address",),
        "tier3_ip_addresses": ("host_ip", "address", "vrf"),
        "tier3_device_feature_tags": ("device", "tag_slug"),
    }.get(reducer_id)
    if fields is None:
        raise ForwardQueryError(
            f"Unsupported Tier 3 contributor reducer `{reducer_id}`."
        )
    return tuple(row.get(field) for field in fields)


def _row_identity(
    model_string: str,
    row: dict[str, Any],
    coalesce_fields: Iterable[Iterable[str]],
) -> tuple[Any, ...]:
    if model_string == "ipam.fhrpgroup":
        return (
            "participant",
            *(row.get(field) for field in ("protocol", "group_id", "address", "vrf")),
            row.get("device"),
            row.get("interface"),
        )
    for field_set in coalesce_fields:
        fields = tuple(field_set)
        if all(
            row_coalesce_field_is_complete(model_string, row, field) for field in fields
        ):
            return (
                "coalesce",
                fields,
                tuple((field, row.get(field)) for field in fields),
            )
    raise ForwardQueryError(
        f"Tier 3 reducer emitted a `{model_string}` row without a complete identity."
    )


def _rows_by_identity(model_string, rows, coalesce_fields):
    indexed = {}
    for row in _distinct_rows(rows):
        identity = _row_identity(model_string, row, coalesce_fields)
        existing = indexed.get(identity)
        if existing is not None and _canonical_json(existing) != _canonical_json(row):
            raise ForwardQueryError(
                f"Tier 3 reducer emitted conflicting `{model_string}` rows for one identity."
            )
        indexed[identity] = row
    return indexed


def diff_normalized_model_rows(
    model_string: str,
    before_rows: Iterable[dict[str, Any]],
    after_rows: Iterable[dict[str, Any]],
    coalesce_fields: Iterable[Iterable[str]],
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    """Diff two complete reduced sides without losing representative fallback."""

    before = _rows_by_identity(model_string, before_rows, coalesce_fields)
    after = _rows_by_identity(model_string, after_rows, coalesce_fields)
    upserts = [
        after[identity]
        for identity in sorted(after, key=repr)
        if identity not in before
        or _canonical_json(before[identity]) != _canonical_json(after[identity])
    ]
    deletes = [
        before[identity]
        for identity in sorted(before, key=repr)
        if identity not in after
    ]
    return upserts, deletes
