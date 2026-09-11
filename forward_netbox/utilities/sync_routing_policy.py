# netbox-routing policy objects: prefix lists, community lists, route maps.
#
# Forward has no structured routing-policy model - nothing on `Device`,
# `BgpNeighbor` or the RIB path carries prefix lists, community lists or route
# maps - so the three bundled maps parse `device.files.config` and emit one row
# per entry. This module turns those rows into netbox-routing objects.
#
# Two facts about the destination shape everything here:
#
# - **netbox-routing names policy objects globally** (`PrefixList.name`,
#   `CommunityList.name` and `RouteMap.name` are each unique, case-insensitively)
#   and gives them no device. A prefix list is device-local configuration, and
#   on a real fleet a third of the names are defined differently on different
#   devices - mostly disjointly. So the three maps build a CATALOGUE in NQE:
#   per configured name, every device's definition is grouped by content, the
#   definition shared by the most devices owns the bare name, and each other
#   definition is stored as `<name>@<lowest device holding it>`. Rows arrive
#   here already carrying the stored `name`, the configured `list_name` /
#   `map_name`, the representative `device` and `device_count`. Shared policy
#   appears once under its real name and nothing is dropped. The route-map M2M
#   links (`match_prefix_list`, `match_community_list`) are not written: the
#   staging engine stages concrete fields only, and the JSON `match`/`set`
#   blobs carry the referenced names verbatim.
# - **Entries are validated by the plugin's own `clean()`**, which the merge
#   runs. `PrefixListEntry.clean()` in 0.4.3 rejects the normal `ge X le Y`
#   pair (it raises when `ge < le`) and any bound not longer than the prefix.
#   Rather than let the merge reject those rows on every run, the adapter
#   predicts the rejection, stores the entry without bounds and records the
#   verbatim modifiers in `description` plus one rolled-up warning.
import re
from ipaddress import ip_network

from ..exceptions import ForwardQueryError
from .sync_primitives import forget_lookup_object
from .sync_reporting import EXPANDED_COMMUNITY_LIST_REASON
from .sync_reporting import NON_NUMERIC_COMMUNITY_REASON
from .sync_reporting import POLICY_NAME_TOO_LONG_REASON
from .sync_reporting import UNREPRESENTABLE_PREFIX_BOUNDS_REASON
from .sync_routing_impl import preview_leaf_outcome

PREFIX_LIST_MODEL = "netbox_routing.prefixlistentry"
COMMUNITY_LIST_MODEL = "netbox_routing.communitylistentry"
ROUTE_MAP_MODEL = "netbox_routing.routemapentry"

ROUTING_POLICY_MODELS = (PREFIX_LIST_MODEL, COMMUNITY_LIST_MODEL, ROUTE_MAP_MODEL)

# `Community.community` in netbox-routing 0.4.3 admits only numeric forms
# (`65000:100`, `1.2.3.4:100`, `65000:1:2`); well-known names and regexes are
# not communities there.
_COMMUNITY_VALUE = re.compile(r"^[\d.]+(?::[\d.]+(?::[\d.]+)?)?$")

ROUTING_POLICY_ROLLUP_REASONS = frozenset(
    {
        UNREPRESENTABLE_PREFIX_BOUNDS_REASON,
        EXPANDED_COMMUNITY_LIST_REASON,
        NON_NUMERIC_COMMUNITY_REASON,
        POLICY_NAME_TOO_LONG_REASON,
    }
)

_NAME_MAX_LENGTH = 100
_DESCRIPTION_MAX_LENGTH = 200


def policy_object_name(row):
    """The stored name, decided by the query's catalogue grouping."""
    return str(row.get("name") or "").strip()


def policy_description(row, configured_name):
    """What the catalogue knows about this object, for its description."""
    count = _int_or_none(row.get("device_count")) or 1
    device = str(row.get("device") or "").strip()
    if "@" in policy_object_name(row):
        head = f"{configured_name}: variant"
    else:
        head = f"{configured_name}"
    return _description(
        f"{head} defined identically on {count} device(s), e.g. {device} (Forward)"
    )


def _policy_name_or_skip(runner, row, model_string, policy_name):
    name = policy_object_name(row)
    if not name:
        raise ForwardQueryError(
            "Policy row did not include its catalogue `name`.",
            model_string=model_string,
            context={"device": row.get("device"), "configured_name": policy_name},
            data=row,
        )
    if len(name) > _NAME_MAX_LENGTH:
        runner._record_aggregated_skip_warning(
            model_string=model_string,
            reason=POLICY_NAME_TOO_LONG_REASON,
            warning_message=(
                f"Skipping {model_string} row because `{name}` exceeds "
                f"{_NAME_MAX_LENGTH} characters."
            ),
            sample=name,
        )
        return None
    return name


def _description(text):
    return str(text or "")[:_DESCRIPTION_MAX_LENGTH]


def _int_or_none(value):
    if value in (None, ""):
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


# --- prefix lists -----------------------------------------------------------


def prefix_length_bounds(prefix, *, ge=None, le=None, eq=None):
    """What `PrefixListEntry.clean()` will accept for this prefix.

    Returns `(ge, le, representable)`. `eq N` is `ge N le N`, which the
    plugin accepts (its check is strictly `ge < le`). A pair with `ge < le`
    - the normal IOS form - or a bound not longer than the prefix is rejected
    by the plugin, so `representable` is False and the caller stores neither.
    """
    ge = _int_or_none(ge)
    le = _int_or_none(le)
    eq = _int_or_none(eq)
    if eq is not None:
        ge, le = eq, eq
    if ge is None and le is None:
        return None, None, True
    try:
        network = ip_network(str(prefix), strict=False)
    except ValueError:
        return None, None, False
    boundary = 32 if network.version == 4 else 128
    for bound in (ge, le):
        if bound is not None and (bound > boundary or network.prefixlen >= bound):
            return None, None, False
    if ge is not None and le is not None and ge < le:
        return None, None, False
    return ge, le, True


def prefix_list_modifiers_text(*, ge=None, le=None, eq=None):
    parts = []
    if _int_or_none(ge) is not None:
        parts.append(f"ge {int(ge)}")
    if _int_or_none(le) is not None:
        parts.append(f"le {int(le)}")
    if _int_or_none(eq) is not None:
        parts.append(f"eq {int(eq)}")
    return " ".join(parts)


def ensure_custom_prefix(runner, prefix):
    CustomPrefix = runner._optional_model(
        "netbox_routing", "CustomPrefix", PREFIX_LIST_MODEL
    )
    values = runner._model_field_values(CustomPrefix, {"prefix": str(prefix)})
    obj, _ = runner._upsert_values_from_defaults(
        "netbox_routing.customprefix",
        CustomPrefix,
        values=values,
        coalesce_sets=[("prefix",)],
    )
    return obj


def ensure_prefix_list(runner, row):
    PrefixList = runner._optional_model(
        "netbox_routing", "PrefixList", PREFIX_LIST_MODEL
    )
    name = _policy_name_or_skip(runner, row, PREFIX_LIST_MODEL, row.get("list_name"))
    if name is None:
        return None
    family = 6 if str(row.get("family") or "4") == "6" else 4
    values = runner._model_field_values(
        PrefixList,
        {
            "name": name,
            "family": family,
            "description": policy_description(row, row.get("list_name")),
        },
    )
    prefix_list, _ = runner._upsert_values_from_defaults(
        "netbox_routing.prefixlist",
        PrefixList,
        values=values,
        coalesce_sets=[("name",)],
    )
    return prefix_list


def ensure_prefix_list_entry(runner, row, *, preview=False):
    PrefixListEntry = runner._optional_model(
        "netbox_routing", "PrefixListEntry", PREFIX_LIST_MODEL
    )
    sequence = _int_or_none(row.get("sequence"))
    prefix = str(row.get("prefix") or "").strip()
    if sequence is None or not prefix or not row.get("list_name"):
        raise ForwardQueryError(
            "Prefix-list row did not include `list_name`, `sequence` and `prefix`.",
            model_string=PREFIX_LIST_MODEL,
            context={"device": row.get("device"), "list_name": row.get("list_name")},
            data=row,
        )
    prefix_list = ensure_prefix_list(runner, row)
    if prefix_list is None:
        return False
    custom_prefix = ensure_custom_prefix(runner, prefix)
    ge, le, representable = prefix_length_bounds(
        prefix, ge=row.get("ge"), le=row.get("le"), eq=row.get("eq")
    )
    modifiers = prefix_list_modifiers_text(
        ge=row.get("ge"), le=row.get("le"), eq=row.get("eq")
    )
    description = ""
    if not representable:
        description = _description(f"{prefix} {modifiers}".strip())
        runner._record_aggregated_skip_warning(
            model_string=PREFIX_LIST_MODEL,
            reason=UNREPRESENTABLE_PREFIX_BOUNDS_REASON,
            warning_message=(
                f"Stored `{prefix_list.name}` seq {sequence} without bounds: "
                f"`{prefix} {modifiers}` is rejected by netbox-routing validation."
            ),
            sample=f"{prefix_list.name} seq {sequence}",
        )
    values = runner._model_field_values(
        PrefixListEntry,
        {
            "prefix_list": prefix_list,
            "sequence": sequence,
            "action": str(row.get("action") or "permit").lower(),
            "assigned_prefix_type": runner._content_type_for(custom_prefix.__class__),
            "assigned_prefix_id": custom_prefix.pk,
            "ge": ge,
            "le": le,
            "description": description,
        },
    )
    entry, _ = runner._upsert_values_from_defaults(
        PREFIX_LIST_MODEL,
        PrefixListEntry,
        values=values,
        coalesce_sets=[("prefix_list", "sequence")],
    )
    return entry


def apply_netbox_routing_prefixlistentry(runner, row, *, preview=False):
    entry = ensure_prefix_list_entry(runner, row, preview=preview)
    if preview:
        if entry is False:
            return False
        return preview_leaf_outcome(runner, entry)
    return entry


def _delete_parent_if_empty(runner, parent, related_name):
    if parent is None or getattr(parent, "pk", None) is None:
        return
    if getattr(parent, related_name).exists():
        return
    from django.db.models.deletion import ProtectedError

    try:
        forget_lookup_object(runner, parent)
        parent.delete()
    except ProtectedError:
        # An operator linked the list to a peer policy by hand; the empty
        # list stays, which is the same answer the BGP scope tree gives.
        return


def _resolve_policy_parent(runner, model, row, policy_name):
    # `_get_unique_or_raise` raises only on ambiguity; a miss is None.
    name = policy_object_name(row)
    if not name:
        return None
    return runner._get_unique_or_raise(model, {"name": name})


def delete_netbox_routing_prefixlistentry(runner, row):
    PrefixList = runner._optional_model(
        "netbox_routing", "PrefixList", PREFIX_LIST_MODEL
    )
    PrefixListEntry = runner._optional_model(
        "netbox_routing", "PrefixListEntry", PREFIX_LIST_MODEL
    )
    prefix_list = _resolve_policy_parent(runner, PrefixList, row, row.get("list_name"))
    if prefix_list is None:
        return False
    sequence = _int_or_none(row.get("sequence"))
    if sequence is None:
        return False
    deleted = runner._delete_by_coalesce(
        PrefixListEntry, [{"prefix_list": prefix_list, "sequence": sequence}]
    )
    _delete_parent_if_empty(runner, prefix_list, "prefix_list_entries")
    return deleted


# --- community lists --------------------------------------------------------


def community_value(value):
    """The numeric community netbox-routing will store, or None."""
    text = str(value or "").strip().strip('"')
    if not text or not _COMMUNITY_VALUE.match(text):
        return None
    return text


def ensure_community(runner, value):
    Community = runner._optional_model(
        "netbox_routing", "Community", COMMUNITY_LIST_MODEL
    )
    values = runner._model_field_values(Community, {"community": value})
    community, _ = runner._upsert_values_from_defaults(
        "netbox_routing.community",
        Community,
        values=values,
        coalesce_sets=[("community",)],
    )
    return community


def ensure_community_list(runner, row):
    CommunityList = runner._optional_model(
        "netbox_routing", "CommunityList", COMMUNITY_LIST_MODEL
    )
    name = _policy_name_or_skip(runner, row, COMMUNITY_LIST_MODEL, row.get("list_name"))
    if name is None:
        return None
    values = runner._model_field_values(
        CommunityList,
        {
            "name": name,
            "description": policy_description(row, row.get("list_name")),
        },
    )
    community_list, _ = runner._upsert_values_from_defaults(
        "netbox_routing.communitylist",
        CommunityList,
        values=values,
        coalesce_sets=[("name",)],
    )
    return community_list


def ensure_community_list_entry(runner, row, *, preview=False):
    CommunityListEntry = runner._optional_model(
        "netbox_routing", "CommunityListEntry", COMMUNITY_LIST_MODEL
    )
    if not row.get("list_name"):
        raise ForwardQueryError(
            "Community-list row did not include `list_name`.",
            model_string=COMMUNITY_LIST_MODEL,
            context={"device": row.get("device")},
            data=row,
        )
    label = policy_object_name(row)
    if str(row.get("form") or "").lower() == "expanded":
        runner._record_aggregated_skip_warning(
            model_string=COMMUNITY_LIST_MODEL,
            reason=EXPANDED_COMMUNITY_LIST_REASON,
            warning_message=f"Skipping expanded community list `{label}`.",
            sample=label,
        )
        return False
    value = community_value(row.get("community"))
    if value is None:
        runner._record_aggregated_skip_warning(
            model_string=COMMUNITY_LIST_MODEL,
            reason=NON_NUMERIC_COMMUNITY_REASON,
            warning_message=(
                f"Skipping `{label}` entry `{row.get('community')}`: not a numeric community."
            ),
            sample=f"{label} {row.get('community')}",
        )
        return False
    community_list = ensure_community_list(runner, row)
    if community_list is None:
        return False
    community = ensure_community(runner, value)
    values = runner._model_field_values(
        CommunityListEntry,
        {
            "community_list": community_list,
            "community": community,
            "action": str(row.get("action") or "permit").lower(),
        },
    )
    entry, _ = runner._upsert_values_from_defaults(
        COMMUNITY_LIST_MODEL,
        CommunityListEntry,
        values=values,
        coalesce_sets=[("community_list", "community")],
    )
    return entry


def apply_netbox_routing_communitylistentry(runner, row, *, preview=False):
    entry = ensure_community_list_entry(runner, row, preview=preview)
    if preview:
        if entry is False:
            return False
        return preview_leaf_outcome(runner, entry)
    return entry


def delete_netbox_routing_communitylistentry(runner, row):
    CommunityList = runner._optional_model(
        "netbox_routing", "CommunityList", COMMUNITY_LIST_MODEL
    )
    CommunityListEntry = runner._optional_model(
        "netbox_routing", "CommunityListEntry", COMMUNITY_LIST_MODEL
    )
    Community = runner._optional_model(
        "netbox_routing", "Community", COMMUNITY_LIST_MODEL
    )
    community_list = _resolve_policy_parent(
        runner, CommunityList, row, row.get("list_name")
    )
    if community_list is None:
        return False
    value = community_value(row.get("community"))
    if value is None:
        return False
    community = runner._get_unique_or_raise(Community, {"community": value})
    if community is None:
        return False
    deleted = runner._delete_by_coalesce(
        CommunityListEntry,
        [{"community_list": community_list, "community": community}],
    )
    _delete_parent_if_empty(runner, community_list, "communitylistentries")
    return deleted


# --- route maps -------------------------------------------------------------

# Sub-verbs that extend the clause key rather than name a value:
# `set as-path prepend 65000` keys on `as_path_prepend`, but `match as-path
# LOCAL_ONLY` keys on `as_path` with the list name as its value.
_SUB_VERBS = {
    "ip": {"address", "next-hop", "route-source", "precedence"},
    "ipv6": {"address", "next-hop", "route-source"},
    "as-path": {"prepend", "tag"},
    "extcommunity": {"rt", "soo", "vpn-distinguisher"},
}
_LIST_KINDS = frozenset({"prefix-list", "access-list"})
# Trailing flags that qualify a clause rather than name a value.
_TRAILING_FLAGS = frozenset({"exact-match", "additive", "delete"})


def _clause_key(tokens):
    """`match ip address prefix-list X` -> ("ip_address_prefix_list", ["X"], [])."""
    head = tokens[0].lower()
    rest = list(tokens[1:])
    key = [head]
    if rest and rest[0].lower() in _SUB_VERBS.get(head, ()):
        key.append(rest.pop(0).lower())
        if head in ("ip", "ipv6") and rest and rest[0].lower() in _LIST_KINDS:
            key.append(rest.pop(0).lower())
    flags = [token.lower() for token in rest if token.lower() in _TRAILING_FLAGS]
    values = [token for token in rest if token.lower() not in _TRAILING_FLAGS]
    return "_".join(part.replace("-", "_") for part in key), values, flags


def parse_route_map_clauses(clauses):
    """The stanza's child lines as the entry's fields.

    `match` and `set` become JSON mappings keyed by the clause head with the
    remaining tokens as a list (values that are lists of names, like
    `match ip address prefix-list A B`, stay lists; a qualifier such as
    `exact-match` or `additive` becomes `<key>_<flag>: true`). `continue N`
    is `flow_control`; `description` is the description. Anything else is
    kept verbatim under `other` so nothing is silently dropped.
    """
    match = {}
    set_ = {}
    other = []
    flow_control = None
    description = ""
    for raw in clauses or ():
        text = str(raw or "").strip()
        if not text:
            continue
        tokens = text.split()
        verb = tokens[0].lower()
        if verb == "match" and len(tokens) > 1:
            key, values, flags = _clause_key(tokens[1:])
            match.setdefault(key, []).extend(values)
            for flag in flags:
                match[f"{key}_{flag.replace('-', '_')}"] = True
        elif verb == "set" and len(tokens) > 1:
            key, values, flags = _clause_key(tokens[1:])
            set_.setdefault(key, []).extend(values)
            for flag in flags:
                set_[f"{key}_{flag.replace('-', '_')}"] = True
        elif verb == "continue":
            flow_control = _int_or_none(tokens[1]) if len(tokens) > 1 else None
            if flow_control is None:
                other.append(text)
        elif verb == "description":
            description = " ".join(tokens[1:])
        else:
            other.append(text)
    if other:
        match["other"] = other
    return {
        "match": match or None,
        "set": set_ or None,
        "flow_control": flow_control,
        "description": _description(description),
    }


def ensure_route_map(runner, row):
    RouteMap = runner._optional_model("netbox_routing", "RouteMap", ROUTE_MAP_MODEL)
    name = _policy_name_or_skip(runner, row, ROUTE_MAP_MODEL, row.get("map_name"))
    if name is None:
        return None
    values = runner._model_field_values(
        RouteMap,
        {
            "name": name,
            "description": policy_description(row, row.get("map_name")),
        },
    )
    route_map, _ = runner._upsert_values_from_defaults(
        "netbox_routing.routemap",
        RouteMap,
        values=values,
        coalesce_sets=[("name",)],
    )
    return route_map


def ensure_route_map_entry(runner, row, *, preview=False):
    RouteMapEntry = runner._optional_model(
        "netbox_routing", "RouteMapEntry", ROUTE_MAP_MODEL
    )
    sequence = _int_or_none(row.get("sequence"))
    if sequence is None or not row.get("map_name"):
        raise ForwardQueryError(
            "Route-map row did not include `map_name` and `sequence`.",
            model_string=ROUTE_MAP_MODEL,
            context={"device": row.get("device"), "map_name": row.get("map_name")},
            data=row,
        )
    route_map = ensure_route_map(runner, row)
    if route_map is None:
        return False
    parsed = parse_route_map_clauses(row.get("clauses"))
    values = runner._model_field_values(
        RouteMapEntry,
        {
            "route_map": route_map,
            "sequence": sequence,
            "action": str(row.get("action") or "permit").lower(),
            "flow_control": parsed["flow_control"],
            "match": parsed["match"],
            "set": parsed["set"],
            "description": parsed["description"],
        },
    )
    entry, _ = runner._upsert_values_from_defaults(
        ROUTE_MAP_MODEL,
        RouteMapEntry,
        values=values,
        coalesce_sets=[("route_map", "sequence")],
    )
    return entry


def apply_netbox_routing_routemapentry(runner, row, *, preview=False):
    entry = ensure_route_map_entry(runner, row, preview=preview)
    if preview:
        if entry is False:
            return False
        return preview_leaf_outcome(runner, entry)
    return entry


def delete_netbox_routing_routemapentry(runner, row):
    RouteMap = runner._optional_model("netbox_routing", "RouteMap", ROUTE_MAP_MODEL)
    RouteMapEntry = runner._optional_model(
        "netbox_routing", "RouteMapEntry", ROUTE_MAP_MODEL
    )
    route_map = _resolve_policy_parent(runner, RouteMap, row, row.get("map_name"))
    if route_map is None:
        return False
    sequence = _int_or_none(row.get("sequence"))
    if sequence is None:
        return False
    deleted = runner._delete_by_coalesce(
        RouteMapEntry, [{"route_map": route_map, "sequence": sequence}]
    )
    _delete_parent_if_empty(runner, route_map, "route_map_entries")
    return deleted


__all__ = (
    "COMMUNITY_LIST_MODEL",
    "EXPANDED_COMMUNITY_LIST_REASON",
    "NON_NUMERIC_COMMUNITY_REASON",
    "POLICY_NAME_TOO_LONG_REASON",
    "PREFIX_LIST_MODEL",
    "ROUTE_MAP_MODEL",
    "ROUTING_POLICY_MODELS",
    "ROUTING_POLICY_ROLLUP_REASONS",
    "UNREPRESENTABLE_PREFIX_BOUNDS_REASON",
    "apply_netbox_routing_communitylistentry",
    "apply_netbox_routing_prefixlistentry",
    "apply_netbox_routing_routemapentry",
    "community_value",
    "delete_netbox_routing_communitylistentry",
    "delete_netbox_routing_prefixlistentry",
    "delete_netbox_routing_routemapentry",
    "ensure_community",
    "ensure_community_list",
    "ensure_community_list_entry",
    "ensure_custom_prefix",
    "ensure_prefix_list",
    "ensure_prefix_list_entry",
    "ensure_route_map",
    "ensure_route_map_entry",
    "parse_route_map_clauses",
    "policy_description",
    "policy_object_name",
    "prefix_length_bounds",
    "prefix_list_modifiers_text",
)
