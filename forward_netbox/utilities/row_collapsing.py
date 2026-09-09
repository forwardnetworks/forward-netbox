# Rows that write the same object, collapsed into one before anything reads them.
#
# Some Forward queries return one row per OBSERVATION while the apply upserts
# one object per IDENTITY. `forward_ospf_interfaces.nqe` selects
# `foreach neighbor in area.neighbors`, so a broadcast segment with three
# neighbours yields three rows carrying the same `local_interface`;
# `ensure_ospf_interface` coalesces on `("interface",)`, so all three collapse
# onto one OSPFInterface.
#
# Left alone that is not merely wasteful, it never converges. Each row writes
# its own `comments`, the last writer wins, and every later comparison finds
# the other rows still wanting to write theirs - so they report as drift on
# every run, forever, and no sync can resolve them. A customer estate showed
# 180 of 2854 OSPF interface rows permanently drifted, which is what makes
# `In sync` a question that can never be answered Yes.
#
# The collapse happens ONCE, upstream of both the apply and the comparison,
# because the bug this fixes is precisely the two disagreeing about how many
# objects a set of rows means. A collapse applied to only one of them would be
# the same defect wearing different clothes.
from .sync_routing_impl import COLLAPSED_COMMENTS_KEY
from .sync_routing_impl import ospf_interface_comments
from .sync_routing_impl import ROUTING_INTERFACE_PREFIX_ALIASES


def _canonical_interface(name):
    """The name the interface lookup will settle on, for grouping purposes.

    Expands the LONGEST matching alias, reusing the lookup's own table, so
    `gi0/0` and `GigabitEthernet0/0` group together exactly when they would
    resolve to the same NetBox interface. Longest wins because the table holds
    both `gi` and `gigabitethernet`, and the short one matches the long one's
    expansion too - taking any match would map a name onto itself twice over.
    """
    raw = str(name or "").strip()
    if not raw:
        return ""
    lowered = raw.lower()
    match = None
    for alias, canonical in ROUTING_INTERFACE_PREFIX_ALIASES:
        if lowered.startswith(alias) and (match is None or len(alias) > len(match[0])):
            match = (alias, canonical)
    if match is None:
        return lowered
    alias, canonical = match
    return f"{canonical}{raw[len(alias):]}".lower()


# The fields that differ between neighbours on one interface. Everything else
# in the row describes the interface itself and is identical across the group.
_NEIGHBOUR_LABELS = (
    ("Remote device", "remote_device"),
    ("Remote interface", "remote_interface"),
    ("Remote interface IP", "remote_interface_ip"),
    ("Remote router ID", "remote_router_id"),
)


def _neighbour_sort_key(row):
    return tuple(str(row.get(key) or "") for _label, key in _NEIGHBOUR_LABELS)


def _ospf_interface_key(row):
    return (
        str(row.get("device") or ""),
        _canonical_interface(row.get("local_interface")),
    )


def _merge_ospf_interface_rows(rows):
    """One row per interface, carrying every neighbour seen on it.

    Sorted, so the merged text is identical on every run whatever order
    Forward returned the neighbours in - an unsorted merge would swap the
    churn for a subtler one.

    A single-neighbour interface returns the untouched row, so the overwhelming
    majority of an estate keeps byte-identical comments and does not rewrite.
    """
    ordered = sorted(rows, key=_neighbour_sort_key)
    if len(ordered) == 1:
        return ordered[0]

    blocks = [ospf_interface_comments(ordered[0])]
    for row in ordered[1:]:
        lines = [
            f"{label}: {row.get(key)}"
            for label, key in _NEIGHBOUR_LABELS
            if row.get(key) not in ("", None)
        ]
        if lines:
            blocks.append("\n".join(lines))

    merged = dict(ordered[0])
    merged[COLLAPSED_COMMENTS_KEY] = "\n\n".join(blocks)
    return merged


def _ospf_area_key(row):
    return str(row.get("area_id") or "")


def _merge_ospf_area_rows(rows):
    """Every device in an area contributes a row; the area is one object.

    Nothing is merged into the text - the area's fields are the area's, not the
    reporting device's. The rows are sorted so that when two of them disagree
    about `area_type` the same one wins on every run. That disagreement is a
    data conflict worth surfacing separately; what must not happen is the two
    taking turns and reporting drift forever.
    """
    return sorted(rows, key=lambda row: str(row.get("area_type") or ""))[0]


_COLLAPSERS = {
    "netbox_routing.ospfinterface": (_ospf_interface_key, _merge_ospf_interface_rows),
    "netbox_routing.ospfarea": (_ospf_area_key, _merge_ospf_area_rows),
}


def collapses_rows(model_string) -> bool:
    return model_string in _COLLAPSERS


def collapse_rows(model_string, rows):
    """Rows reduced to one per object the apply would write.

    Order-preserving on first appearance, so the apply still walks the estate
    in the order Forward reported it and a log read against a previous run
    stays comparable.
    """
    collapser = _COLLAPSERS.get(model_string)
    if collapser is None:
        return rows

    key_of, merge = collapser
    groups: dict = {}
    for row in rows:
        groups.setdefault(key_of(row), []).append(row)
    return [merge(group) for group in groups.values()]
