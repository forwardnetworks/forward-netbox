# The numbers a customer reads off the Scope Reconciliation page, in the page's
# own words, so a figure they quote can be found in their support bundle by the
# label they used. The raw report is in the bundle too, but under field names
# nobody reads aloud: a customer's "278 uncovered" was the page's "Carry no
# include tag" (`unmanaged.untagged_total`), and searching the bundle for the
# number or the word found neither.
#
# Counts and device primary keys only. Names stay out, as everywhere else in the
# bundle; keys are not customer data (NetBox's own device table renders them).

PAGE = "Scope Reconciliation"

# The most keys one list may carry: enough to name every device of a large
# estate's unclaimed or orphan set, small enough to keep the file readable.
PK_LIST_LIMIT = 5000

# (the page's label, how to read it from the stored report). A dotted path walks
# the report; a tuple adds the parts shown beside the total on the page.
FIGURES = (
    ("NetBox devices", "netbox_device_count", ()),
    ("In scope (collected)", "forward_in_scope_completed", ()),
    ("In scope (SNMP endpoints)", "forward_in_scope_endpoints", ()),
    (
        "Tagged but backfilled",
        "forward_tagged_backfilled",
        (("view tagged", "netbox_present_backfilled"),),
    ),
    ("Out of scope (orphans)", "netbox_out_of_scope", ()),
    ("Empty orphan sites", "netbox_empty_orphan_site_count", ()),
    ("In scope, missing from NetBox", "forward_missing_in_netbox", ()),
    (
        "Carry no include tag",
        "unmanaged.untagged_total",
        (
            ("created by this sync", "unmanaged.owned_untagged"),
            ("not claimed by this sync", "unmanaged.unclaimed"),
        ),
    ),
)


def _walk(report, path):
    value = report
    for part in path.split("."):
        if not isinstance(value, dict):
            return None
        value = value.get(part)
    return value


def scope_figures(report):
    """The page's figures, each with the parts it shows beside the total."""
    report = report if isinstance(report, dict) else {}
    # The tag names are the operator's own words (often people's names), so the
    # row carries how many there are, as `scope_configuration` does.
    rows = [
        {
            "page": PAGE,
            "label": "Include tags",
            "value": len(report.get("include_tags") or ()),
            "note": "count only; the names are not exported",
        }
    ]
    for label, path, parts in FIGURES:
        value = _walk(report, path)
        if value is None:
            continue
        row = {"page": PAGE, "label": label, "value": value}
        if parts:
            row["parts"] = [
                {"label": part_label, "value": _walk(report, part_path)}
                for part_label, part_path in parts
                if _walk(report, part_path) is not None
            ]
        rows.append(row)
    return rows


def _is_pk_list(value):
    return isinstance(value, list) and all(
        isinstance(item, int) and not isinstance(item, bool) for item in value
    )


def device_pk_lists(report):
    """Every ``*_device_ids`` list in the report, keyed by its dotted path less
    that suffix (``unmanaged.unclaimed``), with ``count``, ``truncated``, ``pks``.

    The export filter turns a key ending ``_device_ids`` or ``_pks`` into a
    count, which says how many devices sit in a set but never which, and "which"
    is what a reader needs to look the set up. Keys are not customer data (the
    filter's own docstring says a bundle carries them), so these are kept under
    keys that filter leaves alone. A list past ``PK_LIST_LIMIT`` is cut and says so.
    """
    found = {}

    def visit(node, prefix):
        if not isinstance(node, dict):
            return
        for key, value in node.items():
            path = f"{prefix}{key}"
            if key.endswith("_device_ids") and _is_pk_list(value):
                found[path[: -len("_device_ids")]] = {
                    "count": len(value),
                    "truncated": len(value) > PK_LIST_LIMIT,
                    "pks": sorted(value)[:PK_LIST_LIMIT],
                }
            elif isinstance(value, dict):
                visit(value, path + ".")

    visit(report, "")
    return found


def _in_sync_badge(row):
    if not row.get("comparison_available"):
        if row.get("comparison_unavailable_reason") == "full_only_by_design":
            return "By design"
        error = row.get("comparison_error")
        return f"Not measured ({error})" if error else "Not measured"
    return "Yes" if row.get("in_sync") else "No"


def drift_table(report):
    """The Drift report's per-model table, one dict per row keyed by the page's
    column headings, with the small lines under Pending removals and the words
    the In sync badge shows. Reads the same computed report the page renders."""
    rows = []
    for row in (report or {}).get("models") or ():
        removals = {"value": row.get("pending_removes")}
        if row.get("already_removed"):
            removals["declared, not staged"] = row["already_removed"]
        if row.get("outside_scope_in_netbox"):
            removals["outside scope"] = row["outside_scope_in_netbox"]
        rows.append(
            {
                "Model": row.get("label") or row.get("model"),
                "Forward rows": row.get("forward_rows"),
                "Upsert/change candidates": row.get("pending_changes"),
                "Pending removals": removals,
                "Estimated apply work": row.get("estimated_apply_work"),
                "Drift": (
                    row.get("drift") or 0
                    if row.get("comparison_available")
                    else "Not measured"
                ),
                "In sync": _in_sync_badge(row),
                "What it means": " ".join(
                    part for part in (row.get("meaning"), row.get("action")) if part
                ),
            }
        )
    return rows
