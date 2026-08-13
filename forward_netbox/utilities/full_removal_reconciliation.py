# Removals for a FULL execution, computed against the promoted local baseline.
#
# Until this existed, removals reached NetBox one way only: a Forward NQE diff,
# which reports what the CURRENT query stopped returning. A full run computed
# none at all. So every row a map wrote before it was re-pointed at a different
# query was orphaned permanently - nothing revisited it, for any model. A
# customer hit it through the DLM hardware notices, where the leftovers render
# as a flat list beside their replacements and look like duplicate writes.
#
# The proof that the plugin wrote a row is the contributor baseline: the last
# promoted run persists every row each contract returned, chunked and
# checksummed. Comparing the current full result against it is a local
# operation, needs no extra Forward call, and - crucially - is indifferent to
# whether the CONTRACT changed. That is the whole point: a re-pointed map is
# exactly when the baseline and the current result disagree about identity, and
# exactly when the old rows need collecting.
#
# Identity is the model's COALESCE key, built here rather than borrowed from
# `row_shard_key`. That function is for bucketing work and is unsafe as a
# deletion identity in two ways: it falls back to a whole-row key when no
# coalesce set is complete, which turns any field change into "absent", and for
# device-scoped models it returns `device:<name>`, which every row of that
# device shares. Both are fine for sharding and wrong for deciding what to
# delete.
from .branch_budget import row_coalesce_field_is_complete
from .contributor_baseline import ContributorBaselineUnavailable
from .contributor_baseline import iter_relation_entries

# An independent brake, deliberately NOT the validation row-shrink guard.
#
# That guard runs at validation, blocks the whole run, and is the right first
# line - but it skips comparison entirely when the operator's scope
# configuration changed, and a scope change is precisely the situation that
# produces a large legitimate-looking removal set. It can also be relaxed by a
# drift policy. Removal is destructive and irreversible in a way a refused run
# is not, so it carries its own limit that no policy can widen.
MAX_REMOVAL_PERCENT = 30
MIN_REMOVAL_ROWS = 20


# Models whose query is NETWORK-COMPLETE: not tag-scoped, not sharded, one row
# per identity for the whole Forward network. For these the current full result
# is the authoritative set, so NetBox rows absent from it are stale no matter
# when they were written - which is the only way to reach rows orphaned before
# the contributor baseline that holds them was superseded.
#
# `netbox_dlm.hardwarenotice` qualifies precisely because it is NOT tag-scoped.
# That same property is what made the old "device type holds no devices" rule
# wrong: notices are written network-wide while devices are imported
# tag-scoped, so hardware outside the include tags permanently has no devices
# and is not stale at all.
#
# Adding a model here is a claim that the plugin is the sole author of every row
# in that table. Do not add a tag-scoped model: its result covers part of the
# estate, and everything outside would be deleted.
NETWORK_COMPLETE_MODELS = ("netbox_dlm.hardwarenotice",)


# Models a baseline comparison may remove. An ALLOWLIST, deliberately.
#
# This started as "every model", which is how it shipped in 2.7.11, and that was
# wrong in a way no threshold catches: it made a full sync delete DEVICES that
# were absent from the current result. Device removal is gated behind Scope
# Reconciliation -> Prune orphans, with a shrink guard and an explicit "confirm
# in Forward before deleting anything" warning, precisely because absence from a
# query result is not evidence a device is gone. Reconciling devices here
# bypassed that gate entirely and did it unattended, on every full run.
#
# A deployment on 2.7.12 showed it in one run: one `dcim.device` ProtectedError
# and five `netbox_dlm.softwareversion` protected-delete skips, with their
# untagged device count dropping by 18.
#
# So the rule is now: only models the plugin solely authors, whose rows are
# derived from a device that still exists, and whose deletion an operator would
# never be asked to review one at a time.
#
# NOT here, on purpose:
#   dcim.device, dcim.site      - operator-gated through the prune flow
#   dcim.devicetype/platform/   - shared catalogues; an empty one is not garbage
#     manufacturer/devicerole     and may be a Device Type Library import
#   ipam.prefix/vlan/vrf        - global IPAM, never pruned by device scope
#   netbox_dlm.softwareversion  - a catalogue with children; the protected-delete
#                                 skips above are exactly this
BASELINE_REMOVAL_MODELS = frozenset(
    {
        "dcim.interface",
        "dcim.macaddress",
        "dcim.inventoryitem",
        "dcim.module",
        "dcim.cable",
        "ipam.ipaddress",
        "ipam.fhrpgroup",
        "netbox_dlm.hardwarenotice",
        "netbox_dlm.devicesoftware",
        "netbox_dlm.inventoryitemsoftware",
        "netbox_dlm.cve",
        "netbox_dlm.vulnerability",
        "netbox_routing.bgppeer",
        "netbox_routing.bgpaddressfamily",
        "netbox_routing.bgppeeraddressfamily",
        "netbox_routing.ospfinstance",
        "netbox_routing.ospfarea",
        "netbox_routing.ospfinterface",
    }
)


class RemovalReconciliationRefused(Exception):
    """The removal set was too large a share of the baseline to trust."""


def previous_full_rows(sync, model_string):
    """Rows the current promoted baseline recorded for a model, or None.

    ``None`` means "cannot prove what was written" - no baseline, or a payload
    that failed its own checksum - and every caller must treat it as "remove
    nothing". A corrupt baseline is a reason to delete less, never more.
    """
    from ..models import ForwardContributorBaseline

    baseline = (
        ForwardContributorBaseline.objects.filter(sync=sync, is_current=True)
        .prefetch_related("relations__chunks")
        .first()
    )
    if baseline is None:
        return None
    rows = []
    try:
        # Every relation for this model, NOT only the one matching the current
        # contract key. A map re-pointed at a different query writes its rows
        # under a new contract key, and the previous relation is precisely what
        # nothing else will ever look at again.
        for relation in baseline.relations.filter(model_string=model_string):
            for _identity, _target_key, row in iter_relation_entries(relation):
                rows.append(row)
    except ContributorBaselineUnavailable:
        return None
    return rows


def coalesce_identity(model_string, row, coalesce_fields):
    """The row's object identity, or None when it cannot be established.

    Only a COMPLETE coalesce set counts. A row missing part of its identity
    cannot be matched against the other side either, so treating it as absent
    would delete on the strength of a missing field.
    """
    for field_set in coalesce_fields or ():
        values = []
        complete = True
        for field_name in field_set:
            if not row_coalesce_field_is_complete(model_string, row, field_name):
                complete = False
                break
            values.append(f"{field_name}={row.get(field_name)}")
        if complete and values:
            return "|".join(values)
    return None


def network_complete_removals(model_string, *, current_rows):
    """Rows in NetBox that a network-complete result no longer covers.

    Separate from the baseline comparison on purpose. The baseline can only
    speak for rows it recorded, so it never sees an orphan created before the
    current baseline was written - a map re-pointed months ago leaves rows that
    no later baseline mentions. A network-complete result speaks for the whole
    table, so it does.

    Returns `(delete_rows, refusal_reason)`. Any inability to establish
    completeness yields no removals and a reason, never a partial deletion.
    """
    if model_string not in NETWORK_COMPLETE_MODELS:
        return [], ""
    if not current_rows:
        return [], "the result was empty, which cannot be told from a failed fetch"
    if model_string == "netbox_dlm.hardwarenotice":
        from .dlm_notice_audit import emitted_device_type_slugs
        from .dlm_notice_audit import stale_hardware_notices

        # sample_limit=None: this path DELETES, and a sample would act on the
        # first page while reporting the full count.
        report = stale_hardware_notices(
            emitted_device_type_slugs(current_rows),
            sample_limit=None,
        )
        if not report["available"]:
            return [], report["reason"]
        # Shaped for `delete_netbox_dlm_hardwarenotice`, which resolves the
        # device type by slug.
        return (
            [
                {"device_type": row["model"], "device_type_slug": row["slug"]}
                for row in report["stale_notices"]
            ],
            "",
        )
    return [], ""


def _key_set(model_string, rows, coalesce_fields):
    keys = {}
    for row in rows:
        key = coalesce_identity(model_string, row, coalesce_fields)
        if key is None:
            continue
        keys.setdefault(key, row)
    return keys


def compute_full_removals(
    model_string,
    *,
    current_rows,
    previous_rows,
    coalesce_fields,
    max_removal_percent=MAX_REMOVAL_PERCENT,
    min_removal_rows=MIN_REMOVAL_ROWS,
):
    """Baseline rows absent from the current full result.

    Raises `RemovalReconciliationRefused` when the removal set is large enough
    to look like a narrowed query rather than real churn.
    """
    if model_string not in BASELINE_REMOVAL_MODELS:
        # Not an oversight and not a threshold: this model's rows are either
        # operator-gated, shared, or not solely ours to delete.
        return []
    if previous_rows is None:
        return []
    if not current_rows:
        # An empty result is the single most dangerous input: a query that
        # failed open, a permission change, an emptied collection region. It
        # would remove the entire model.
        return []
    previous_keys = _key_set(model_string, previous_rows, coalesce_fields)
    if not previous_keys:
        return []
    current_keys = set(_key_set(model_string, current_rows, coalesce_fields))
    removals = [row for key, row in previous_keys.items() if key not in current_keys]
    if not removals:
        return []
    dropped_percent = len(removals) / len(previous_keys) * 100
    if len(removals) >= min_removal_rows and dropped_percent > max_removal_percent:
        raise RemovalReconciliationRefused(
            f"{len(removals)} of {len(previous_keys)} baseline rows for "
            f"{model_string} are absent from this full result "
            f"({dropped_percent:.1f}%), past the {max_removal_percent}% limit. "
            "Removing them is refused because a narrowed query looks exactly "
            "like this. Nothing is removed for this model; the rest of the run "
            "is unaffected."
        )
    return removals
