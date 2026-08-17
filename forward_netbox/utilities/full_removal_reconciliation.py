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


# Models a Forward NQE DIFF may remove. Also an ALLOWLIST, and for the same
# reasons - it was missing for two releases after the baseline path got one.
#
# The two producers were guarded asymmetrically. `BASELINE_REMOVAL_MODELS`
# refuses `dcim.site`, `dcim.devicetype`, `ipam.vrf` and `dcim.device` by name,
# with the reasoning written out directly above. `_split_diff_rows` refused
# nothing: every `DELETED` row became a delete for any model, and the only gate
# downstream is the ACI suppression check. So the exact models one path
# protects, the other deleted unattended.
#
# A deployment on 2.8.1 showed it: an `ipam.vrf`, a `dcim.devicetype` and three
# `dcim.site` rows all reached `delete()`. Nothing was lost, because PROTECT
# refused them - a database constraint caught what a gate should have. A site
# with no devices, or a device type with no devices, has nothing holding it.
#
# The diff signal is NOT stronger evidence than baseline absence, which is the
# tempting reason to allow more here. A `DELETED` row means the row was in the
# query result at the before-snapshot and is not at the after-snapshot, and a
# device disabled in Forward, a failed collection, and a narrowed query all
# produce exactly that.
#
# This governs `DELETED` rows only. A `MODIFIED` row whose identity key changed
# is a RENAME: Forward reports the same object under a new identity, and the
# after-side is written in the same batch, so the before-row is superseded by a
# row we are creating. Refusing that preserves nothing and strands a duplicate
# nothing will collect. See `_split_diff_rows`, which draws the line.
#
# Wider than the baseline list on purpose, though. Baseline reconciliation
# compares against rows the plugin itself persisted, so it can only speak for
# models it has a baseline for; the diff speaks for whatever the query covers.
# The additions are all rows the plugin solely authors and no operator
# maintains by hand.
#
# REFUSED here, with the same reasons as the baseline list:
#   dcim.device, dcim.site      - operator-gated through Scope Reconciliation
#                                 -> Prune orphans, which exists precisely
#                                 because absence is not evidence
#   dcim.devicetype/platform/   - shared catalogues; an empty one is not garbage
#     manufacturer/devicerole     and may be a Device Type Library import
#   ipam.prefix/vlan/vrf        - global IPAM, never pruned by device scope
#   netbox_dlm.softwareversion  - a catalogue with children
#
# `DIFF_REMOVAL_REFUSED_MODELS` below names them, and a test asserts every
# delete handler on the runner appears in exactly one of the two sets - so a
# model added later cannot land in neither and silently inherit "deletable".
DIFF_REMOVAL_MODELS = frozenset(
    BASELINE_REMOVAL_MODELS
    | {
        # Device-derived and plugin-authored, like the baseline set.
        "dcim.virtualchassis",
        "extras.taggeditem",
        "netbox_peering_manager.peeringsession",
        # ACI inventory. Kept deletable because a stale fabric object is real
        # garbage, and this path already carries its own brake:
        # `should_suppress_aci_deletes` holds every ACI delete back unless
        # `aci_allow_deletes` is set, because a failed APIC collection empties
        # the fabric query.
        "netbox_cisco_aci.acibridgedomain",
        "netbox_cisco_aci.acifabric",
        "netbox_cisco_aci.acifilter",
        "netbox_cisco_aci.acil3out",
        "netbox_cisco_aci.acinode",
        "netbox_cisco_aci.acipod",
        "netbox_cisco_aci.acitenant",
        "netbox_cisco_aci.acivrf",
    }
)


# The other half of the partition. Not decorative: the parity test reads it, so
# removing a model from here without adding it above breaks the build rather
# than quietly making it deletable.
DIFF_REMOVAL_REFUSED_MODELS = frozenset(
    {
        "dcim.device",
        "dcim.site",
        "dcim.devicetype",
        "dcim.platform",
        "dcim.manufacturer",
        "dcim.devicerole",
        "ipam.prefix",
        "ipam.vlan",
        "ipam.vrf",
        "netbox_dlm.softwareversion",
    }
)


# Models the operator's tag-scope prune may remove. The THIRD producer, found
# the same way as the second: a customer on 2.8.2 still had six
# `netbox_dlm.softwareversion` protected-delete skips after both other paths
# were gated, because rows filtered out by device-tag scope become deletes
# whenever `device_tag_prune_out_of_scope` is on, and nothing consulted a model
# policy on the way.
#
# Wider than the diff list in exactly one respect, and it has to be: removing
# devices and their sites IS what Prune orphans is for, and an operator turned
# it on deliberately after a shrink guard and a confirm-in-Forward warning.
#
# But "this device left tag scope" is not a statement about a SHARED CATALOGUE.
# A software version, a device type or a VRF is not device-derived, so device
# scope cannot speak for it - the same sentence the baseline list already
# carries for the same models. Pruning them because a device moved out of scope
# deletes rows the operator never chose to delete and that Forward will
# recreate.
PRUNE_REMOVAL_MODELS = frozenset(DIFF_REMOVAL_MODELS | {"dcim.device", "dcim.site"})


def prune_removals_allowed(model_string) -> bool:
    """Whether the tag-scope prune may delete rows of this model.

    Fail closed, like its siblings.
    """
    return model_string in PRUNE_REMOVAL_MODELS


def diff_removals_allowed(model_string) -> bool:
    """Whether a Forward diff may turn `DELETED` rows into deletes.

    Fail closed. A model in neither set is unknown to this policy, and the
    2.7.11 regression is what "unknown defaults to deletable" costs.
    """
    return model_string in DIFF_REMOVAL_MODELS


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
