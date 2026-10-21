# What a drift row means, and what to do about it.
#
# The drift table shows five figures and a badge per model and says nothing about what
# any of them counts, so a customer reading "Forward rows 3,862, pending removals 29
# (+12 already removed), In sync: No" could not tell what the numbers were. This holds
# the plain wording.
#
# It is deliberately modest, because three audits found the figures cannot carry more:
#
#   * They are the dependency preview's estimates, not a check of NetBox row by row.
#   * A model's removals are clamped between what Forward declares and what the durable
#     state stages, and a sync can stage removals (an ownership sweep, missing entries)
#     the figure does not include, so it is neither a floor nor a ceiling on a sync.
#   * The candidates are every fetched row where a model could not be compared exactly,
#     and what a sync stages depends on its stored state, so it can differ.
#
# So the text says what is counted and what to look at next. It does not say what a sync
# will do to a row, does not say what a sync stages, does not say why a removal is counted, and does not promise a count
# is the most or least a sync will stage.
# (column, what it counts)
COLUMN_HELP = (
    (
        "Forward rows",
        "The rows the preview counted from Forward for this model. For the bundled "
        "device maps that is the devices matching the sync's tags, which may be "
        "smaller than your whole Forward network. For a model fetched as a "
        "difference it is the changed rows in that difference.",
    ),
    (
        "Upsert/change candidates",
        "The preview's estimate of rows to create or update. It is an estimate, not a "
        "promise of what a sync will stage.",
    ),
    (
        "Pending removals",
        "The removals the preview counts for this model, not checked row by row "
        "against NetBox. Whether a removal happens follows the sync's removal rules. "
        "The smaller line is how many more removals Forward declared than this run "
        "stages, for reasons such as an earlier removal, a row never imported, or "
        "protection.",
    ),
    (
        "Estimated apply work",
        "Candidates plus removals as counted here. It is an estimate, not a promise of "
        "what a sync will do: a sync can stage a different number.",
    ),
    (
        "Drift",
        "The same figure when the model could be compared row by row, and Not "
        "measured when it could not.",
    ),
    (
        "In sync",
        "Yes: nothing counted. No: the preview counted work for this model. By "
        "design or Not measured: see What it means.",
    ),
)

DEVICE_DENOMINATOR = (
    "For the bundled device maps, Forward rows counts the devices matching this "
    "sync's tag scope, which may be smaller than your whole Forward network. How the "
    "device numbers fit together, on the Health page, lays this out."
)


def _count(count, singular, plural):
    return f"{count} {singular if count == 1 else plural}"


def explain_drift_row(row):
    """``{"meaning", "action"}`` for one drift row; both empty when nothing is needed."""
    meaning = []
    action = []
    model = row.get("model")
    if not row.get("comparison_available"):
        reason = row.get("comparison_unavailable_reason") or ""
        if reason == "full_only_by_design":
            meaning.append(
                "No row-by-row comparison is made for this row, either because the map "
                "has no difference path or because its comparison is shown on another "
                "row for the same model. That is how it is built, not a fault."
            )
            action.append("Nothing to do.")
        elif reason == "error":
            meaning.append(
                "The comparison for this model failed; the other models were still "
                "attempted."
            )
            action.append(
                "Run Preview Dependencies again. If it fails the same way, send the "
                "support bundle."
            )
        else:
            meaning.append(
                "The figures on this row are an estimate, not a measured difference: "
                "the model has no exact row comparison, or its comparison is shown on "
                "another row for the same model."
            )
            action.append(
                "If every row reads this way, run Preview Dependencies again; "
                "otherwise nothing to do."
            )
    elif not row.get("in_sync"):
        changes = int(row.get("pending_changes") or 0)
        removes = int(row.get("pending_removes") or 0)
        if changes:
            meaning.append(
                f"{_count(changes, 'row is', 'rows are')} counted as new or "
                "different from NetBox."
            )
        if removes:
            meaning.append(
                f"{_count(removes, 'removal is', 'removals are')} counted for this "
                "model, not checked row by row against NetBox."
            )
            action.append("Whether a removal happens follows the sync's removal rules.")
            if model == "dcim.device":
                action.append(
                    "Scope Reconciliation lists the devices outside the tag scope."
                )
        already = int(row.get("already_removed") or 0)
        if already:
            meaning.append(
                f"Forward also declared {_count(already, 'removal', 'removals')} "
                "more than this run stages, for reasons such as an earlier removal, a "
                "row never imported, or protection."
            )
    if model == "dcim.device":
        meaning.append(DEVICE_DENOMINATOR)
    return {"meaning": " ".join(meaning), "action": " ".join(action)}
