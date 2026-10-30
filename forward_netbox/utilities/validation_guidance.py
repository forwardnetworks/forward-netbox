# What an operator does about each reason validation can block a sync for.
#
# The reason sentences say what was found. A user who lands on a blocked sync
# also needs the next step, and the one that mattered most in practice, a query
# that failed inside Forward, is not obvious: the plugin already resubmits it,
# so the right move is to run the sync again and, if it recurs, send the bundle.
#
# Matched on the plugin's own sentence openings (`validation.py`), never on
# anything Forward said, so it works the same for every deployment.
from .validation import COLLECTION_COLLAPSE_REASON_PREFIX
from .validation import ROW_SHRINK_REASON_PREFIX

_RE_RUN = (
    "Forward failed to answer a query this sync depends on. If Forward reported "
    "an internal error (the Model Results say so) the plugin already resubmitted "
    "it, so run the sync again. If the same query fails again, send the support "
    "bundle: it records the outcome of each failed query."
)

# (opening of the reason, guidance). First match wins.
_GUIDANCE = (
    ("`dcim.device` query failed", _RE_RUN),
    ("Foundational device metadata query failed", _RE_RUN),
    ("These query failures blocked the sync", ""),
    (
        "Target snapshot is not processed",
        "Wait for Forward to finish processing the snapshot, then run the sync "
        "again, or point the sync at a processed snapshot.",
    ),
    (
        "No rows were returned for enabled models",
        "A model you enabled returned nothing. Check that model's NQE map and "
        "the sync's device-tag scope, or disable the model if you do not want it.",
    ),
    (
        "Delete count",
        "More objects would be deleted than the drift policy allows. Review the "
        "pending removals on the Drift report. If they are expected, raise the "
        "limit or Force-Allow this run once.",
    ),
    (
        "Delete percentage",
        "More of this run would be deletions than the drift policy allows. Review "
        "the pending removals on the Drift report. If they are expected, raise "
        "the limit or Force-Allow this run once.",
    ),
    (
        ROW_SHRINK_REASON_PREFIX,
        "If the smaller row count is real (devices retired, scope narrowed), "
        "Force-Allow this run once. If not, a query is returning less than it "
        "used to: check the map's pinned commit or publish the bundled queries "
        "again.",
    ),
    (
        COLLECTION_COLLAPSE_REASON_PREFIX,
        "Check collection health in Forward before allowing this run: a failed "
        "collection still produces a normal-looking snapshot.",
    ),
)

_QUERY_FAILURES = "query failures were reported"
_QUERY_FAILURES_GUIDANCE = (
    "One or more queries failed. The Model Results below say which. Run the "
    "sync again; the plugin resubmits a query Forward failed on internally. If "
    "the same one fails again, send the support bundle."
)


def guidance_for(reason) -> str:
    """The next step for one blocking reason, or "" when there is none."""
    text = str(reason or "")
    for opening, guidance in _GUIDANCE:
        if text.startswith(opening):
            return guidance
    if _QUERY_FAILURES in text:
        return _QUERY_FAILURES_GUIDANCE
    return ""


def guidance_for_reasons(reasons) -> list:
    """The distinct next steps for a run's reasons, in the order first needed."""
    steps = []
    for reason in reasons or ():
        step = guidance_for(reason)
        if step and step not in steps:
            steps.append(step)
    return steps
