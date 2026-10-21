# What a skip reason means and what an operator can do about it.
#
# A skipped row records WHY as a short slug. The slug is exact but means nothing to
# the person reading the issue list: a customer saw 55 interface skips and a bundle
# said `subinterface-name-taken-by-parentless-interface`. Each slug gets a sentence
# a person can act on, whether they can fix it, and the next step. The reason slugs
# are an open set (`missing-<model>`, `still-referenced-by-<model>`, and named ones
# the raisers define), so anything not listed gets an honest generic answer rather
# than none.
from .primary_ip_reasons import NEEDS_A_PERSON

# slug -> (why, fixable, what to do)
KNOWN = {
    "subinterface-parent-not-found": (
        "The subinterface names a parent interface that is not on the device in "
        "NetBox, so it was not created without one.",
        NEEDS_A_PERSON,
        "Check that the parent interface synced (look for its own skip), then run "
        "the sync again.",
    ),
    "subinterface-name-taken-by-parentless-interface": (
        "NetBox already holds a top-level interface with the subinterface's dotted "
        "name, and the sync neither rewrites an existing interface nor creates a "
        "duplicate. Dotted names are legitimate on some platforms.",
        NEEDS_A_PERSON,
        "Usually nothing to do. If the subinterface's addresses are missing, send "
        "the support bundle's interface-state section to support. Do not set a "
        "parent by hand: the interface is a real top-level one.",
    ),
    "subinterface-name-taken-by-top-level-row": (
        "Forward lists a top-level interface with the same name in the same run, so "
        "the name is taken.",
        NEEDS_A_PERSON,
        "Nothing is lost: the top-level interface wins. If both should exist, the "
        "two names collide on the device itself.",
    ),
    "subinterface-parent-not-saved": (
        "The parent interface's save failed in this run, so the subinterface was "
        "removed rather than left without a parent.",
        NEEDS_A_PERSON,
        "Fix the parent interface's own failure first; the subinterface follows on "
        "the next sync.",
    ),
    "subinterface-parent-is-itself": (
        "Forward names the subinterface as its own parent.",
        NEEDS_A_PERSON,
        "This is a data problem on the device in Forward; send the support bundle to "
        "support.",
    ),
    "subinterface-parent-is-a-subinterface": (
        "Forward names another subinterface as the parent, and NetBox allows only "
        "one level.",
        NEEDS_A_PERSON,
        "Nothing to fix in NetBox; the nested subinterface is not represented.",
    ),
    "dependency-unnamed": (
        "A row was skipped waiting on something the sync did not name.",
        NEEDS_A_PERSON,
        "Open the issues for this model; each row's message says what it waited on.",
    ),
}

GENERIC = (
    "The sync could not apply these rows: a rule or a missing dependency rejected "
    "them.",
    NEEDS_A_PERSON,
    "Open the issues for this model; each row's message names the rule.",
)


def explain_skip_reason(reason):
    """``(why, fixable, what to do)`` for a skip reason slug."""
    reason = str(reason or "")
    if reason in KNOWN:
        return KNOWN[reason]
    if reason.startswith("missing-"):
        what = reason[len("missing-") :].replace("-", " ")
        return (
            f"A row needs a {what} that is not in NetBox, usually because that "
            "object was skipped or not in scope.",
            NEEDS_A_PERSON,
            f"Fix the {what} first (look for its own skip); the next sync retries "
            "these rows.",
        )
    if reason.startswith("still-referenced-by-"):
        what = reason[len("still-referenced-by-") :].replace("-", " ")
        return (
            f"A cleanup could not delete the object because a {what} still uses it.",
            NEEDS_A_PERSON,
            f"Move or remove the {what} that uses it, then run the cleanup again.",
        )
    return GENERIC
