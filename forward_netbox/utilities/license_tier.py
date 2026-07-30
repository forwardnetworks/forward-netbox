# Recognise a Forward license-tier denial and say what it actually means.
#
# Forward's tiered licensing gates NQE in layers, and a denial arrives as a plain
# HTTP error whose body is one sentence:
#
#     Query <name> is not permitted for this organization's license tier
#
# That is accurate and almost useless to an operator: it names a query but not
# what the license is missing, and it surfaces mid-sync as a generic Forward API
# failure alongside timeouts and auth errors. This module turns it into a message
# that names the capability at issue.
#
# **The tier is not readable from the API.** There is no endpoint exposing it, so
# the plugin cannot pre-flight a sync against the license or warn before running.
# Recognising the denial after the fact is the only signal available; do not add a
# "check the tier first" gate on the assumption one exists.
#
# Only two facts are asserted here, both read from Forward's own policy code
# rather than inferred from behaviour:
#
# * Org-authored queries are permitted only for tiers carrying the NETWORK facet.
#   The policy short-circuits on repository type, so for a tier without it *every*
#   org query is denied regardless of its content. This plugin's maps default to
#   the org repository, which makes NETWORK a floor for the plugin as a whole.
# * Queries reading CVE, CIS or STIG data additionally require the SECURITY facet.
#
# Deliberately not encoded: a per-map facet table. Mapping each bundled query to a
# minimum tier requires knowing which schema paths it reads, and a survey based on
# matching path names in query text over-reports badly. A wrong table would send
# operators to buy the wrong thing.
import re

# Forward raises this from `QueryIdentityPolicy.getAccessDeniedException`. The
# apostrophe varies with response encoding, so match around it.
LICENSE_TIER_DENIAL_RE = re.compile(
    r"\bis not permitted for this organization.{0,3}s license tier\b",
    re.IGNORECASE,
)
_QUERY_NAME_RE = re.compile(
    r"\bQuery\s+(?P<name>.+?)\s+is not permitted for this organization",
    re.IGNORECASE | re.DOTALL,
)

NETWORK_FACET_GUIDANCE = (
    "Forward permits organization-authored NQE queries only for license tiers "
    "that include the NETWORK facet (N, NP, NS or NSP). This plugin publishes "
    "its queries to the organization repository, so a Base (B) or "
    "Security-only (S) tier cannot run any of them."
)
SECURITY_FACET_GUIDANCE = (
    "Queries that read CVE, CIS or STIG data additionally require the SECURITY "
    "facet (S, NS or NSP)."
)


def is_license_tier_denial(message) -> bool:
    """True when `message` is Forward's license-tier refusal."""
    return bool(LICENSE_TIER_DENIAL_RE.search(str(message or "")))


def denied_query_name(message) -> str:
    """The query named in the denial, or "" when the phrasing does not carry one."""
    match = _QUERY_NAME_RE.search(str(message or ""))
    if match is None:
        return ""
    return match.group("name").strip().strip("`\"'")


def license_tier_denial_message(message) -> str:
    """An operator-actionable rendering of a license-tier denial.

    Keeps Forward's own sentence — it is the authoritative statement and names
    the query — then explains which capability the license is missing.
    """
    name = denied_query_name(message)
    subject = (
        f"Forward denied NQE query `{name}`" if name else "Forward denied an NQE query"
    )
    return (
        f"{subject} because of this organization's license tier. "
        f"{NETWORK_FACET_GUIDANCE} {SECURITY_FACET_GUIDANCE} "
        "Forward does not expose the license tier over its API, so this can only "
        "be reported when a query is refused, not checked in advance."
    )
