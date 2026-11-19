# The NetBox site a Forward device belongs to, and the one rule that decides it.
#
# Forward knows a device by its location. NetBox sites are usually coarser: many
# Forward locations can make up one data centre. A device tagged
# `NetBox_Site_<name>` is placed at the site `<name>`, whatever its location; an
# untagged device keeps its location. Site names are lowercased like every site
# name the sync creates.
#
# The shipped NQE maps get this from the shared `deviceSiteName` helper in
# `queries/netbox_utilities.nqe`. A few ad-hoc queries the plugin builds in Python
# cannot import that module, so they embed the same rule from here, and
# `test_site_tag_override` keeps the two spellings in step. `site_name_for` is the
# plain-Python statement of the rule, used as the reference in tests and docs.
import re
from pathlib import Path

SITE_TAG_PREFIX = "NetBox_Site_"


def site_name_for(tag_names, location_name, *, fallback="unknown"):
    """The lowercased site name for a device's tags and location name.

    More than one site tag takes the first alphabetically, so the answer never
    depends on tag order. A tag with nothing after the prefix is not a site tag.
    """
    tagged = sorted(
        str(tag)[len(SITE_TAG_PREFIX) :]
        for tag in tag_names or ()
        if str(tag).startswith(SITE_TAG_PREFIX) and len(str(tag)) > len(SITE_TAG_PREFIX)
    )
    if tagged:
        return tagged[0].lower()
    location = str(location_name or "")
    return location.lower() if location else fallback


def site_tag_nqe_let(variable="device", name="site_tag"):
    """The NQE `let` line that finds a device's site tag (null when it has none)."""
    return (
        f"let {name} = min(foreach tag in {variable}.tagNames "
        f'where matches(tag, "{SITE_TAG_PREFIX}*") '
        f'where length(replace(tag, "{SITE_TAG_PREFIX}", "")) > 0 '
        f'select replace(tag, "{SITE_TAG_PREFIX}", ""))'
    )


def site_name_nqe(variable="device", name="site_tag", fallback='"unknown"'):
    """The NQE expression for the site name, after `site_tag_nqe_let`."""
    return (
        f"if isPresent({name}) then toLowerCase({name}) "
        f"else if isPresent({variable}.locationName) "
        f"&& length({variable}.locationName) > 0 "
        f"then toLowerCase({variable}.locationName) else {fallback}"
    )


# --- Site mapping rules -------------------------------------------------------
#
# An operator's list of "devices at this Forward location, or with this Forward
# tag, go to this NetBox site". By default a device's site is its Forward
# location; a rule is what changes that. One rule is {kind, pattern, site}:
#
#   kind     "location" (pattern is a glob as NQE `matches` evaluates it, matched
#            case-insensitively) or "tag" (pattern is an exact Forward tag name)
#   pattern  the glob, or the tag name
#   site     the NetBox site name (lowercased like every site name the sync makes)
#
# The list is stored on the Forward source. The queries receive it as a record
# list with a `rank` that is the rule's position, derived when it is sent and never
# stored, so two rules can never tie. See docs/02_Reference/site-tag-override.md.

SITE_RULE_KINDS = ("location", "tag")
SITE_RULE_KEYS = frozenset({"kind", "pattern", "site"})
MAX_SITE_RULES = 100
SITE_RULE_ARROW = " -> "


class SiteRuleError(ValueError):
    """A site rule list the operator cannot save; the message says which rule."""


def site_slug(value):
    """The slug the shared NQE `slugify` makes: lowercase, `&` as "and", every run of
    characters outside a-z and 0-9 as one dash, dashes at the ends trimmed."""
    text = str(value or "").lower().replace("&", " and ")
    return re.sub(r"[^a-z0-9]+", "-", text).strip("-")


def normalize_site_rules(raw):
    """Validate a stored or submitted rule list and return it cleaned, in order."""
    if raw is None or raw == []:
        return []
    if not isinstance(raw, (list, tuple)):
        raise SiteRuleError("Site rules must be a list of rules.")
    if len(raw) > MAX_SITE_RULES:
        raise SiteRuleError(f"At most {MAX_SITE_RULES} site rules are allowed.")
    rules = []
    seen = {}
    slugs = {}
    for number, item in enumerate(raw, start=1):
        label = f"Rule {number}"
        if not isinstance(item, dict):
            raise SiteRuleError(
                f"{label} must be an object with kind, pattern and site."
            )
        if set(item) != SITE_RULE_KEYS:
            raise SiteRuleError(
                f"{label} must have exactly the keys kind, pattern and site."
            )
        kind = str(item["kind"] or "").strip()
        pattern = str(item["pattern"] or "").strip()
        site = str(item["site"] or "").strip()
        if kind not in SITE_RULE_KINDS:
            raise SiteRuleError(f"{label}: the kind must be 'location' or 'tag'.")
        if not pattern:
            raise SiteRuleError(f"{label}: the {kind} is blank.")
        if not site:
            raise SiteRuleError(f"{label}: the NetBox site is blank.")
        if kind == "location" and pattern == "*":
            raise SiteRuleError(
                f"{label}: a location of just '*' would put every device in one site."
            )
        slug = site_slug(site)
        if not slug:
            raise SiteRuleError(
                f"{label}: '{site}' has no letters or digits to make a site slug from."
            )
        key = (kind, pattern.lower() if kind == "location" else pattern)
        if key in seen:
            raise SiteRuleError(
                f"{label} repeats rule {seen[key]}: the same {kind} is matched twice."
            )
        seen[key] = number
        previous = slugs.setdefault(slug, site.lower())
        if previous != site.lower():
            raise SiteRuleError(
                f"{label}: '{site}' and '{previous}' make the same site slug '{slug}'."
            )
        rules.append({"kind": kind, "pattern": pattern, "site": site})
    return rules


def query_site_rules(rules):
    """The rule list as the queries receive it: records with `rank` set to the
    position, the site lowercased, and a location pattern lowercased (NQE matches
    the lowercased location). A tag is left exact."""
    sent = []
    for rank, item in enumerate(rules or [], start=1):
        kind = item["kind"]
        pattern = item["pattern"]
        sent.append(
            {
                "kind": kind,
                "pattern": pattern.lower() if kind == "location" else pattern,
                "site": item["site"].lower(),
                "rank": rank,
            }
        )
    return sent


def format_site_rules(rules):
    """One rule per line: `<kind> <pattern> -> <site>`."""
    return "\n".join(
        f"{item['kind']} {item['pattern']}{SITE_RULE_ARROW}{item['site']}"
        for item in rules or []
    )


def parse_site_rules_text(text):
    """The inverse of `format_site_rules`. A pattern may contain spaces (Forward
    location names do), so the kind is the first word and the arrow splits the
    rest."""
    rules = []
    for number, line in enumerate(str(text or "").splitlines(), start=1):
        line = line.strip()
        if not line:
            continue
        kind, _, rest = line.partition(" ")
        pattern, arrow, site = rest.partition(SITE_RULE_ARROW)
        if not (kind and arrow and pattern.strip() and site.strip()):
            raise SiteRuleError(
                f"Line {number}: write '<location|tag> <pattern> -> <site>'."
            )
        rules.append({"kind": kind, "pattern": pattern.strip(), "site": site.strip()})
    return rules


SITE_RULES_NQE_TYPE = (
    "List<{ kind: String, pattern: String, site: String, rank: Number }>"
)
_HELPERS_BEGIN = "// BEGIN site naming helpers"
_HELPERS_END = "// END site naming helpers"


def site_helper_nqe():
    """The site naming helpers of `queries/netbox_utilities.nqe`, for a query built
    in Python (which cannot import the module): the exact block between its markers,
    with `export` removed. Read from the file so the two cannot drift apart."""
    source = (
        Path(__file__).resolve().parents[1] / "queries" / "netbox_utilities.nqe"
    ).read_text(encoding="utf-8")
    block = source[source.index(_HELPERS_BEGIN) : source.index(_HELPERS_END)]
    return block.split("\n", 1)[1].replace("export ", "")


def with_site_rules(query):
    """A bare inline query wrapped so it can call the site helpers and receive the
    rules as the declared `site_rules` parameter. The rules are sent as a parameter,
    never written into the text."""
    return (
        f"{site_helper_nqe().rstrip()}\n\n@query\n"
        f"f(site_rules: {SITE_RULES_NQE_TYPE}) =\n{query.rstrip()};\n"
    )
