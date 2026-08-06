from django.core.exceptions import ValidationError
from django.utils.text import slugify


RESERVED_STATUS_TAG_SLUGS = frozenset(
    {
        "forward-backfilled",
        "forward-out-of-scope",
    }
)


def normalized_managed_tag_slug(value):
    """The slug a managed tag is CREATED with. One name, one slug."""
    name = str(value or "").strip()
    return slugify(name) or slugify(name.replace(".", "-"))


def candidate_managed_tag_slugs(value):
    """Every slug a tag for this name may already be stored under.

    `slugify` drops a dot rather than replacing it, so `A.Person` normalizes to
    `aperson` and the `.`-to-`-` arm of `normalized_managed_tag_slug` is
    unreachable for any name that survives slugify at all - it only ever fires
    for names with no ASCII word characters. A tag created through the NetBox
    UI, an import, or a different convention can therefore hold `a-person`
    while this module computes `aperson`, and a lookup by the computed slug
    alone will not find it.

    Creation still uses exactly one slug. This is for RESOLUTION, where missing
    an existing row is what turns a benign collision into a refusal.
    """
    name = str(value or "").strip()
    return {slug for slug in (slugify(name), slugify(name.replace(".", "-"))) if slug}


def validate_scope_tag_names(values):
    normalized = {}
    for value in values:
        name = str(value or "").strip()
        slug = normalized_managed_tag_slug(name)
        if not slug:
            raise ValidationError(
                "Forward scope tag names must contain a letter or number."
            )
        if slug in RESERVED_STATUS_TAG_SLUGS:
            raise ValidationError(
                f"Forward scope tag `{name}` normalizes to reserved status tag "
                f"slug `{slug}`. Choose a different include tag."
            )
        normalized[name] = slug
    return normalized
