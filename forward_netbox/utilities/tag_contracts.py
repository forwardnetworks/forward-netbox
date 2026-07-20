from django.core.exceptions import ValidationError
from django.utils.text import slugify


RESERVED_STATUS_TAG_SLUGS = frozenset(
    {
        "forward-backfilled",
        "forward-out-of-scope",
    }
)


def normalized_managed_tag_slug(value):
    name = str(value or "").strip()
    return slugify(name) or slugify(name.replace(".", "-"))


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
