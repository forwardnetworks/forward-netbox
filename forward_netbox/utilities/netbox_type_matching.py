# Reuse the device and module types NetBox already holds.
#
# The sync matched a type only on an exact manufacturer plus model or slug, so a
# library type written a little differently (`CM7148-2-DAC` where Forward says
# `Opengear CM7148-2-DAC`) was never found and a duplicate was created. When the
# difference is only punctuation, case, a leading vendor name or a part number, and
# exactly one existing type fits, the sync now uses that type.
#
# Two rules keep it safe:
#
# * A matched type is never written. A device type's own row is dropped from the
#   fetch (the type exists, so there is nothing to apply), so no value can be
#   overwritten and no stored identity for it can later turn into a delete. Device,
#   module and hardware-notice rows are pointed at the existing type's own values.
# * A match must be unique and deterministic. Two candidates, or none, leaves the
#   row as it was and the sync creates a type as before.
import re
from collections import defaultdict

# model string -> (kind, model field, slug field, part number field)
ROW_FIELDS = {
    "dcim.devicetype": ("device", "model", "slug", "part_number"),
    "dcim.device": ("device", "device_type", "device_type_slug", "part_number"),
    "netbox_dlm.hardwarenotice": ("device", "device_type", "device_type_slug", None),
    "dcim.module": ("module", "model", None, "part_number"),
}

# A name this short says too little to equate two types on.
MIN_KEY_LENGTH = 3

# The module query truncates a part number to this many characters (and falls back to
# the component description when there is no part id), so a value this long is a
# truncated description, not a part number.
TRUNCATED_PART_LENGTH = 50

# Values some platforms report for "no part number".
PLACEHOLDER_PARTS = frozenset({"na", "none", "null", "unknown", "notapplicable", "tbd"})


def usable_part(value):
    """Whether ``value`` can identify a type as a part number."""
    text = str(value or "").strip()
    key = normalize(text)
    return (
        len(key) >= MIN_KEY_LENGTH
        and len(text) < TRUNCATED_PART_LENGTH
        and key not in PLACEHOLDER_PARTS
    )


def normalize(value):
    """Lowercase letters and digits only: what punctuation and case never change."""
    return re.sub(r"[^a-z0-9]", "", str(value or "").lower())


def _strip_prefix(key, vendor):
    """``key`` without a leading ``vendor``, or ``None`` if that leaves too little."""
    if vendor and key.startswith(vendor) and len(key) - len(vendor) >= MIN_KEY_LENGTH:
        return key[len(vendor) :]
    return None


class TypeIndex:
    """The existing types of one kind, looked up by exact value, part number or name."""

    def __init__(self, entries):
        # entries: dicts with pk, manufacturer_slug, manufacturer_name, model, slug,
        # part_number (and, for modules, description and comments)
        self.entries = list(entries)
        self.exact_model = set()
        self.exact_slug = set()
        self.by_part = defaultdict(list)
        self.by_name = defaultdict(list)
        self.vendors = set()
        for entry in self.entries:
            slug = entry["manufacturer_slug"]
            vendor_keys = {
                key
                for key in (normalize(entry["manufacturer_name"]), normalize(slug))
                if key
            }
            entry["_vendors"] = vendor_keys
            self.vendors |= vendor_keys
            for scope in (slug, None):
                self.exact_model.add((scope, entry["model"]))
                if entry["slug"]:
                    self.exact_slug.add((scope, entry["slug"]))
                raw_part = str(entry["part_number"] or "").strip()
                if usable_part(raw_part):
                    self.by_part[(scope, raw_part.casefold())].append(entry)
            # A type is findable by its own name, and by its name without its own
            # manufacturer's name in front of it.
            key = normalize(entry["model"])
            variants = {key}
            for vendor in vendor_keys:
                stripped = _strip_prefix(key, vendor)
                if stripped:
                    variants.add(stripped)
            for variant in variants:
                if len(variant) >= MIN_KEY_LENGTH and variant not in PLACEHOLDER_PARTS:
                    for scope in (slug, None):
                        self.by_name[(scope, variant)].append(entry)

    def resolve(self, *, manufacturer_slug, model, slug="", part_number=""):
        """The one existing type this row means, or ``None`` to leave the row as is.

        ``None`` covers three cases that all mean "do what the sync did before": the
        type exists exactly, nothing fits, or more than one fits.
        """
        model = str(model or "").strip()
        scope = manufacturer_slug or None
        if not model:
            return None
        if (scope, model) in self.exact_model or (
            slug and (scope, slug) in self.exact_slug
        ):
            return None
        wanted_parts = {
            str(value).strip().casefold()
            for value in (model, part_number)
            if usable_part(value)
        }
        by_part = {}
        for part in wanted_parts:
            for entry in self.by_part.get((scope, part), ()):
                by_part[entry["pk"]] = entry
        if len(by_part) == 1:
            return next(iter(by_part.values()))
        if len(by_part) > 1:
            return None
        # Row names to look for: the name as written, and the name without a vendor
        # in front of it. A stripped name may only match a type of THAT vendor, so a
        # model written "Dell 3048" never lands on another vendor's "3048".
        key = normalize(model)
        candidates = {}
        if len(key) >= MIN_KEY_LENGTH and key not in PLACEHOLDER_PARTS:
            for entry in self.by_name.get((scope, key), ()):
                candidates[entry["pk"]] = entry
        for vendor in self.vendors:
            stripped = _strip_prefix(key, vendor)
            if not stripped:
                continue
            for entry in self.by_name.get((scope, stripped), ()):
                if vendor in entry["_vendors"]:
                    candidates[entry["pk"]] = entry
        return next(iter(candidates.values())) if len(candidates) == 1 else None


def _load_index(kind):
    from dcim.models import DeviceType
    from dcim.models.modules import ModuleType

    if kind == "device":
        rows = DeviceType.objects.values_list(
            "pk",
            "manufacturer__slug",
            "manufacturer__name",
            "model",
            "slug",
            "part_number",
        )
        return TypeIndex(
            {
                "pk": pk,
                "manufacturer_slug": m_slug,
                "manufacturer_name": m_name,
                "model": model,
                "slug": slug,
                "part_number": part_number or "",
            }
            for pk, m_slug, m_name, model, slug, part_number in rows
        )
    rows = ModuleType.objects.values_list(
        "pk",
        "manufacturer__slug",
        "manufacturer__name",
        "model",
        "part_number",
        "description",
        "comments",
    )
    return TypeIndex(
        {
            "pk": pk,
            "manufacturer_slug": m_slug,
            "manufacturer_name": m_name,
            "model": model,
            "slug": "",
            "part_number": part_number or "",
            "description": description or "",
            "comments": comments or "",
        }
        for pk, m_slug, m_name, model, part_number, description, comments in rows
    )


def match_existing_types(model_string, rows, *, logger=None, obj=None):
    """``rows`` with their types pointed at existing NetBox types where one fits.

    Returns the same list when nothing matches (or the model names no type);
    otherwise a new list. Never mutates its input. A device type's own row is dropped
    when its type exists, so a matched type is never written.
    """
    kept, _dropped = match_existing_types_split(
        model_string, rows, logger=logger, obj=obj
    )
    return kept


def match_existing_types_split(model_string, rows, *, logger=None, obj=None):
    """``(kept rows, dropped rows)``: the dropped ones are device-type rows whose type
    NetBox already holds. The caller can count them, but must never feed them to a
    delete.
    """
    spec = ROW_FIELDS.get(model_string)
    if spec is None or not rows:
        return rows, []
    kind, model_field, slug_field, part_field = spec
    index = _load_index(kind)
    if not index.entries:
        return rows, []
    dropped = []
    out = []
    matched = 0
    resolved = {}
    for row in rows:
        model = row.get(model_field)
        slug = row.get(slug_field) if slug_field else ""
        part = (row.get(part_field) if part_field else "") or ""
        key = (row.get("manufacturer_slug") or "", model, slug or "", part)
        if key not in resolved:
            resolved[key] = index.resolve(
                manufacturer_slug=key[0], model=model, slug=key[2], part_number=part
            )
        entry = resolved[key]
        if entry is None:
            out.append(row)
            continue
        matched += 1
        if model_string == "dcim.devicetype":
            # The type exists: nothing to apply, and nothing of it to overwrite.
            dropped.append(row)
            continue
        changed = dict(row)
        changed[model_field] = entry["model"]
        if slug_field:
            changed[slug_field] = entry["slug"]
        if part_field and part_field in changed:
            # The existing type keeps its own part number; Forward's would
            # overwrite it.
            changed[part_field] = entry["part_number"]
        if kind == "module":
            # The module apply writes these onto the type as well.
            # Always, not only when the row has them: the shipped module query emits
            # no comments, and the apply would write a blank over the type's own.
            changed["description"] = entry["description"]
            changed["comments"] = entry["comments"]
        out.append(changed)
    if matched and logger is not None:
        names = len({k for k, v in resolved.items() if v is not None})
        logger.log_info(
            f"Matched {matched} {model_string} row(s) ({names} Forward model "
            "name(s)) to existing NetBox types instead of creating new ones.",
            obj=obj,
        )
    return (out if matched else rows), dropped
