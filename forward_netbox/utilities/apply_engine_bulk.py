from typing import Any


def bulk_orm_apply_simple_models(runner, model_string: str, rows: list[dict[str, Any]]):
    from django.db import transaction
    from django.db.models import Q

    from dcim.models import DeviceType
    from dcim.models import DeviceRole
    from dcim.models import Manufacturer
    from dcim.models import Platform
    from dcim.models import Site
    from ipam.models import VLAN
    from ipam.models import VRF

    specs = {
        "dcim.site": {
            "model": Site,
            "required": ("name", "slug"),
            "fields": ("name", "slug"),
            "lookup_fields": ("slug", "name"),
            "lookup_sets": (("slug",), ("name",)),
        },
        "dcim.manufacturer": {
            "model": Manufacturer,
            "required": ("name", "slug"),
            "fields": ("name", "slug"),
            "lookup_fields": ("slug", "name"),
            "lookup_sets": (("slug",), ("name",)),
        },
        "dcim.devicerole": {
            "model": DeviceRole,
            "required": ("name", "slug"),
            "fields": ("name", "slug", "color"),
            "lookup_fields": ("slug", "name"),
            "lookup_sets": (("slug",), ("name",)),
        },
        "dcim.platform": {
            "model": Platform,
            "required": ("name", "slug"),
            "fields": ("name", "slug", "manufacturer"),
            "lookup_fields": ("slug", "name"),
            "lookup_sets": (("slug",), ("name",)),
        },
        "dcim.devicetype": {
            "model": DeviceType,
            "required": ("manufacturer", "model", "slug"),
            "fields": ("manufacturer", "model", "slug"),
            "lookup_fields": ("slug", "model"),
            "lookup_sets": (("slug",), ("manufacturer", "model")),
        },
        "ipam.vlan": {
            "model": VLAN,
            "required": ("vid", "name", "status"),
            "fields": ("site", "vid", "name", "status"),
            "lookup_fields": ("site", "vid"),
            "lookup_sets": (("site", "vid"),),
        },
        "ipam.vrf": {
            "model": VRF,
            "required": ("name",),
            "fields": ("name", "rd", "description", "enforce_unique"),
            "lookup_fields": ("rd", "name"),
            "lookup_sets": (("rd",), ("name",)),
        },
    }
    spec = specs.get(model_string)
    if not spec:
        return False

    model = spec["model"]
    fields = tuple(spec["fields"])
    required = tuple(spec["required"])
    lookup_fields = tuple(spec["lookup_fields"])
    lookup_sets = tuple(tuple(lookup_set) for lookup_set in spec["lookup_sets"])

    site_by_slug = {}
    site_by_name = {}
    manufacturer_by_slug = {}
    manufacturer_by_name = {}
    if model_string == "ipam.vlan":
        site_rows = [
            {"name": row.get("site"), "slug": row.get("site_slug") or row.get("site")}
            for row in rows
            if row.get("site")
        ]
        bulk_orm_apply_simple_models(runner, "dcim.site", site_rows)
        site_values = {
            value
            for row in site_rows
            for value in (row.get("slug"), row.get("name"))
            if value not in ("", None)
        }
        sites = Site.objects.filter(Q(slug__in=site_values) | Q(name__in=site_values))
        site_by_slug = {site.slug: site for site in sites if site.slug}
        site_by_name = {site.name: site for site in sites if site.name}
    if model_string == "dcim.devicetype":
        manufacturer_rows = [
            {
                "name": row.get("manufacturer"),
                "slug": row.get("manufacturer_slug") or row.get("manufacturer"),
            }
            for row in rows
            if row.get("manufacturer")
        ]
        bulk_orm_apply_simple_models(runner, "dcim.manufacturer", manufacturer_rows)
        manufacturer_values = {
            value
            for row in manufacturer_rows
            for value in (row.get("slug"), row.get("name"))
            if value not in ("", None)
        }
        manufacturers = Manufacturer.objects.filter(
            Q(slug__in=manufacturer_values) | Q(name__in=manufacturer_values)
        )
        manufacturer_by_slug = {
            manufacturer.slug: manufacturer
            for manufacturer in manufacturers
            if manufacturer.slug
        }
        manufacturer_by_name = {
            manufacturer.name: manufacturer
            for manufacturer in manufacturers
            if manufacturer.name
        }
    if model_string == "dcim.platform":
        manufacturer_rows = [
            {
                "name": row.get("manufacturer"),
                "slug": row.get("manufacturer_slug") or row.get("manufacturer"),
            }
            for row in rows
            if row.get("manufacturer")
        ]
        bulk_orm_apply_simple_models(runner, "dcim.manufacturer", manufacturer_rows)
        manufacturer_values = {
            value
            for row in manufacturer_rows
            for value in (row.get("slug"), row.get("name"))
            if value not in ("", None)
        }
        manufacturers = Manufacturer.objects.filter(
            Q(slug__in=manufacturer_values) | Q(name__in=manufacturer_values)
        )
        manufacturer_by_slug = {
            manufacturer.slug: manufacturer
            for manufacturer in manufacturers
            if manufacturer.slug
        }
        manufacturer_by_name = {
            manufacturer.name: manufacturer
            for manufacturer in manufacturers
            if manufacturer.name
        }

    lookup_values = {field_name: [] for field_name in lookup_fields}
    normalized_rows: list[dict[str, Any]] = []
    for row in rows:
        if any(not row.get(field_name) for field_name in required):
            runner._record_issue(
                model_string,
                "Bulk ORM row missing required identity fields.",
                row,
                context={"required": required},
            )
            runner.logger.increment_statistics(model_string, outcome="failed")
            continue
        normalized = {field_name: row.get(field_name) for field_name in fields}
        if model_string == "dcim.devicerole" and not normalized.get("color"):
            normalized["color"] = "9e9e9e"
        if model_string == "dcim.platform":
            manufacturer = None
            if row.get("manufacturer"):
                manufacturer = manufacturer_by_slug.get(
                    row.get("manufacturer_slug")
                ) or manufacturer_by_name.get(row.get("manufacturer"))
            normalized["manufacturer"] = manufacturer
        if model_string == "dcim.devicetype":
            manufacturer = None
            if row.get("manufacturer"):
                manufacturer = manufacturer_by_slug.get(
                    row.get("manufacturer_slug")
                ) or manufacturer_by_name.get(row.get("manufacturer"))
            normalized["manufacturer"] = manufacturer
            if not normalized.get("slug"):
                normalized["slug"] = normalized.get("model")
        if model_string == "ipam.vlan":
            site = None
            if row.get("site"):
                site = site_by_slug.get(row.get("site_slug")) or site_by_name.get(
                    row.get("site")
                )
            normalized["site"] = site
            normalized["vid"] = int(normalized["vid"])
        if model_string == "ipam.vrf":
            normalized["rd"] = normalized.get("rd") or None
            normalized["description"] = normalized.get("description") or ""
            normalized["enforce_unique"] = bool(normalized.get("enforce_unique"))
        normalized_rows.append(normalized)
        for field_name in lookup_fields:
            value = normalized.get(field_name)
            if value not in ("", None):
                lookup_values[field_name].append(value)

    if not normalized_rows:
        return True

    if model_string in {"dcim.devicerole", "dcim.platform"}:
        return bulk_orm_apply_tree_models(
            runner=runner,
            model_string=model_string,
            model=model,
            fields=fields,
            lookup_sets=lookup_sets,
            normalized_rows=normalized_rows,
        )

    existing_qs = model.objects.none()
    if any(lookup_values.values()):
        query = Q()
        for field_name, values in lookup_values.items():
            if values:
                query |= Q(**{f"{field_name}__in": values})
        existing_qs = model.objects.filter(query)

    existing_by_lookup = {lookup_set: {} for lookup_set in lookup_sets}
    for obj in existing_qs:
        for lookup_set in lookup_sets:
            key = lookup_key_from_object(obj, lookup_set)
            if key is not None:
                existing_by_lookup[lookup_set][key] = obj

    create_objects = []
    update_objects = []
    for values in normalized_rows:
        existing = None
        for lookup_set in lookup_sets:
            key = lookup_key_from_values(values, lookup_set)
            if key is None:
                continue
            existing = existing_by_lookup[lookup_set].get(key)
            if existing is not None:
                break
        if existing is None:
            obj = model(**values)
            obj.full_clean()
            create_objects.append(obj)
            for lookup_set in lookup_sets:
                key = lookup_key_from_values(values, lookup_set)
                if key is not None:
                    existing_by_lookup[lookup_set][key] = obj
            runner.logger.increment_statistics(model_string, outcome="applied")
            runner.events_clearer.increment()
            continue
        changed = False
        for field_name in fields:
            incoming = values.get(field_name)
            if getattr(existing, field_name) != incoming:
                setattr(existing, field_name, incoming)
                changed = True
        if changed and getattr(existing, "pk", None) is not None:
            existing.full_clean()
            update_objects.append(existing)
        runner.logger.increment_statistics(model_string, outcome="applied")
        runner.events_clearer.increment()

    with transaction.atomic():
        if create_objects:
            model.objects.bulk_create(create_objects, batch_size=1000)
        if update_objects:
            model.objects.bulk_update(
                update_objects,
                fields=list(fields),
                batch_size=1000,
            )
    runner.events_clearer.clear()
    return True


def lookup_key_from_object(obj, lookup_set):
    values = []
    for field_name in lookup_set:
        value = getattr(obj, field_name, None)
        values.append(lookup_key_value(value))
    if any(value in ("", None) for value in values):
        return None
    return "|".join(str(value) for value in values)


def lookup_key_from_values(values, lookup_set):
    parts = [lookup_key_value(values.get(field_name)) for field_name in lookup_set]
    if any(value in ("", None) for value in parts):
        return None
    return "|".join(str(value) for value in parts)


def lookup_key_value(value):
    if hasattr(value, "pk"):
        return value.pk
    return value


def bulk_orm_apply_tree_models(
    *,
    runner,
    model_string: str,
    model,
    fields: tuple[str, ...],
    lookup_sets: tuple[tuple[str, ...], ...],
    normalized_rows: list[dict[str, Any]],
):
    from django.db import transaction

    with transaction.atomic():
        for values in normalized_rows:
            existing = None
            for lookup_set in lookup_sets:
                lookup = {
                    field_name: values.get(field_name) for field_name in lookup_set
                }
                if any(value in ("", None) for value in lookup.values()):
                    continue
                existing = model.objects.filter(**lookup).order_by("pk").first()
                if existing is not None:
                    break
            if existing is None:
                obj = model(**values)
                obj.full_clean()
                obj.save()
                runner.logger.increment_statistics(model_string, outcome="applied")
                runner.events_clearer.increment()
                continue

            changed = False
            for field_name in fields:
                incoming = values.get(field_name)
                if getattr(existing, field_name) != incoming:
                    setattr(existing, field_name, incoming)
                    changed = True
            if changed:
                existing.full_clean()
                existing.save()
            runner.logger.increment_statistics(model_string, outcome="applied")
            runner.events_clearer.increment()
    runner.events_clearer.clear()
    return True
