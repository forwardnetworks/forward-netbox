from ipaddress import ip_interface

from django.db import IntegrityError
from django.db.models import Q
from django.db.models.deletion import ProtectedError

from ..exceptions import ForwardDependencySkipError
from ..exceptions import ForwardSearchError


DEPENDENCY_LOOKUP_PAIR_CHUNK_SIZE = 500
UNIQUE_LOOKUP_CACHE_FIELD_SETS = {
    "dcim.devicerole": (("slug",), ("name",)),
    "dcim.devicetype": (("slug",), ("manufacturer", "model")),
    "dcim.inventoryitemrole": (("slug",), ("name",)),
    "dcim.manufacturer": (("slug",), ("name",)),
    "dcim.moduletype": (("manufacturer", "model"),),
    "dcim.platform": (("slug",), ("name",)),
    "dcim.site": (("slug",), ("name",)),
    "extras.tag": (("slug",), ("name",)),
    "ipam.ipaddress": (
        ("address", "vrf"),
        ("address__net_host", "vrf__isnull"),
    ),
    "ipam.vlan": (("site", "vid"),),
    "ipam.prefix": (("prefix", "vrf"),),
    "netbox_peering_manager.peeringsession": (("bgp_peer",),),
    "netbox_peering_manager.relationship": (("slug",), ("name",)),
    "netbox_routing.bgpaddressfamily": (("scope", "address_family"),),
    "netbox_routing.bgprouter": (
        ("assigned_object_type", "assigned_object_id", "asn"),
    ),
    "netbox_routing.bgpscope": (("router", "vrf"),),
    "netbox_routing.bgppeer": (("scope", "peer"),),
    "netbox_routing.bgppeeraddressfamily": (
        ("assigned_object_type", "assigned_object_id", "address_family"),
    ),
    "netbox_routing.ospfarea": (("area_id",),),
    "netbox_routing.ospfinstance": (("device", "vrf", "process_id"),),
    "netbox_routing.ospfinterface": (("interface",),),
}


def update_existing_or_create(
    runner, model, *, lookup, defaults, fallback_lookups=None, conflict_policy="strict"
):
    return coalesce_update_or_create(
        runner,
        model,
        coalesce_lookups=[lookup, *(fallback_lookups or [])],
        create_values={**lookup, **defaults},
        update_values=defaults,
        conflict_policy=conflict_policy,
    )


def coalesce_update_or_create(
    runner,
    model,
    *,
    coalesce_lookups,
    create_values,
    update_values=None,
    conflict_policy="strict",
):
    lookups = [lookup for lookup in (coalesce_lookups or []) if lookup]
    if not lookups:
        raise ValueError("At least one coalesce lookup must be provided.")

    update_values = create_values if update_values is None else update_values

    obj = None
    for lookup in lookups:
        obj = get_unique_or_raise(runner, model, lookup)
        if obj is not None:
            break

    if obj is None:
        try:
            obj = model(**create_values)
            obj.full_clean()
            obj.save()
            remember_lookup_object(runner, obj)
            _remember_unique_lookups(runner, model, lookups, obj)
            return obj, True
        except IntegrityError:
            if conflict_policy != "reuse_on_unique_conflict":
                raise
            for retry_lookup in lookups:
                obj = get_unique_or_raise(runner, model, retry_lookup)
                if obj is not None:
                    break
            if obj is None:
                raise

    update_fields = []
    for field, value in update_values.items():
        if not _model_field_value_matches(model, obj, field, value):
            setattr(obj, field, value)
            update_fields.append(field)
    if update_fields:
        obj.full_clean()
        obj.save(update_fields=update_fields)
    remember_lookup_object(runner, obj)
    _remember_unique_lookups(runner, model, lookups, obj)
    return obj, False


def _model_field_value_matches(model, obj, field_name, value):
    field = model._meta.get_field(field_name)
    if getattr(field, "many_to_one", False) or getattr(field, "one_to_one", False):
        current_id = getattr(obj, field.attname)
        desired_id = getattr(value, "pk", value)
        return current_id == desired_id
    return getattr(obj, field_name) == value


def get_unique_or_raise(runner, model, lookup):
    cached_obj = _cached_unique_identity_object(runner, model, lookup)
    if cached_obj is not None:
        return cached_obj
    cache_key = _unique_lookup_cache_key(model, lookup)
    if cache_key is not None and cache_key in runner._primed_missing_unique_lookup_keys:
        return None
    matches = list(model.objects.filter(**lookup).order_by("pk")[:2])
    if not matches:
        return None
    if len(matches) > 1:
        raise ForwardSearchError(
            f"Ambiguous coalesce lookup for `{model._meta.label_lower}` with {lookup}.",
            model_string=model._meta.label_lower,
            context=lookup,
        )
    obj = matches[0]
    remember_lookup_object(runner, obj)
    _remember_unique_lookup(runner, model, lookup, obj)
    return obj


def _cached_unique_identity_object(runner, model, lookup):
    label = model._meta.label_lower
    if label == "dcim.device":
        return _cached_simple_identity_object(
            lookup,
            "name",
            runner._device_by_name_cache,
        )
    if label == "dcim.interface":
        return _cached_device_scoped_name_object(
            runner._interface_by_device_name_cache,
            lookup,
        )
    if label == "dcim.modulebay":
        return _cached_device_scoped_name_object(
            runner._module_bay_by_device_name_cache,
            lookup,
        )
    if label == "ipam.vrf":
        return _cached_simple_identity_object(
            lookup,
            "name",
            runner._vrf_by_name_cache,
        ) or _cached_simple_identity_object(
            lookup,
            "rd",
            runner._vrf_by_rd_cache,
        )
    if label == "extras.tag":
        return _cached_simple_identity_object(
            lookup,
            "slug",
            runner._tag_by_slug_cache,
        ) or _cached_simple_identity_object(
            lookup,
            "name",
            runner._tag_by_name_cache,
        )
    if label == "ipam.asn":
        return _cached_asn_identity_object(lookup, runner._asn_by_number_cache)
    cache_key = _unique_lookup_cache_key(model, lookup)
    if cache_key is not None:
        return runner._unique_lookup_cache.get(cache_key)
    return None


def _remember_unique_lookups(runner, model, lookups, obj):
    for lookup in lookups:
        _remember_unique_lookup(runner, model, lookup, obj)


def _remember_unique_lookup(runner, model, lookup, obj):
    cache_key = _unique_lookup_cache_key(model, lookup)
    if cache_key is not None:
        runner._unique_lookup_cache[cache_key] = obj
        runner._primed_missing_unique_lookup_keys.discard(cache_key)


def _unique_lookup_cache_key(model, lookup):
    label = model._meta.label_lower
    allowed_field_sets = UNIQUE_LOOKUP_CACHE_FIELD_SETS.get(label)
    if not allowed_field_sets:
        return None
    lookup_fields = set(lookup)
    for field_names in allowed_field_sets:
        if lookup_fields == set(field_names):
            values = tuple(
                _cache_lookup_value(lookup[field_name]) for field_name in field_names
            )
            if any(value == "" for value in values):
                return None
            return (label, field_names, values)
    return None


def _cache_lookup_value(value):
    if hasattr(value, "pk"):
        return value.pk
    return value


def _cached_asn_identity_object(lookup, cache):
    if set(lookup) != {"asn"}:
        return None
    try:
        asn_number = int(lookup.get("asn"))
    except (TypeError, ValueError):
        return None
    return cache.get(asn_number)


def _cached_simple_identity_object(lookup, field_name, cache):
    if set(lookup) != {field_name}:
        return None
    value = lookup.get(field_name)
    if value in ("", None):
        return None
    return cache.get(value)


def _cached_device_scoped_name_object(cache, lookup):
    name = lookup.get("name")
    device_id = lookup.get("device_id")
    if device_id is None and lookup.get("device") is not None:
        device_id = getattr(lookup["device"], "pk", None)
    if not device_id or not name:
        return None
    return cache.get((device_id, name))


def coalesce_lookup(row, *fields):
    return {
        field: row[field]
        for field in fields
        if field in row and row[field] not in ("", None)
    }


def coalesce_upsert(
    runner,
    model_string,
    model,
    *,
    coalesce_lookups,
    create_values,
    update_values=None,
):
    return coalesce_update_or_create(
        runner,
        model,
        coalesce_lookups=coalesce_lookups,
        create_values=create_values,
        update_values=update_values,
        conflict_policy=runner._conflict_policy(model_string),
    )


def coalesce_sets_for(runner, model_string, default_sets):
    configured = runner._model_coalesce_fields.get(model_string)
    if configured:
        return configured
    return [list(field_set) for field_set in default_sets]


def upsert_row(
    runner,
    model_string,
    model,
    *,
    row,
    coalesce_sets,
    create_values,
    update_values=None,
):
    lookups = _dedupe_lookups(
        [coalesce_lookup(row, *coalesce_set) for coalesce_set in coalesce_sets]
    )
    return coalesce_upsert(
        runner,
        model_string,
        model,
        coalesce_lookups=lookups,
        create_values=create_values,
        update_values=update_values,
    )


def upsert_row_from_defaults(
    runner, model_string, model, *, row, coalesce_sets, defaults
):
    return upsert_row(
        runner,
        model_string,
        model,
        row=row,
        coalesce_sets=coalesce_sets,
        create_values=defaults,
        update_values=defaults,
    )


def upsert_values_from_defaults(runner, model_string, model, *, values, coalesce_sets):
    lookups = _dedupe_lookups(
        [coalesce_lookup(values, *coalesce_set) for coalesce_set in coalesce_sets]
    )
    return coalesce_upsert(
        runner,
        model_string,
        model,
        coalesce_lookups=lookups,
        create_values=values,
        update_values=values,
    )


def delete_by_coalesce(runner, model, lookups):
    lookups = _dedupe_lookups([lookup for lookup in lookups or [] if lookup])
    if not lookups:
        return False
    for lookup in lookups:
        obj = get_unique_or_raise(runner, model, lookup)
        if obj is not None:
            try:
                obj.delete()
            except ProtectedError as exc:
                raise ForwardDependencySkipError(
                    f"Skipping delete for `{model._meta.label_lower}` due to protected dependencies: {exc}",
                    model_string=model._meta.label_lower,
                    context=lookup,
                ) from exc
            forget_lookup_object(runner, obj)
            return True
    return False


def optional_model(app_label, model_name, model_string):
    from django.apps import apps

    from ..exceptions import ForwardQueryError

    try:
        return apps.get_model(app_label, model_name)
    except LookupError as exc:
        raise ForwardQueryError(
            f"`{model_string}` sync requires the optional `{app_label}` NetBox "
            "plugin to be installed and migrated."
        ) from exc


def model_field_values(model, values):
    model_fields = {
        field.name
        for field in model._meta.fields
        if not getattr(field, "auto_created", False)
    }
    return {key: value for key, value in values.items() if key in model_fields}


def content_type_for(runner, model):
    from django.contrib.contenttypes.models import ContentType

    key = model._meta.label_lower
    if key not in runner._content_types:
        runner._content_types[key] = ContentType.objects.get_for_model(model)
    return runner._content_types[key]


def remember_lookup_object(runner, obj):
    if obj is None:
        return
    label = obj._meta.label_lower
    if label == "dcim.device" and getattr(obj, "name", None):
        runner._device_by_name_cache[obj.name] = obj
        runner._missing_device_by_name_cache.discard(obj.name)
    elif label == "dcim.interface" and getattr(obj, "name", None):
        key = (obj.device_id, obj.name)
        runner._interface_by_device_name_cache[key] = obj
        runner._missing_interface_by_device_name_cache.discard(key)
    elif label == "dcim.modulebay" and getattr(obj, "name", None):
        key = (obj.device_id, obj.name)
        runner._module_bay_by_device_name_cache[key] = obj
        runner._missing_module_bay_by_device_name_cache.discard(key)
    elif label == "ipam.vrf":
        if getattr(obj, "name", None):
            runner._vrf_by_name_cache[obj.name] = obj
        if getattr(obj, "rd", None):
            runner._vrf_by_rd_cache[obj.rd] = obj
    elif label == "extras.tag":
        if getattr(obj, "slug", None):
            runner._tag_by_slug_cache[obj.slug] = obj
        if getattr(obj, "name", None):
            runner._tag_by_name_cache[obj.name] = obj
    elif label == "ipam.asn" and getattr(obj, "asn", None):
        runner._asn_by_number_cache[obj.asn] = obj


def forget_lookup_object(runner, obj):
    if obj is None:
        return
    label = obj._meta.label_lower
    _forget_unique_lookup_object(runner, obj, label)
    if label == "dcim.device" and getattr(obj, "name", None):
        runner._device_by_name_cache.pop(obj.name, None)
        runner._missing_device_by_name_cache.add(obj.name)
    elif label == "dcim.interface" and getattr(obj, "name", None):
        key = (obj.device_id, obj.name)
        runner._interface_by_device_name_cache.pop(key, None)
        runner._missing_interface_by_device_name_cache.discard(key)
    elif label == "dcim.modulebay" and getattr(obj, "name", None):
        key = (obj.device_id, obj.name)
        runner._module_bay_by_device_name_cache.pop(key, None)
        runner._missing_module_bay_by_device_name_cache.add(key)
    elif label == "ipam.vrf":
        if getattr(obj, "name", None):
            runner._vrf_by_name_cache.pop(obj.name, None)
        if getattr(obj, "rd", None):
            runner._vrf_by_rd_cache.pop(obj.rd, None)
    elif label == "extras.tag":
        if getattr(obj, "slug", None):
            runner._tag_by_slug_cache.pop(obj.slug, None)
        if getattr(obj, "name", None):
            runner._tag_by_name_cache.pop(obj.name, None)
    elif label == "ipam.asn" and getattr(obj, "asn", None):
        runner._asn_by_number_cache.pop(obj.asn, None)


def _forget_unique_lookup_object(runner, obj, label):
    obj_pk = getattr(obj, "pk", None)
    if obj_pk is None:
        return
    for cache_key, cached_obj in list(runner._unique_lookup_cache.items()):
        if cache_key[0] == label and getattr(cached_obj, "pk", None) == obj_pk:
            runner._unique_lookup_cache.pop(cache_key, None)


def get_device_by_name(runner, device_name):
    from dcim.models import Device

    if device_name in runner._device_by_name_cache:
        return runner._device_by_name_cache[device_name]
    if device_name in runner._missing_device_by_name_cache:
        raise Device.DoesNotExist
    try:
        device = Device.objects.get(name=device_name)
    except Device.DoesNotExist:
        runner._missing_device_by_name_cache.add(device_name)
        raise
    remember_lookup_object(runner, device)
    return device


def lookup_device_by_name(runner, device_name):
    from dcim.models import Device

    if not device_name:
        return None
    if device_name in runner._device_by_name_cache:
        return runner._device_by_name_cache[device_name]
    device = Device.objects.filter(name=device_name).order_by("pk").first()
    if device is not None:
        remember_lookup_object(runner, device)
    return device


def lookup_interface(runner, device, interface_name):
    from dcim.models import Interface

    if device is None or not interface_name:
        return None
    key = (device.pk, interface_name)
    if key in runner._interface_by_device_name_cache:
        return runner._interface_by_device_name_cache[key]
    if key in runner._missing_interface_by_device_name_cache:
        return None
    interface = Interface.objects.filter(device=device, name=interface_name).first()
    if interface is not None:
        remember_lookup_object(runner, interface)
    else:
        runner._missing_interface_by_device_name_cache.add(key)
    return interface


def lookup_module_bay(runner, device, module_bay_name):
    if device is None or not module_bay_name:
        return None
    key = (device.pk, module_bay_name)
    if key in runner._module_bay_by_device_name_cache:
        return runner._module_bay_by_device_name_cache[key]
    if key in runner._missing_module_bay_by_device_name_cache:
        return None
    module_bay = device.modulebays.filter(name=module_bay_name).order_by("pk").first()
    if module_bay is not None:
        remember_lookup_object(runner, module_bay)
    else:
        runner._missing_module_bay_by_device_name_cache.add(key)
    return module_bay


def prime_dependency_lookup_caches(runner, model_string, rows):
    runner._primed_missing_unique_lookup_keys = set()
    _prime_dcim_dependency_identity_cache(runner, model_string, rows)
    device_names = _dependency_device_names(model_string, rows)
    if device_names:
        _prime_device_cache(runner, device_names)
    tag_rows = _dependency_tag_rows(model_string, rows)
    if tag_rows:
        _prime_tag_cache(runner, tag_rows)
    interface_pairs = _dependency_interface_pairs(model_string, rows)
    if interface_pairs:
        _prime_interface_cache(runner, interface_pairs)
    module_bay_pairs = _dependency_module_bay_pairs(model_string, rows)
    if module_bay_pairs:
        _prime_module_bay_cache(runner, module_bay_pairs)
    _prime_ipam_coalesce_identity_cache(runner, model_string, rows)


def _dependency_device_names(model_string, rows):
    fields_by_model = {
        "dcim.cable": ("device", "remote_device"),
        "dcim.device": ("name",),
        "dcim.interface": ("device",),
        "dcim.inventoryitem": ("device",),
        "dcim.macaddress": ("device",),
        "dcim.module": ("device",),
        "dcim.virtualchassis": ("device",),
        "extras.taggeditem": ("device",),
        "ipam.ipaddress": ("device",),
        "netbox_peering_manager.peeringsession": ("device",),
        "netbox_routing.bgpaddressfamily": ("device",),
        "netbox_routing.bgppeer": ("device",),
        "netbox_routing.bgppeeraddressfamily": ("device",),
        "netbox_routing.ospfinstance": ("device",),
        "netbox_routing.ospfinterface": ("device",),
    }.get(model_string, ())
    return {
        str(row.get(field)).strip()
        for row in rows
        for field in fields_by_model
        if row.get(field) not in ("", None)
    }


def _dependency_interface_pairs(model_string, rows):
    fields_by_model = {
        "dcim.cable": (("device", "interface"), ("remote_device", "remote_interface")),
        "dcim.interface": (("device", "name"),),
        "dcim.macaddress": (("device", "interface"),),
        "ipam.ipaddress": (("device", "interface"),),
        "netbox_routing.ospfinterface": (("device", "local_interface"),),
    }.get(model_string, ())
    return {
        (str(row.get(device_field)).strip(), str(row.get(interface_field)).strip())
        for row in rows
        for device_field, interface_field in fields_by_model
        if row.get(device_field) not in ("", None)
        and row.get(interface_field) not in ("", None)
    }


def _dependency_module_bay_pairs(model_string, rows):
    if model_string != "dcim.module":
        return set()
    return {
        (str(row.get("device")).strip(), str(row.get("module_bay")).strip())
        for row in rows
        if row.get("device") not in ("", None)
        and row.get("module_bay") not in ("", None)
    }


def _dependency_tag_rows(model_string, rows):
    if model_string != "extras.taggeditem":
        return set()
    return {
        (
            str(row.get("tag_slug")).strip(),
            str(row.get("tag")).strip(),
        )
        for row in rows
        if row.get("tag_slug") not in ("", None) or row.get("tag") not in ("", None)
    }


def _prime_dcim_dependency_identity_cache(runner, model_string, rows):
    if model_string == "dcim.device":
        _prime_dcim_device_identity_cache(runner, rows)
        return
    if model_string in {"dcim.inventoryitem", "dcim.module"}:
        _prime_dcim_inventory_module_identity_cache(runner, model_string, rows)


def _prime_dcim_device_identity_cache(runner, rows):
    from dcim.models import DeviceRole
    from dcim.models import Manufacturer
    from dcim.models import Platform
    from dcim.models import Site

    site_slugs, site_names = _slug_name_identity_inputs(rows, "site_slug", "site")
    _prime_slug_name_identity_cache(
        runner,
        Site,
        slugs=site_slugs,
        names=site_names,
    )

    manufacturer_slugs, manufacturer_names = _slug_name_identity_inputs(
        rows, "manufacturer_slug", "manufacturer"
    )
    _prime_slug_name_identity_cache(
        runner,
        Manufacturer,
        slugs=manufacturer_slugs,
        names=manufacturer_names,
    )

    role_slugs, role_names = _slug_name_identity_inputs(rows, "role_slug", "role")
    _prime_slug_name_identity_cache(
        runner,
        DeviceRole,
        slugs=role_slugs,
        names=role_names,
    )

    platform_slugs, platform_names = _slug_name_identity_inputs(
        rows, "platform_slug", "platform"
    )
    _prime_slug_name_identity_cache(
        runner,
        Platform,
        slugs=platform_slugs,
        names=platform_names,
    )

    _prime_device_type_identity_cache(runner, rows)


def _prime_dcim_inventory_module_identity_cache(runner, model_string, rows):
    from dcim.models import InventoryItemRole
    from dcim.models import Manufacturer
    from dcim.models.modules import ModuleType

    manufacturer_slugs, manufacturer_names = _slug_name_identity_inputs(
        rows, "manufacturer_slug", "manufacturer"
    )
    _prime_slug_name_identity_cache(
        runner,
        Manufacturer,
        slugs=manufacturer_slugs,
        names=manufacturer_names,
    )
    if model_string == "dcim.inventoryitem":
        role_slugs, role_names = _slug_name_identity_inputs(rows, "role_slug", "role")
        _prime_slug_name_identity_cache(
            runner,
            InventoryItemRole,
            slugs=role_slugs,
            names=role_names,
        )
        return

    requested_pairs: set[tuple[int, str]] = set()
    for row in rows:
        manufacturer = _manufacturer_from_row_cache(runner, row)
        if manufacturer is None:
            continue
        model_name = str(row.get("model") or "").strip()
        if not model_name:
            continue
        requested_pairs.add((manufacturer.pk, model_name))
    if not requested_pairs:
        return
    found_pairs: set[tuple[int, str]] = set()
    for manufacturer_id, model_values in _group_model_names_by_manufacturer(
        requested_pairs
    ).items():
        for chunk in _chunks(sorted(model_values), DEPENDENCY_LOOKUP_PAIR_CHUNK_SIZE):
            for obj in ModuleType.objects.filter(
                manufacturer_id=manufacturer_id,
                model__in=chunk,
            ):
                _remember_unique_lookup(
                    runner,
                    ModuleType,
                    {"manufacturer": obj.manufacturer_id, "model": obj.model},
                    obj,
                )
                found_pairs.add((obj.manufacturer_id, obj.model))
    for manufacturer_id, model_name in requested_pairs - found_pairs:
        _mark_missing_unique_lookup(
            runner,
            ModuleType,
            {"manufacturer": manufacturer_id, "model": model_name},
        )


def _prime_slug_name_identity_cache(runner, model, *, slugs, names):
    requested_slugs = {slug for slug in slugs if slug}
    requested_names = {name for name in names if name}
    if not requested_slugs and not requested_names:
        return

    found_slugs: set[str] = set()
    found_names: set[str] = set()
    if requested_slugs:
        for chunk in _chunks(
            sorted(requested_slugs), DEPENDENCY_LOOKUP_PAIR_CHUNK_SIZE
        ):
            for obj in model.objects.filter(slug__in=chunk):
                remember_lookup_object(runner, obj)
                _remember_unique_lookup(runner, model, {"slug": obj.slug}, obj)
                found_slugs.add(obj.slug)
    if requested_names:
        for chunk in _chunks(
            sorted(requested_names), DEPENDENCY_LOOKUP_PAIR_CHUNK_SIZE
        ):
            for obj in model.objects.filter(name__in=chunk):
                remember_lookup_object(runner, obj)
                _remember_unique_lookup(runner, model, {"name": obj.name}, obj)
                found_names.add(obj.name)
    for slug in requested_slugs - found_slugs:
        _mark_missing_unique_lookup(runner, model, {"slug": slug})
    for name in requested_names - found_names:
        _mark_missing_unique_lookup(runner, model, {"name": name})


def _slug_name_identity_inputs(rows, slug_field, name_field):
    slugs = set()
    names = set()
    for row in rows:
        slug_value = str(row.get(slug_field) or "").strip()
        name_value = str(row.get(name_field) or "").strip()
        if slug_value:
            slugs.add(slug_value)
            continue
        if name_value:
            names.add(name_value)
    return slugs, names


def _prime_device_type_identity_cache(runner, rows):
    from dcim.models import DeviceType

    requested_slugs = {
        str(row.get("device_type_slug")).strip()
        for row in rows
        if row.get("device_type_slug") not in ("", None)
    }
    requested_pairs: set[tuple[int, str]] = set()
    for row in rows:
        model_name = row.get("device_type")
        if model_name in ("", None):
            continue
        manufacturer = _manufacturer_from_row_cache(runner, row)
        if manufacturer is None:
            continue
        manufacturer_id = getattr(manufacturer, "pk", None)
        if manufacturer_id is None:
            continue
        requested_pairs.add((manufacturer_id, str(model_name).strip()))

    found_slugs: set[str] = set()
    found_pairs: set[tuple[int, str]] = set()
    if requested_slugs:
        for chunk in _chunks(
            sorted(requested_slugs), DEPENDENCY_LOOKUP_PAIR_CHUNK_SIZE
        ):
            for obj in DeviceType.objects.filter(slug__in=chunk):
                _remember_unique_lookup(runner, DeviceType, {"slug": obj.slug}, obj)
                _remember_unique_lookup(
                    runner,
                    DeviceType,
                    {"manufacturer": obj.manufacturer_id, "model": obj.model},
                    obj,
                )
                found_slugs.add(obj.slug)
                found_pairs.add((obj.manufacturer_id, obj.model))
    for manufacturer_id, model_values in _group_model_names_by_manufacturer(
        requested_pairs
    ).items():
        for chunk in _chunks(sorted(model_values), DEPENDENCY_LOOKUP_PAIR_CHUNK_SIZE):
            for obj in DeviceType.objects.filter(
                manufacturer_id=manufacturer_id,
                model__in=chunk,
            ):
                _remember_unique_lookup(runner, DeviceType, {"slug": obj.slug}, obj)
                _remember_unique_lookup(
                    runner,
                    DeviceType,
                    {"manufacturer": obj.manufacturer_id, "model": obj.model},
                    obj,
                )
                found_slugs.add(obj.slug)
                found_pairs.add((obj.manufacturer_id, obj.model))
    for slug in requested_slugs - found_slugs:
        _mark_missing_unique_lookup(runner, DeviceType, {"slug": slug})
    for manufacturer_id, model_name in requested_pairs - found_pairs:
        _mark_missing_unique_lookup(
            runner,
            DeviceType,
            {"manufacturer": manufacturer_id, "model": model_name},
        )


def _manufacturer_from_row_cache(runner, row):
    from dcim.models import Manufacturer

    manufacturer_slug = row.get("manufacturer_slug")
    manufacturer_name = row.get("manufacturer")
    manufacturer = None
    if manufacturer_slug not in ("", None):
        manufacturer = _cached_unique_identity_object(
            runner,
            Manufacturer,
            {"slug": str(manufacturer_slug).strip()},
        )
    if manufacturer is None and manufacturer_name not in ("", None):
        manufacturer = _cached_unique_identity_object(
            runner,
            Manufacturer,
            {"name": str(manufacturer_name).strip()},
        )
    return manufacturer


def _group_model_names_by_manufacturer(requested_pairs):
    grouped: dict[int, set[str]] = {}
    for manufacturer_id, model_name in requested_pairs:
        grouped.setdefault(manufacturer_id, set()).add(model_name)
    return grouped


def _mark_missing_unique_lookup(runner, model, lookup):
    cache_key = _unique_lookup_cache_key(model, lookup)
    if cache_key is not None:
        runner._primed_missing_unique_lookup_keys.add(cache_key)


def _prime_device_cache(runner, device_names):
    from dcim.models import Device

    missing = [
        name
        for name in device_names
        if name
        and name not in runner._device_by_name_cache
        and name not in runner._missing_device_by_name_cache
    ]
    if not missing:
        return
    for chunk in _chunks(sorted(missing), DEPENDENCY_LOOKUP_PAIR_CHUNK_SIZE):
        found_names = set()
        for device in Device.objects.filter(name__in=chunk):
            remember_lookup_object(runner, device)
            found_names.add(device.name)
        runner._missing_device_by_name_cache.update(set(chunk) - found_names)


def _prime_tag_cache(runner, tag_rows):
    from extras.models import Tag

    slugs = {
        slug for slug, _ in tag_rows if slug and slug not in runner._tag_by_slug_cache
    }
    names = {
        name
        for slug, name in tag_rows
        if not slug and name and name not in runner._tag_by_name_cache
    }
    if not slugs and not names:
        return
    if slugs:
        for chunk in _chunks(sorted(slugs), DEPENDENCY_LOOKUP_PAIR_CHUNK_SIZE):
            for tag in Tag.objects.filter(slug__in=chunk):
                remember_lookup_object(runner, tag)
    if names:
        for chunk in _chunks(sorted(names), DEPENDENCY_LOOKUP_PAIR_CHUNK_SIZE):
            for tag in Tag.objects.filter(name__in=chunk):
                remember_lookup_object(runner, tag)


def _prime_interface_cache(runner, interface_pairs):
    from dcim.models import Interface

    _prime_device_scoped_name_cache(
        runner,
        Interface,
        interface_pairs,
        runner._interface_by_device_name_cache,
        missing_cache=runner._missing_interface_by_device_name_cache,
    )


def _prime_module_bay_cache(runner, module_bay_pairs):
    from dcim.models.device_components import ModuleBay

    _prime_device_scoped_name_cache(
        runner,
        ModuleBay,
        module_bay_pairs,
        runner._module_bay_by_device_name_cache,
        missing_cache=runner._missing_module_bay_by_device_name_cache,
    )


def _prime_ipam_coalesce_identity_cache(runner, model_string, rows):
    if model_string not in {"ipam.prefix", "ipam.ipaddress"}:
        return
    from ipam.models import IPAddress
    from ipam.models import Prefix
    from ipam.models import VRF

    field_name = "prefix" if model_string == "ipam.prefix" else "address"
    model = Prefix if model_string == "ipam.prefix" else IPAddress

    identity_rows: list[tuple[str, str | None]] = []
    for row in rows:
        raw_value = row.get(field_name)
        if raw_value in ("", None):
            continue
        value = str(raw_value).strip()
        if not value:
            continue
        vrf_name = _vrf_name_from_row(row)
        identity_rows.append((value, vrf_name))
    if not identity_rows:
        return

    vrf_names = {vrf_name for _, vrf_name in identity_rows if vrf_name is not None}
    missing_vrf_names = [
        name for name in vrf_names if name not in runner._vrf_by_name_cache
    ]
    if missing_vrf_names:
        for vrf in VRF.objects.filter(name__in=missing_vrf_names):
            remember_lookup_object(runner, vrf)

    requested_pairs: set[tuple[str, int | None]] = set()
    for value, vrf_name in identity_rows:
        if vrf_name is None:
            requested_pairs.add((value, None))
            continue
        vrf = runner._vrf_by_name_cache.get(vrf_name)
        if vrf is not None:
            requested_pairs.add((value, vrf.pk))
    if not requested_pairs:
        return

    found_by_pair: dict[tuple[str, int | None], list[object]] = {}
    requested_by_vrf: dict[int | None, set[str]] = {}
    for value, vrf_id in requested_pairs:
        requested_by_vrf.setdefault(vrf_id, set()).add(value)

    for vrf_id, values in requested_by_vrf.items():
        for value_chunk in _chunks(sorted(values), DEPENDENCY_LOOKUP_PAIR_CHUNK_SIZE):
            filters = {f"{field_name}__in": value_chunk}
            if vrf_id is None:
                filters["vrf_id__isnull"] = True
            else:
                filters["vrf_id"] = vrf_id
            for obj in model.objects.filter(**filters):
                pair = (str(getattr(obj, field_name)), obj.vrf_id)
                if pair not in requested_pairs:
                    continue
                found_by_pair.setdefault(pair, []).append(obj)
    for pair, matches in found_by_pair.items():
        if len(matches) != 1:
            continue
        value, vrf_id = pair
        _remember_unique_lookup(
            runner,
            model,
            {field_name: value, "vrf": vrf_id},
            matches[0],
        )
    for value, vrf_id in requested_pairs - set(found_by_pair):
        cache_key = _unique_lookup_cache_key(model, {field_name: value, "vrf": vrf_id})
        if cache_key is not None:
            runner._primed_missing_unique_lookup_keys.add(cache_key)

    if model_string != "ipam.ipaddress":
        return
    _prime_ipam_global_host_identity_cache(
        runner,
        IPAddress,
        rows,
    )


def _vrf_name_from_row(row):
    raw_vrf = row.get("vrf")
    if raw_vrf in ("", None):
        return None
    value = str(raw_vrf).strip()
    return value or None


def _prime_ipam_global_host_identity_cache(runner, model, rows):
    host_values = {
        host_value
        for row in rows
        if _vrf_name_from_row(row) is None
        if (host_value := _row_ipam_host_value(row)) is not None
    }
    if not host_values:
        return
    found_by_host: dict[str, list[object]] = {}
    missing_host_values = set(host_values)
    for chunk in _chunks(sorted(host_values), DEPENDENCY_LOOKUP_PAIR_CHUNK_SIZE):
        if not chunk:
            continue
        query = Q(vrf__isnull=True)
        host_query = Q()
        for host_value in chunk:
            host_query |= Q(address__net_host=host_value)
        query &= host_query
        for obj in model.objects.filter(query):
            host_value = _ipaddress_host_value_from_address(obj.address)
            if host_value is None or host_value not in host_values:
                continue
            found_by_host.setdefault(host_value, []).append(obj)
            missing_host_values.discard(host_value)

    for host_value, matches in found_by_host.items():
        if len(matches) != 1:
            continue
        _remember_unique_lookup(
            runner,
            model,
            {"address__net_host": host_value, "vrf__isnull": True},
            matches[0],
        )
    for host_value in missing_host_values:
        cache_key = _unique_lookup_cache_key(
            model,
            {"address__net_host": host_value, "vrf__isnull": True},
        )
        if cache_key is not None:
            runner._primed_missing_unique_lookup_keys.add(cache_key)


def _row_ipam_host_value(row):
    host_value = row.get("host_ip")
    if host_value not in ("", None):
        host_string = str(host_value).strip()
        if host_string:
            return host_string
    return _ipaddress_host_value_from_address(row.get("address"))


def _ipaddress_host_value_from_address(value):
    if value in ("", None):
        return None
    try:
        return str(ip_interface(str(value)).ip)
    except ValueError:
        return None


def _prime_device_scoped_name_cache(runner, model, pairs, cache, *, missing_cache=None):
    device_names = {device_name for device_name, _ in pairs if device_name}
    _prime_device_cache(runner, device_names)
    missing_keys = {
        (device.pk, name)
        for device_name, name in pairs
        if (device := runner._device_by_name_cache.get(device_name)) is not None
        and (device.pk, name) not in cache
        and (missing_cache is None or (device.pk, name) not in missing_cache)
    }
    if not missing_keys:
        return
    for chunk in _chunks(sorted(missing_keys), DEPENDENCY_LOOKUP_PAIR_CHUNK_SIZE):
        query = _device_scoped_name_query(chunk)
        found_keys = set()
        for obj in model.objects.filter(query):
            remember_lookup_object(runner, obj)
            found_keys.add((obj.device_id, obj.name))
        if missing_cache is not None:
            missing_cache.update(set(chunk) - found_keys)


def _device_scoped_name_query(pairs):
    grouped_names_by_device: dict[int, set[str]] = {}
    for device_id, name in pairs:
        grouped_names_by_device.setdefault(device_id, set()).add(name)
    query = Q()
    for device_id, names in grouped_names_by_device.items():
        query |= Q(device_id=device_id, name__in=sorted(names))
    return query


def _chunks(items, size):
    for index in range(0, len(items), size):
        end_index = index + size
        yield items[index:end_index]


def _dedupe_lookups(lookups):
    deduped = []
    seen = set()
    for lookup in lookups:
        if not lookup:
            continue
        signature = tuple(
            sorted((key, _cache_lookup_value(value)) for key, value in lookup.items())
        )
        if signature in seen:
            continue
        seen.add(signature)
        deduped.append(lookup)
    return deduped
