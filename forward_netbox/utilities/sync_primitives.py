from ipaddress import ip_interface

from django.db import IntegrityError
from django.db.models import Q
from django.db.models.deletion import ProtectedError

from ..exceptions import ForwardDependencySkipError
from ..exceptions import ForwardQueryError
from ..exceptions import ForwardSearchError
from .sync_contracts import preserve_existing_on_blank_fields_for_model


DEPENDENCY_LOOKUP_PAIR_CHUNK_SIZE = 500
UNIQUE_LOOKUP_CACHE_FIELD_SETS = {
    "dcim.devicerole": (("slug",), ("name",)),
    "dcim.cable": (("pk",),),
    "dcim.devicetype": (("slug",), ("manufacturer", "slug"), ("manufacturer", "model")),
    "dcim.inventoryitemrole": (("slug",), ("name",)),
    "dcim.manufacturer": (("slug",), ("name",)),
    "dcim.modulebay": (("device", "name"),),
    "dcim.moduletype": (("manufacturer", "model"),),
    "dcim.platform": (("slug",), ("name",)),
    "dcim.site": (("slug",), ("name",)),
    "extras.tag": (("slug",), ("name",)),
    "ipam.ipaddress": (
        ("address", "vrf"),
        ("address__net_host", "vrf"),
        ("address__net_host", "vrf__isnull"),
    ),
    "ipam.fhrpgroup": (("protocol", "group_id", "name"),),
    "ipam.vlan": (("site", "vid"),),
    "ipam.prefix": (("prefix", "vrf"), ("prefix", "vrf__isnull")),
    "netbox_peering_manager.peeringsession": (("bgp_peer",),),
    "netbox_dlm.softwareversion": (("platform", "version"),),
    "netbox_dlm.hardwarenotice": (("device_type",),),
    "netbox_dlm.devicesoftware": (("device",),),
    "netbox_routing.bgprouter": (
        ("assigned_object_type", "assigned_object_id", "asn"),
    ),
    "netbox_routing.bgpscope": (("router", "vrf"),),
    "netbox_cisco_aci.acibridgedomain": (("aci_tenant", "name"),),
    "netbox_cisco_aci.acifabric": (("name",),),
    "netbox_cisco_aci.acifilter": (("aci_tenant", "name"),),
    "netbox_cisco_aci.acil3out": (("aci_tenant", "name"),),
    "netbox_cisco_aci.acinode": (("aci_pod", "node_id"), ("aci_pod", "name")),
    "netbox_cisco_aci.acipod": (("aci_fabric", "pod_id"), ("aci_fabric", "name")),
    "netbox_cisco_aci.acitenant": (("aci_fabric", "name"),),
    "netbox_cisco_aci.acivrf": (("aci_tenant", "name"),),
    "netbox_peering_manager.relationship": (("slug",), ("name",)),
    "netbox_routing.bgpaddressfamily": (("scope", "address_family"),),
    "netbox_routing.bgprouter": (
        ("assigned_object_type", "assigned_object_id", "asn"),
    ),
    "netbox_routing.bgppeer": (("scope", "peer"),),
    "netbox_routing.bgppeeraddressfamily": (
        ("assigned_object_type", "assigned_object_id", "address_family"),
    ),
    "netbox_routing.ospfarea": (("area_id",),),
    "netbox_routing.ospfinstance": (
        ("device", "process_id"),
        ("device", "vrf", "process_id"),
    ),
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
    return_change=False,
    create_instance_attrs=None,
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
            # Non-field instance attributes consulted by the model's save() — e.g.
            # dcim.Module honours `_adopt_components` to attach already-present
            # components instead of recreating (and colliding with) them.
            for attr, value in (create_instance_attrs or {}).items():
                setattr(obj, attr, value)
            obj.full_clean()
            obj.save()
            remember_lookup_object(runner, obj)
            _remember_unique_lookups(runner, model, lookups, obj)
            if return_change:
                return obj, True, True
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
    for field, value in _authoritative_update_values(
        model._meta.label_lower,
        update_values,
    ).items():
        if not _model_field_value_matches(model, obj, field, value):
            setattr(obj, field, value)
            update_fields.append(field)
    if update_fields:
        obj.full_clean()
        obj.save(update_fields=update_fields)
    remember_lookup_object(runner, obj)
    _remember_unique_lookups(runner, model, lookups, obj)
    if return_change:
        return obj, False, bool(update_fields)
    return obj, False


def _authoritative_update_values(model_string, update_values):
    preserved_fields = preserve_existing_on_blank_fields_for_model(model_string)
    if not preserved_fields:
        return dict(update_values)
    return {
        field: value
        for field, value in update_values.items()
        if not (field in preserved_fields and value in ("", None))
    }


def _model_field_value_matches(model, obj, field_name, value):
    field = model._meta.get_field(field_name)
    if getattr(field, "many_to_one", False) or getattr(field, "one_to_one", False):
        current_id = getattr(obj, field.attname)
        desired_id = getattr(value, "pk", value)
        return current_id == desired_id
    current = getattr(obj, field_name)
    if current == value:
        return True
    if (
        model._meta.label_lower == "ipam.prefix"
        and field_name == "prefix"
        and current is not None
        and value is not None
    ):
        return str(current) == str(value)
    if (
        model._meta.label_lower == "netbox_routing.ospfinstance"
        and field_name == "router_id"
        and current is not None
        and value is not None
    ):
        try:
            return str(ip_interface(str(current)).ip) == str(
                ip_interface(str(value)).ip
            )
        except ValueError:
            return str(current) == str(value)
    return False


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
    return_change=False,
    create_instance_attrs=None,
):
    return coalesce_update_or_create(
        runner,
        model,
        coalesce_lookups=coalesce_lookups,
        create_values=create_values,
        update_values=update_values,
        conflict_policy=runner._conflict_policy(model_string),
        return_change=return_change,
        create_instance_attrs=create_instance_attrs,
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


def upsert_values_from_defaults(
    runner, model_string, model, *, values, coalesce_sets, create_instance_attrs=None
):
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
        create_instance_attrs=create_instance_attrs,
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
    except Device.MultipleObjectsReturned:
        # NetBox Device.name is unique per site, not globally, so a duplicate
        # name (likelier once SNMP endpoints import as devices) raised straight
        # through here and failed the entire apply workload for the model.
        # Resolve deterministically to the earliest row instead and say so; the
        # by-name cache keeps this to one warning per duplicate name.
        device = Device.objects.filter(name=device_name).order_by("pk").first()
        runner.logger.log_warning(
            f"Multiple NetBox devices share the name `{device_name}`; using the "
            f"earliest (id {device.pk}). Rows keyed to this name may attach to "
            "another site's device — consider renaming the duplicates.",
            obj=runner.sync,
        )
    remember_lookup_object(runner, device)
    return device


def lookup_device_by_name(runner, device_name):
    from dcim.models import Device

    if not device_name:
        return None
    try:
        return get_device_by_name(runner, device_name)
    except Device.DoesNotExist:
        return None


def lookup_interface(runner, device, interface_name):
    from dcim.models import Interface

    from .interface_naming import canonical_interface_key

    if device is None or not interface_name:
        return None
    key = (device.pk, interface_name)
    if key in runner._interface_by_device_name_cache:
        return runner._interface_by_device_name_cache[key]
    if key in runner._missing_interface_by_device_name_cache:
        return None
    interface = runner._get_unique_or_raise(
        Interface,
        {"device": device, "name": interface_name},
    )
    if interface is None:
        # Forward reports abbreviated interface names (gi0/0/2); NetBox
        # device-type templates and pre-existing data use the canonical full form
        # (GigabitEthernet0/0/2). Match on the canonical (type, number) key so the
        # sync UPDATES the existing interface instead of creating a duplicate
        # (the gi0/0/2 + GigabitEthernet0/0/2 double-listing). Per-device map is
        # built once and cached.
        canon = canonical_interface_key(interface_name)
        if canon is not None:
            device_map = runner._interface_canonical_cache.get(device.pk)
            if device_map is None:
                device_map = {}
                for candidate in Interface.objects.filter(device=device):
                    candidate_key = canonical_interface_key(candidate.name)
                    if candidate_key is not None:
                        device_map.setdefault(candidate_key, candidate)
                runner._interface_canonical_cache[device.pk] = device_map
            interface = device_map.get(canon)
    if interface is None:
        runner._missing_interface_by_device_name_cache.add(key)
    else:
        runner._interface_by_device_name_cache[key] = interface
    return interface


def lookup_module_bay(runner, device, module_bay_name):
    from dcim.models import ModuleBay

    if device is None or not module_bay_name:
        return None
    key = (device.pk, module_bay_name)
    if key in runner._module_bay_by_device_name_cache:
        return runner._module_bay_by_device_name_cache[key]
    if key in runner._missing_module_bay_by_device_name_cache:
        return None
    module_bay = runner._get_unique_or_raise(
        ModuleBay,
        {"device": device, "name": module_bay_name},
    )
    if module_bay is None:
        runner._missing_module_bay_by_device_name_cache.add(key)
    return module_bay


def prime_dependency_lookup_caches(runner, model_string, rows):
    summary = {
        "available": False,
        "model": model_string,
        "row_count": len(rows),
        "device_name_count": 0,
        "tag_row_count": 0,
        "interface_pair_count": 0,
        "routing_interface_alias_count": 0,
        "routing_ospf_area_count": 0,
        "routing_ospf_instance_count": 0,
        "module_bay_pair_count": 0,
        "fhrp_group_count": 0,
        "vlan_pair_count": 0,
        "ipam_identity_row_count": 0,
        "ipam_global_host_row_count": 0,
        "primed_target_count": 0,
    }
    runner._primed_missing_unique_lookup_keys = set()
    _prime_dcim_dependency_identity_cache(runner, model_string, rows)
    device_names = _dependency_device_names(model_string, rows)
    if device_names:
        _prime_device_cache(runner, device_names)
        summary["device_name_count"] = len(device_names)
    tag_rows = _dependency_tag_rows(model_string, rows)
    if tag_rows:
        _prime_tag_cache(runner, tag_rows)
        summary["tag_row_count"] = len(tag_rows)
    interface_pairs = _dependency_interface_pairs(model_string, rows)
    if interface_pairs:
        _prime_interface_cache(runner, interface_pairs)
        summary["interface_pair_count"] = len(interface_pairs)
    routing_interface_alias_pairs = _dependency_routing_interface_alias_pairs(
        model_string, rows
    )
    if routing_interface_alias_pairs:
        _prime_routing_interface_candidate_cache(runner, routing_interface_alias_pairs)
        summary["routing_interface_alias_count"] = len(routing_interface_alias_pairs)
    routing_identity_summary = _prime_optional_dependency_cache(
        _prime_routing_bgp_identity_cache,
        runner,
        model_string,
        rows,
    )
    if routing_identity_summary:
        summary["routing_asn_count"] = routing_identity_summary["routing_asn_count"]
        summary["routing_bgp_router_count"] = routing_identity_summary[
            "routing_bgp_router_count"
        ]
        summary["routing_bgp_scope_count"] = routing_identity_summary[
            "routing_bgp_scope_count"
        ]
    routing_ospf_summary = _prime_optional_dependency_cache(
        _prime_routing_ospf_identity_cache,
        runner,
        model_string,
        rows,
    )
    if routing_ospf_summary:
        summary["routing_ospf_area_count"] = routing_ospf_summary[
            "routing_ospf_area_count"
        ]
        summary["routing_ospf_instance_count"] = routing_ospf_summary[
            "routing_ospf_instance_count"
        ]
    module_bay_pairs = _dependency_module_bay_pairs(model_string, rows)
    if module_bay_pairs:
        _prime_module_bay_cache(runner, module_bay_pairs)
        summary["module_bay_pair_count"] = len(module_bay_pairs)
    fhrp_group_keys = _dependency_fhrp_group_keys(model_string, rows)
    if fhrp_group_keys:
        _prime_fhrp_group_cache(runner, fhrp_group_keys)
        summary["fhrp_group_count"] = len(fhrp_group_keys)
    vlan_pairs = _dependency_vlan_pairs(model_string, rows)
    if vlan_pairs:
        _prime_vlan_cache(runner, vlan_pairs)
        summary["vlan_pair_count"] = len(vlan_pairs)
    ipam_identity_summary = _prime_ipam_coalesce_identity_cache(
        runner,
        model_string,
        rows,
    )
    if ipam_identity_summary:
        summary["ipam_identity_row_count"] = int(
            ipam_identity_summary.get("ipam_identity_row_count") or 0
        )
        summary["ipam_global_host_row_count"] = int(
            ipam_identity_summary.get("ipam_global_host_row_count") or 0
        )
    summary["primed_target_count"] = (
        summary["device_name_count"]
        + summary["tag_row_count"]
        + summary["interface_pair_count"]
        + summary["routing_interface_alias_count"]
        + summary.get("routing_asn_count", 0)
        + summary.get("routing_bgp_router_count", 0)
        + summary.get("routing_bgp_scope_count", 0)
        + summary["routing_ospf_area_count"]
        + summary["routing_ospf_instance_count"]
        + summary["module_bay_pair_count"]
        + summary["fhrp_group_count"]
        + summary["vlan_pair_count"]
        + summary["ipam_identity_row_count"]
        + summary["ipam_global_host_row_count"]
    )
    summary["available"] = bool(summary["primed_target_count"])
    return summary


DEPENDENCY_PARENT_DEVICE_FIELDS = {
    "dcim.interface": ("device",),
    "dcim.macaddress": ("device",),
    "dcim.cable": ("device", "remote_device"),
    "dcim.inventoryitem": ("device",),
    "dcim.module": ("device",),
    "dcim.virtualchassis": ("device",),
    "extras.taggeditem": ("device",),
    "ipam.fhrpgroup": ("device",),
    "ipam.ipaddress": ("device",),
    "netbox_peering_manager.peeringsession": ("device",),
    "netbox_routing.bgpaddressfamily": ("device",),
    "netbox_routing.bgppeer": ("device",),
    "netbox_routing.bgppeeraddressfamily": ("device",),
    "netbox_routing.ospfinstance": ("device",),
    "netbox_routing.ospfinterface": ("device",),
    "netbox_dlm.devicesoftware": ("name",),
}
DEPENDENCY_PARENT_DEVICE_MODELS = tuple(DEPENDENCY_PARENT_DEVICE_FIELDS)


def _dependency_parent_row_identity(model_string, row):
    for field in ("name", "interface", "module_bay", "address", "tag"):
        value = row.get(field)
        if value not in ("", None):
            return str(value)
    if model_string == "ipam.fhrpgroup":
        return str(row.get("group_id") or "")
    if model_string == "dcim.cable":
        left = row.get("interface") or ""
        right = row.get("remote_interface") or ""
        if left or right:
            return f"{left}->{right}".strip("->")
    return ""


def dependency_parent_coverage_summary(runner, model_string, rows):
    fields = DEPENDENCY_PARENT_DEVICE_FIELDS.get(model_string, ())
    if not fields:
        return {
            "available": False,
            "model": model_string,
            "row_count": len(rows),
            "blocked_row_count": 0,
            "missing_parent_count": 0,
            "missing_parent_names": [],
            "groups": [],
        }

    missing_device_by_name_cache = getattr(runner, "_missing_device_by_name_cache", {})
    if not isinstance(missing_device_by_name_cache, dict):
        missing_device_by_name_cache = {}
    groups: dict[tuple[str, str], dict] = {}
    for row in rows:
        for field in fields:
            device_name = str(row.get(field) or "").strip()
            if not device_name:
                continue
            if device_name not in missing_device_by_name_cache:
                continue
            key = (field, device_name)
            group = groups.setdefault(
                key,
                {
                    "parent_model": "dcim.device",
                    "parent_field": field,
                    "parent_name": device_name,
                    "row_count": 0,
                    "sample_rows": [],
                },
            )
            group["row_count"] += 1
            if len(group["sample_rows"]) < 5:
                sample = _dependency_parent_row_identity(model_string, row)
                if sample:
                    group["sample_rows"].append(sample)

    ordered_groups = sorted(
        groups.values(),
        key=lambda item: (
            -int(item.get("row_count") or 0),
            str(item.get("parent_field") or ""),
            str(item.get("parent_name") or ""),
        ),
    )
    blocked_row_count = sum(
        int(group.get("row_count") or 0) for group in ordered_groups
    )
    missing_parent_names = sorted(
        {
            str(group.get("parent_name") or "")
            for group in ordered_groups
            if group.get("parent_name")
        }
    )
    return {
        "available": bool(ordered_groups),
        "model": model_string,
        "row_count": len(rows),
        "blocked_row_count": blocked_row_count,
        "missing_parent_count": len(ordered_groups),
        "missing_parent_names": missing_parent_names,
        "groups": ordered_groups,
    }


def _prime_optional_dependency_cache(primer, runner, model_string, rows):
    try:
        return primer(runner, model_string, rows)
    except ForwardQueryError:
        return {}


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
        "ipam.fhrpgroup": ("device",),
        "ipam.ipaddress": ("device",),
        "netbox_peering_manager.peeringsession": ("device",),
        "netbox_routing.bgpaddressfamily": ("device",),
        "netbox_routing.bgppeer": ("device",),
        "netbox_routing.bgppeeraddressfamily": ("device",),
        "netbox_routing.ospfinstance": ("device",),
        "netbox_routing.ospfinterface": ("device",),
        "netbox_dlm.devicesoftware": ("name",),
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
        "ipam.fhrpgroup": (("device", "interface"),),
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


def _dependency_routing_interface_alias_pairs(model_string, rows):
    if model_string != "netbox_routing.ospfinterface":
        return set()
    from .sync_routing_impl import routing_interface_lookup_candidates

    return {
        (
            str(row.get("device")).strip(),
            candidate,
        )
        for row in rows
        if row.get("device") not in ("", None)
        and row.get("local_interface") not in ("", None)
        for candidate in routing_interface_lookup_candidates(row.get("local_interface"))
        if candidate
    }


def _sorted_dependency_scope_keys(keys):
    """Deterministic order for dependency-lookup chunking, tolerant of None.

    Routing scope keys carry a VRF pk that is None for global-table peers/
    instances alongside int VRF pks under the same router/device pk (e.g. a
    device with both a global BGP peer and a VRF peer). A plain ``sorted()`` then
    compares ``None < int`` and raises TypeError once the routing models are
    enabled. PKs and process ids are always >= 1, so a -1 sentinel orders None
    first without colliding; chunk order does not affect the lookup results.
    """
    return sorted(
        keys,
        key=lambda item: tuple(-1 if part is None else part for part in item),
    )


def _prime_routing_bgp_identity_cache(runner, model_string, rows):
    if model_string not in {
        "netbox_routing.bgppeer",
        "netbox_routing.bgpaddressfamily",
        "netbox_routing.bgppeeraddressfamily",
        "netbox_routing.ospfinstance",
        "netbox_routing.ospfinterface",
    }:
        return {}

    from django.apps import apps

    if not apps.is_installed("netbox_routing"):
        return {}

    from dcim.models import Device
    from ipam.models import ASN
    from ipam.models import VRF

    BGPRouter = runner._optional_model(
        "netbox_routing", "BGPRouter", "netbox_routing.bgppeer"
    )
    BGPScope = runner._optional_model(
        "netbox_routing", "BGPScope", "netbox_routing.bgppeer"
    )
    if BGPRouter is None or BGPScope is None:
        return {}

    asn_numbers = set()
    for row in rows:
        for field in ("local_asn", "peer_asn"):
            value = row.get(field)
            if value in ("", None):
                continue
            try:
                asn_numbers.add(int(value))
            except (TypeError, ValueError):
                continue
    if asn_numbers:
        for chunk in _chunks(sorted(asn_numbers), DEPENDENCY_LOOKUP_PAIR_CHUNK_SIZE):
            for asn in ASN.objects.filter(asn__in=chunk):
                remember_lookup_object(runner, asn)

    vrf_names = {
        str(row.get("vrf")).strip()
        for row in rows
        if row.get("vrf") not in ("", None) and str(row.get("vrf")).strip()
    }
    if vrf_names:
        _prime_slug_name_identity_cache(runner, VRF, slugs=set(), names=vrf_names)

    ct = runner._content_type_for(Device)
    requested_router_keys: set[tuple[int, int]] = set()
    for row in rows:
        device_name = str(row.get("device") or "").strip()
        local_asn_value = row.get("local_asn")
        if not device_name or local_asn_value in ("", None):
            continue
        device = runner._device_by_name_cache.get(device_name)
        if device is None:
            continue
        try:
            asn_number = int(local_asn_value)
        except (TypeError, ValueError):
            continue
        local_asn = runner._asn_by_number_cache.get(asn_number)
        if local_asn is None:
            continue
        requested_router_keys.add((device.pk, local_asn.pk))

    found_router_keys = set()
    if requested_router_keys:
        for chunk in _chunks(
            sorted(requested_router_keys), DEPENDENCY_LOOKUP_PAIR_CHUNK_SIZE
        ):
            device_ids = {device_id for device_id, _asn_id in chunk}
            asn_ids = {asn_id for _device_id, asn_id in chunk}
            query = Q(
                assigned_object_type=ct,
                assigned_object_id__in=device_ids,
                asn_id__in=asn_ids,
            )
            for obj in BGPRouter.objects.filter(query):
                _remember_unique_lookup(
                    runner,
                    BGPRouter,
                    {
                        "assigned_object_type": ct,
                        "assigned_object_id": obj.assigned_object_id,
                        "asn": obj.asn_id,
                    },
                    obj,
                )
                found_router_keys.add((obj.assigned_object_id, obj.asn_id))

    requested_scope_keys: set[tuple[int, int | None]] = set()
    for row in rows:
        device_name = str(row.get("device") or "").strip()
        local_asn_value = row.get("local_asn")
        if not device_name or local_asn_value in ("", None):
            continue
        device = runner._device_by_name_cache.get(device_name)
        if device is None:
            continue
        try:
            asn_number = int(local_asn_value)
        except (TypeError, ValueError):
            continue
        local_asn = runner._asn_by_number_cache.get(asn_number)
        if local_asn is None:
            continue
        router = _cached_unique_identity_object(
            runner,
            BGPRouter,
            {
                "assigned_object_type": ct,
                "assigned_object_id": device.pk,
                "asn": local_asn,
            },
        )
        if router is None:
            continue
        vrf_name = _vrf_name_from_row(row)
        vrf = runner._vrf_by_name_cache.get(vrf_name) if vrf_name else None
        requested_scope_keys.add((router.pk, getattr(vrf, "pk", None)))

    found_scope_keys = set()
    if requested_scope_keys:
        for chunk in _chunks(
            _sorted_dependency_scope_keys(requested_scope_keys),
            DEPENDENCY_LOOKUP_PAIR_CHUNK_SIZE,
        ):
            router_ids = {router_id for router_id, _vrf_id in chunk}
            query = Q(router_id__in=router_ids)
            for obj in BGPScope.objects.filter(query):
                _remember_unique_lookup(
                    runner,
                    BGPScope,
                    {"router": obj.router_id, "vrf": obj.vrf_id},
                    obj,
                )
                found_scope_keys.add((obj.router_id, obj.vrf_id))

    return {
        "routing_asn_count": len(asn_numbers),
        "routing_bgp_router_count": len(found_router_keys),
        "routing_bgp_scope_count": len(found_scope_keys),
    }


def _prime_routing_ospf_identity_cache(runner, model_string, rows):
    if model_string not in {
        "netbox_routing.ospfinstance",
        "netbox_routing.ospfinterface",
    }:
        return {}

    from django.apps import apps

    if not apps.is_installed("netbox_routing"):
        return {}

    OSPFInstance = runner._optional_model(
        "netbox_routing", "OSPFInstance", "netbox_routing.ospfinstance"
    )
    OSPFArea = runner._optional_model(
        "netbox_routing", "OSPFArea", "netbox_routing.ospfarea"
    )
    if OSPFInstance is None or OSPFArea is None:
        return {}

    from .sync_routing_impl import ospf_process_values

    vrf_names = {
        str(row.get("vrf")).strip()
        for row in rows
        if row.get("vrf") not in ("", None) and str(row.get("vrf")).strip()
    }
    if vrf_names:
        from ipam.models import VRF

        _prime_slug_name_identity_cache(runner, VRF, slugs=set(), names=vrf_names)

    requested_area_ids = {
        str(row.get("area_id")).strip()
        for row in rows
        if row.get("area_id") not in ("", None) and str(row.get("area_id")).strip()
    }
    found_area_ids = set()
    if requested_area_ids:
        for chunk in _chunks(
            sorted(requested_area_ids), DEPENDENCY_LOOKUP_PAIR_CHUNK_SIZE
        ):
            for obj in OSPFArea.objects.filter(area_id__in=chunk):
                _remember_unique_lookup(runner, OSPFArea, {"area_id": obj.area_id}, obj)
                found_area_ids.add(str(obj.area_id))

    requested_instance_keys: set[tuple[int, int | None, int]] = set()
    for row in rows:
        device_name = str(row.get("device") or "").strip()
        if not device_name:
            continue
        device = runner._device_by_name_cache.get(device_name)
        if device is None:
            continue
        vrf_name = _vrf_name_from_row(row)
        vrf = runner._vrf_by_name_cache.get(vrf_name) if vrf_name else None
        process_id, _process_label = ospf_process_values(row)
        requested_instance_keys.add((device.pk, getattr(vrf, "pk", None), process_id))

    found_instance_keys = set()
    if requested_instance_keys:
        for chunk in _chunks(
            _sorted_dependency_scope_keys(requested_instance_keys),
            DEPENDENCY_LOOKUP_PAIR_CHUNK_SIZE,
        ):
            device_ids = {device_id for device_id, _vrf_id, _process_id in chunk}
            process_ids = {process_id for _device_id, _vrf_id, process_id in chunk}
            query = Q(device_id__in=device_ids, process_id__in=process_ids)
            for obj in OSPFInstance.objects.filter(query):
                key = (obj.device_id, obj.vrf_id, obj.process_id)
                if key not in requested_instance_keys:
                    continue
                _remember_unique_lookup(
                    runner,
                    OSPFInstance,
                    {
                        "device": obj.device_id,
                        "vrf": obj.vrf_id,
                        "process_id": obj.process_id,
                    },
                    obj,
                )
                found_instance_keys.add(key)

    return {
        "routing_ospf_area_count": len(found_area_ids),
        "routing_ospf_instance_count": len(found_instance_keys),
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


def _dependency_vlan_pairs(model_string, rows):
    if model_string != "ipam.vlan":
        return set()
    return {
        (str(row.get("site")).strip(), int(row.get("vid")))
        for row in rows
        if row.get("site") not in ("", None)
        and row.get("vid") not in ("", None)
        and str(row.get("site")).strip()
    }


def _dependency_fhrp_group_keys(model_string, rows):
    if model_string != "ipam.fhrpgroup":
        return set()
    return {
        (
            str(row.get("protocol") or "hsrp").strip().lower() or "hsrp",
            int(row.get("group_id")),
            _dependency_fhrp_group_name(row),
        )
        for row in rows
        if row.get("group_id") not in ("", None) and str(row.get("group_id")).strip()
    }


def _dependency_fhrp_group_name(row):
    protocol = str(row.get("protocol") or "hsrp").strip().lower()
    group_id = str(row.get("group_id") or "").strip()
    address = str(row.get("address") or "").split("/", 1)[0]
    vrf = str(row.get("vrf") or "").strip()
    parts = [protocol, group_id]
    if vrf:
        parts.append(vrf)
    if address:
        parts.append(address)
    return "-".join(part for part in parts if part)[:100]


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
                    {"manufacturer": obj.manufacturer, "slug": obj.slug},
                    obj,
                )
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
                    {"manufacturer": obj.manufacturer, "slug": obj.slug},
                    obj,
                )
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


def _prime_routing_interface_candidate_cache(runner, routing_interface_alias_pairs):
    from dcim.models import Interface

    device_names = {device_name for device_name, _ in routing_interface_alias_pairs}
    _prime_device_cache(runner, device_names)

    requested_by_device: dict[int, set[str]] = {}
    for device_name, candidate in routing_interface_alias_pairs:
        device = runner._device_by_name_cache.get(device_name)
        if device is None:
            continue
        requested_by_device.setdefault(device.pk, set()).add(candidate)

    for device_id, candidates in requested_by_device.items():
        for chunk in _chunks(sorted(candidates), DEPENDENCY_LOOKUP_PAIR_CHUNK_SIZE):
            query = Q(device_id=device_id)
            candidate_query = Q()
            for candidate in chunk:
                candidate_query |= Q(name__iexact=candidate)
            query &= candidate_query
            for obj in Interface.objects.filter(query):
                remember_lookup_object(runner, obj)


def _prime_module_bay_cache(runner, module_bay_pairs):
    from dcim.models.device_components import ModuleBay

    _prime_device_scoped_name_cache(
        runner,
        ModuleBay,
        module_bay_pairs,
        runner._module_bay_by_device_name_cache,
        missing_cache=runner._missing_module_bay_by_device_name_cache,
    )


def _prime_vlan_cache(runner, vlan_pairs):
    from dcim.models import Site
    from ipam.models import VLAN

    site_names = {site_name for site_name, _ in vlan_pairs if site_name}
    _prime_slug_name_identity_cache(
        runner,
        Site,
        slugs=set(),
        names=site_names,
    )
    missing_keys = set()
    for site_name, vid in vlan_pairs:
        site = _cached_unique_identity_object(runner, Site, {"name": site_name})
        if site is None:
            site = _cached_unique_identity_object(runner, Site, {"slug": site_name})
        if site is None:
            continue
        cache_key = _unique_lookup_cache_key(VLAN, {"site": site, "vid": vid})
        if cache_key is None:
            continue
        if cache_key in runner._primed_missing_unique_lookup_keys:
            continue
        if cache_key in runner._unique_lookup_cache:
            continue
        missing_keys.add((site.pk, vid))
    if not missing_keys:
        return
    found_keys = set()
    for chunk in _chunks(sorted(missing_keys), DEPENDENCY_LOOKUP_PAIR_CHUNK_SIZE):
        grouped_by_site: dict[int, set[int]] = {}
        for site_id, vid in chunk:
            grouped_by_site.setdefault(site_id, set()).add(vid)
        query = Q()
        for site_id, vids in grouped_by_site.items():
            query |= Q(site_id=site_id, vid__in=sorted(vids))
        for obj in VLAN.objects.filter(query):
            _remember_unique_lookup(
                runner,
                VLAN,
                {"site": obj.site_id, "vid": obj.vid},
                obj,
            )
            found_keys.add((obj.site_id, obj.vid))
    for site_id, vid in missing_keys - found_keys:
        _mark_missing_unique_lookup(runner, VLAN, {"site": site_id, "vid": vid})


def _prime_fhrp_group_cache(runner, fhrp_group_keys):
    from ipam.models import FHRPGroup

    requested_keys = {
        (protocol, group_id, name)
        for protocol, group_id, name in fhrp_group_keys
        if protocol and group_id not in (None, "")
    }
    if not requested_keys:
        return

    requested_by_protocol: dict[str, dict[int, set[str]]] = {}
    for protocol, group_id, name in requested_keys:
        requested_by_protocol.setdefault(protocol, {}).setdefault(group_id, set()).add(
            name
        )

    found_keys = set()
    for protocol, groups in requested_by_protocol.items():
        for chunk in _chunks(sorted(groups.keys()), DEPENDENCY_LOOKUP_PAIR_CHUNK_SIZE):
            query = Q(protocol=protocol, group_id__in=chunk)
            for obj in FHRPGroup.objects.filter(query):
                key = (str(obj.protocol).strip().lower(), obj.group_id, obj.name)
                if key not in requested_keys:
                    continue
                _remember_unique_lookup(
                    runner,
                    FHRPGroup,
                    {
                        "protocol": key[0],
                        "group_id": key[1],
                        "name": key[2],
                    },
                    obj,
                )
                found_keys.add(key)

    for protocol, group_id, name in requested_keys - found_keys:
        _mark_missing_unique_lookup(
            runner,
            FHRPGroup,
            {"protocol": protocol, "group_id": group_id, "name": name},
        )


def _prime_ipam_coalesce_identity_cache(runner, model_string, rows):
    if model_string not in {"ipam.prefix", "ipam.ipaddress", "ipam.fhrpgroup"}:
        return {}
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
        return {}

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
        return {
            "ipam_identity_row_count": len(identity_rows),
            "ipam_global_host_row_count": 0,
        }

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
        return {
            "ipam_identity_row_count": len(identity_rows),
            "ipam_global_host_row_count": 0,
        }
    _prime_ipam_global_host_identity_cache(
        runner,
        IPAddress,
        rows,
    )
    return {
        "ipam_identity_row_count": len(identity_rows),
        "ipam_global_host_row_count": len(
            {
                host_value
                for row in rows
                if _vrf_name_from_row(row) is None
                if (host_value := _row_ipam_host_value(row)) is not None
            }
        ),
    }


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
