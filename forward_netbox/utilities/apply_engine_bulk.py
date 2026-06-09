from typing import Any


def bulk_orm_apply_simple_models(runner, model_string: str, rows: list[dict[str, Any]]):
    from django.db import transaction
    from django.db.models import Q

    if model_string == "dcim.macaddress":
        return bulk_orm_apply_macaddress(runner, rows)
    if model_string == "dcim.virtualchassis":
        return bulk_orm_apply_virtualchassis(runner, rows)

    from dcim.models import DeviceType
    from dcim.models import DeviceRole
    from dcim.models import Manufacturer
    from dcim.models import Platform
    from dcim.models import Site
    from ipam.models import VLAN
    from ipam.models import Prefix
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
        "ipam.prefix": {
            "model": Prefix,
            "required": ("prefix", "status"),
            "fields": ("prefix", "vrf", "status"),
            "lookup_fields": ("prefix",),
            "lookup_sets": (("prefix", "vrf"),),
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
    if model_string == "ipam.prefix":
        vrf_rows = [
            {
                "name": row.get("vrf"),
                "rd": None,
                "description": "",
                "enforce_unique": False,
            }
            for row in rows
            if row.get("vrf")
        ]
        bulk_orm_apply_simple_models(runner, "ipam.vrf", vrf_rows)
        vrf_names = {
            row.get("name") for row in vrf_rows if row.get("name") not in ("", None)
        }
        vrf_by_name = {
            vrf.name: vrf for vrf in VRF.objects.filter(name__in=vrf_names) if vrf.name
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
        if model_string == "ipam.prefix":
            normalized["vrf"] = (
                vrf_by_name.get(row.get("vrf")) if row.get("vrf") else None
            )
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
            continue
        runner.logger.increment_statistics(model_string, outcome="unchanged")

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


def bulk_orm_apply_macaddress(runner, rows: list[dict[str, Any]]):
    from dcim.models import Device
    from dcim.models import Interface
    from dcim.models import MACAddress
    from django.db import transaction

    from ..exceptions import ForwardDependencySkipError
    from ..exceptions import ForwardSearchError
    from ..exceptions import ForwardSyncDataError

    interface_content_type = runner._content_type_for(Interface)
    device_names = {
        row.get("device") for row in rows if row.get("device") not in ("", None)
    }
    interface_names = {
        row.get("interface") for row in rows if row.get("interface") not in ("", None)
    }
    mac_values = {
        row.get("mac") or row.get("mac_address")
        for row in rows
        if (row.get("mac") or row.get("mac_address")) not in ("", None)
    }

    devices_by_name = {
        device.name: device for device in Device.objects.filter(name__in=device_names)
    }
    interfaces_by_key = {
        (interface.device.name, interface.name): interface
        for interface in Interface.objects.select_related("device").filter(
            device__name__in=device_names,
            name__in=interface_names,
        )
    }
    macs_by_address = {
        str(mac.mac_address): mac
        for mac in MACAddress.objects.filter(mac_address__in=mac_values)
    }

    create_objects = {}
    update_objects = {}
    for row in rows:
        device_name = row.get("device")
        interface_name = row.get("interface")
        mac_address = row.get("mac") or row.get("mac_address")
        if not device_name or not interface_name or not mac_address:
            exc = ForwardSyncDataError(
                "MAC address row is missing required device, interface, or mac identity.",
                model_string="dcim.macaddress",
                context={
                    "required": ("device", "interface", "mac"),
                    "device": device_name,
                    "interface": interface_name,
                },
                data=row,
            )
            runner._mark_dependency_failed("dcim.macaddress", row)
            runner.logger.increment_statistics("dcim.macaddress", outcome="failed")
            runner._record_issue(
                "dcim.macaddress",
                str(exc),
                row,
                exception=exc,
                context=exc.context,
                defaults=exc.defaults,
            )
            continue

        device = devices_by_name.get(device_name)
        if device is None:
            key = (device_name,)
            if runner._dependency_failed("dcim.device", key):
                exc = ForwardDependencySkipError(
                    "Skipping MAC assignment because dependency "
                    f"`dcim.device` failed for {key}.",
                    model_string="dcim.macaddress",
                    context={"device": device_name, "interface": interface_name},
                    data=row,
                )
                runner.logger.increment_statistics(
                    "dcim.macaddress",
                    outcome="skipped",
                )
                runner._record_issue(
                    "dcim.macaddress",
                    str(exc),
                    row,
                    exception=exc,
                    context=exc.context,
                    defaults=exc.defaults,
                )
                continue
            exc = ForwardSearchError(
                f"Unable to find device `{device_name}` for MAC assignment.",
                model_string="dcim.macaddress",
                context={"device": device_name, "interface": interface_name},
                data=row,
            )
            runner._mark_dependency_failed("dcim.macaddress", row)
            runner.logger.increment_statistics("dcim.macaddress", outcome="failed")
            runner._record_issue(
                "dcim.macaddress",
                str(exc),
                row,
                exception=exc,
                context=exc.context,
                defaults=exc.defaults,
            )
            continue

        interface = interfaces_by_key.get((device.name, interface_name))
        if interface is None:
            key = (device.name, interface_name)
            if runner._dependency_failed("dcim.interface", key):
                exc = ForwardDependencySkipError(
                    "Skipping MAC assignment because dependency "
                    f"`dcim.interface` failed for {key}.",
                    model_string="dcim.macaddress",
                    context={"device": device.name, "interface": interface_name},
                    data=row,
                )
                runner.logger.increment_statistics(
                    "dcim.macaddress",
                    outcome="skipped",
                )
                runner._record_issue(
                    "dcim.macaddress",
                    str(exc),
                    row,
                    exception=exc,
                    context=exc.context,
                    defaults=exc.defaults,
                )
                continue
            exc = ForwardSearchError(
                f"Unable to find interface {interface_name} on device {device.name} "
                "for MAC assignment.",
                model_string="dcim.macaddress",
                context={"device": device.name, "interface": interface_name},
                data=row,
            )
            runner._mark_dependency_failed("dcim.macaddress", row)
            runner.logger.increment_statistics("dcim.macaddress", outcome="failed")
            runner._record_issue(
                "dcim.macaddress",
                str(exc),
                row,
                exception=exc,
                context=exc.context,
                defaults=exc.defaults,
            )
            continue

        mac_key = str(mac_address)
        mac = macs_by_address.get(mac_key)
        if mac is None:
            mac = create_objects.get(mac_key)
        if mac is None:
            mac = MACAddress(
                mac_address=mac_address,
                assigned_object_type=interface_content_type,
                assigned_object_id=interface.pk,
            )
            create_objects[mac_key] = mac
            macs_by_address[mac_key] = mac
        else:
            mac.assigned_object_type = interface_content_type
            mac.assigned_object_id = interface.pk
            if getattr(mac, "pk", None):
                update_objects[mac.pk] = mac

        mac.full_clean()
        runner.logger.increment_statistics("dcim.macaddress", outcome="applied")
        runner.events_clearer.increment()

    with transaction.atomic():
        if create_objects:
            MACAddress.objects.bulk_create(
                list(create_objects.values()),
                batch_size=1000,
            )
        if update_objects:
            MACAddress.objects.bulk_update(
                list(update_objects.values()),
                fields=["assigned_object_type", "assigned_object_id"],
                batch_size=1000,
            )

    runner.events_clearer.clear()
    return True


def bulk_orm_apply_virtualchassis(runner, rows: list[dict[str, Any]]):
    from dcim.models import Device
    from dcim.models import VirtualChassis
    from django.core.exceptions import ValidationError
    from django.db import transaction

    from ..exceptions import ForwardDependencySkipError
    from ..exceptions import ForwardSearchError
    from ..exceptions import ForwardSyncDataError

    usable_rows = []
    vc_names = set()
    device_names = set()
    for row in rows:
        vc_name = row.get("vc_name") or row.get("name")
        if row.get("device") and not row.get("vc_position"):
            runner._record_aggregated_skip_warning(
                model_string="dcim.virtualchassis",
                reason="virtual-chassis-without-position",
                warning_message=(
                    "Skipping incomplete virtual chassis assignment for device "
                    f"`{row['device']}` because the row has virtual chassis "
                    "membership but no `vc_position`."
                ),
            )
            runner.logger.increment_statistics(
                "dcim.virtualchassis",
                outcome="skipped",
            )
            continue
        if not vc_name:
            exc = ValidationError("Virtual chassis row is missing `vc_name`.")
            runner.logger.increment_statistics(
                "dcim.virtualchassis",
                outcome="failed",
            )
            runner._record_issue(
                "dcim.virtualchassis",
                str(exc),
                row,
                exception=exc,
                context={"required": ("vc_name", "name")},
            )
            continue
        usable_rows.append((row, vc_name))
        vc_names.add(vc_name)
        if row.get("device"):
            device_names.add(row["device"])

    if not usable_rows:
        runner.events_clearer.clear()
        return True

    existing_vcs = {
        vc.name: vc for vc in VirtualChassis.objects.filter(name__in=vc_names)
    }
    existing_devices = {
        device.name: device for device in Device.objects.filter(name__in=device_names)
    }
    create_vcs = []
    update_vcs = []
    vcs_by_name = dict(existing_vcs)

    for row, vc_name in usable_rows:
        domain = row.get("vc_domain", row.get("domain", ""))
        vc = vcs_by_name.get(vc_name)
        if vc is None:
            vc = VirtualChassis(name=vc_name, domain=domain)
            vc.full_clean()
            create_vcs.append(vc)
            vcs_by_name[vc_name] = vc
            continue
        if vc.domain != domain:
            vc.domain = domain
            vc.full_clean()
            update_vcs.append(vc)

    with transaction.atomic():
        if create_vcs:
            VirtualChassis.objects.bulk_create(create_vcs, batch_size=1000)
        if update_vcs:
            VirtualChassis.objects.bulk_update(
                update_vcs,
                fields=["domain"],
                batch_size=1000,
            )

    vcs_by_name = {
        vc.name: vc for vc in VirtualChassis.objects.filter(name__in=vc_names)
    }
    occupied_positions = {
        (device.virtual_chassis_id, device.vc_position): device
        for device in Device.objects.filter(
            virtual_chassis_id__in=[
                vc.pk for vc in vcs_by_name.values() if getattr(vc, "pk", None)
            ],
            vc_position__isnull=False,
        )
    }
    devices_to_update = []

    for row, vc_name in usable_rows:
        vc = vcs_by_name[vc_name]
        if not row.get("device"):
            runner.logger.increment_statistics(
                "dcim.virtualchassis",
                outcome="applied",
            )
            runner.events_clearer.increment()
            continue

        device_name = row["device"]
        device = existing_devices.get(device_name)
        if device is None:
            key = (device_name,)
            if runner._dependency_failed("dcim.device", key):
                exc = ForwardDependencySkipError(
                    "Skipping virtual chassis assignment because dependency "
                    f"`dcim.device` failed for {key}.",
                    model_string="dcim.virtualchassis",
                    context={"device": device_name},
                    data=row,
                )
                runner.logger.increment_statistics(
                    "dcim.virtualchassis",
                    outcome="skipped",
                )
                runner._record_issue(
                    "dcim.virtualchassis",
                    str(exc),
                    row,
                    exception=exc,
                    context=exc.context,
                    defaults=exc.defaults,
                )
                continue
            exc = ForwardSearchError(
                f"Unable to find device `{device_name}` for virtual chassis assignment.",
                model_string="dcim.virtualchassis",
                context={"device": device_name},
                data=row,
            )
            runner._mark_dependency_failed("dcim.virtualchassis", row)
            runner.logger.increment_statistics(
                "dcim.virtualchassis",
                outcome="failed",
            )
            runner._record_issue(
                "dcim.virtualchassis",
                str(exc),
                row,
                exception=exc,
                context=exc.context,
                defaults=exc.defaults,
            )
            continue

        position = row["vc_position"]
        position_key = (vc.pk, position)
        position_conflict = occupied_positions.get(position_key)
        if position_conflict is not None and position_conflict.pk != device.pk:
            exc = ForwardSyncDataError(
                f"Virtual chassis `{vc_name}` already has device "
                f"`{position_conflict.name}` at position `{position}`.",
                model_string="dcim.virtualchassis",
                context={
                    "device": device_name,
                    "virtual_chassis": vc_name,
                    "vc_position": position,
                    "conflicting_device": position_conflict.name,
                },
                data=row,
            )
            runner._mark_dependency_failed("dcim.virtualchassis", row)
            runner.logger.increment_statistics(
                "dcim.virtualchassis",
                outcome="failed",
            )
            runner._record_issue(
                "dcim.virtualchassis",
                str(exc),
                row,
                exception=exc,
                context=exc.context,
                defaults=exc.defaults,
            )
            continue

        device.virtual_chassis = vc
        device.vc_position = position
        device.full_clean()
        devices_to_update.append(device)
        occupied_positions[position_key] = device
        runner.logger.increment_statistics(
            "dcim.virtualchassis",
            outcome="applied",
        )
        runner.events_clearer.increment()

    with transaction.atomic():
        if devices_to_update:
            Device.objects.bulk_update(
                devices_to_update,
                fields=["virtual_chassis", "vc_position"],
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
    from django.db.models import Q

    with transaction.atomic():
        lookup_values = {
            field_name: []
            for field_name in {
                field for lookup_set in lookup_sets for field in lookup_set
            }
        }
        for values in normalized_rows:
            for field_name in lookup_values:
                value = values.get(field_name)
                if value not in ("", None):
                    lookup_values[field_name].append(value)

        existing_qs = model.objects.none()
        if any(lookup_values.values()):
            query = Q()
            for field_name, values in lookup_values.items():
                if values:
                    query |= Q(**{f"{field_name}__in": values})
            existing_qs = model.objects.filter(query).order_by("pk")

        lookup_cache = {lookup_set: {} for lookup_set in lookup_sets}
        for obj in existing_qs:
            for lookup_set in lookup_sets:
                key = lookup_key_from_object(obj, lookup_set)
                if key is not None and key not in lookup_cache[lookup_set]:
                    lookup_cache[lookup_set][key] = obj

        for values in normalized_rows:
            existing = None
            for lookup_set in lookup_sets:
                lookup_key = lookup_key_from_values(values, lookup_set)
                if lookup_key is None:
                    continue
                if lookup_key in lookup_cache[lookup_set]:
                    existing = lookup_cache[lookup_set][lookup_key]
                if existing is not None:
                    break
            if existing is None:
                obj = model(**values)
                obj.full_clean()
                obj.save()
                runner.logger.increment_statistics(model_string, outcome="applied")
                runner.events_clearer.increment()
                for lookup_set in lookup_sets:
                    lookup_key = lookup_key_from_values(values, lookup_set)
                    if lookup_key is not None:
                        lookup_cache[lookup_set][lookup_key] = obj
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
                continue
            runner.logger.increment_statistics(model_string, outcome="unchanged")
    runner.events_clearer.clear()
    return True
