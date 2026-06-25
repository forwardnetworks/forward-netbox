from django.core.exceptions import ObjectDoesNotExist

from ..exceptions import ForwardDependencySkipError
from ..exceptions import ForwardSearchError
from .sync_primitives import forget_lookup_object


def _device_tag_ids(runner, device):
    cached = runner._device_tag_ids_cache.get(device.pk)
    if cached is None:
        cached = set(device.tags.values_list("pk", flat=True))
        runner._device_tag_ids_cache[device.pk] = cached
    return cached


def _device_has_tag(runner, device, tag):
    return tag.pk in _device_tag_ids(runner, device)


def _device_add_tag(runner, device, tag):
    if _device_has_tag(runner, device, tag):
        return False
    device.tags.add(tag)
    runner._device_tag_ids_cache.setdefault(device.pk, set()).add(tag.pk)
    return True


def _device_remove_tag(runner, device, tag):
    if not _device_has_tag(runner, device, tag):
        return False
    device.tags.remove(tag)
    runner._device_tag_ids_cache.setdefault(device.pk, set()).discard(tag.pk)
    return True


def delete_extras_taggeditem(runner, row):
    from extras.models import Tag

    device = runner._lookup_device_by_name(row.get("device"))
    tag = None
    if row.get("tag_slug"):
        tag = runner._get_unique_or_raise(Tag, {"slug": row.get("tag_slug")})
    if tag is None and row.get("tag"):
        tag = runner._get_unique_or_raise(Tag, {"name": row.get("tag")})
    if device is None or tag is None:
        return False
    return _device_remove_tag(runner, device, tag)


def delete_dcim_interface(runner, row):
    from dcim.models import Interface

    device = runner._lookup_device_by_name(row.get("device"))
    if device is None or not row.get("name"):
        return False
    return runner._delete_by_coalesce(
        Interface,
        [{"device": device, "name": row["name"]}],
    )


def delete_dcim_macaddress(runner, row):
    from dcim.models import MACAddress

    mac_address = row.get("mac_address") or row.get("mac")
    if not mac_address:
        return False
    return runner._delete_by_coalesce(
        MACAddress,
        [{"mac_address": mac_address}],
    )


def apply_extras_taggeditem(runner, row):
    from extras.models import Tag

    try:
        device = runner._get_device_by_name(row["device"])
    except ObjectDoesNotExist as exc:
        key = (row["device"],)
        if runner._dependency_failed("dcim.device", key):
            raise ForwardDependencySkipError(
                f"Skipping feature tag `{row.get('tag')}` because dependency `dcim.device` failed for {key}.",
                model_string="extras.taggeditem",
                context={"device": row["device"], "tag": row.get("tag")},
                data=row,
            ) from exc
        raise ForwardSearchError(
            f"Unable to find device `{row['device']}` for feature tag `{row.get('tag')}`.",
            model_string="extras.taggeditem",
            context={"device": row["device"], "tag": row.get("tag")},
            data=row,
        ) from exc

    tag, _ = runner._upsert_values_from_defaults(
        "extras.taggeditem",
        Tag,
        values={
            "name": row["tag"],
            "slug": row["tag_slug"],
            "color": row["tag_color"],
        },
        coalesce_sets=[("slug",)],
    )
    _device_add_tag(runner, device, tag)


def apply_dcim_macaddress(runner, row):
    from dcim.models import Interface
    from dcim.models import MACAddress

    try:
        device = runner._get_device_by_name(row["device"])
    except ObjectDoesNotExist as exc:
        key = (row["device"],)
        if runner._dependency_failed("dcim.device", key):
            raise ForwardDependencySkipError(
                f"Skipping MAC assignment because dependency `dcim.device` failed for {key}.",
                model_string="dcim.macaddress",
                context={
                    "device": row["device"],
                    "interface": row.get("interface"),
                },
                data=row,
            ) from exc
        raise ForwardSearchError(
            f"Unable to find device `{row['device']}` for MAC assignment.",
            model_string="dcim.macaddress",
            context={"device": row["device"], "interface": row.get("interface")},
            data=row,
        ) from exc
    interface = runner._lookup_interface(device, row["interface"])
    if interface is None:
        key = (device.name, row["interface"])
        if runner._dependency_failed("dcim.interface", key):
            raise ForwardDependencySkipError(
                f"Skipping MAC assignment because dependency `dcim.interface` failed for {key}.",
                model_string="dcim.macaddress",
                context={"device": device.name, "interface": row["interface"]},
                data=row,
            )
        runner._record_aggregated_skip_warning(
            model_string="dcim.macaddress",
            reason="missing-interface",
            warning_message=(
                f"Skipping MAC address `{row['mac']}` on `{device.name}` "
                f"`{row['interface']}` because the target interface was not imported."
            ),
        )
        return False
    runner._upsert_values_from_defaults(
        "dcim.macaddress",
        MACAddress,
        values={
            "mac_address": row["mac"],
            "assigned_object_type": runner._content_type_for(Interface),
            "assigned_object_id": interface.pk,
        },
        coalesce_sets=runner._coalesce_sets_for(
            "dcim.macaddress",
            [("mac_address",)],
        ),
    )


def _interface_untagged_vlan(runner, device, row):
    from ipam.models import VLAN

    vid = row.get("untagged_vlan")
    if vid in (None, ""):
        return False, None
    try:
        vid = int(vid)
    except (TypeError, ValueError):
        runner._record_aggregated_skip_warning(
            model_string="dcim.interface",
            reason="invalid-untagged-vlan",
            warning_message=(
                f"Skipping untagged VLAN assignment for interface `{row.get('name')}` "
                f"on `{device.name}` because VLAN ID `{row.get('untagged_vlan')}` "
                "is not a valid integer."
            ),
        )
        return False, None
    vlan = runner._get_unique_or_raise(VLAN, {"site": device.site, "vid": vid})
    if vlan is None:
        runner._record_aggregated_skip_warning(
            model_string="dcim.interface",
            reason="missing-untagged-vlan",
            warning_message=(
                f"Skipping untagged VLAN `{vid}` assignment for interface "
                f"`{row.get('name')}` on `{device.name}` because the VLAN was not imported "
                f"for site `{device.site}`."
            ),
        )
        return False, None
    return True, vlan


def apply_dcim_interface(runner, row):
    from dcim.models import Interface

    try:
        device = runner._get_device_by_name(row["device"])
    except ObjectDoesNotExist as exc:
        key = (row["device"],)
        if runner._dependency_failed("dcim.device", key):
            raise ForwardDependencySkipError(
                f"Skipping interface `{row.get('name')}` because dependency `dcim.device` failed for {key}.",
                model_string="dcim.interface",
                context={"device": row["device"], "name": row.get("name")},
                data=row,
            ) from exc
        raise ForwardSearchError(
            f"Unable to find device `{row['device']}` for interface `{row.get('name')}`.",
            model_string="dcim.interface",
            context={"device": row["device"], "name": row.get("name")},
            data=row,
        ) from exc
    defaults = {
        "device": device,
        "name": row["name"],
        "type": row["type"],
        "enabled": row["enabled"],
    }
    if row.get("mtu") not in ("", None):
        defaults["mtu"] = row["mtu"]
    if row.get("speed") not in ("", None):
        defaults["speed"] = row["speed"]
    description = row.get("description")
    if description not in (None, ""):
        defaults["description"] = description
    if row.get("mode") in {"access", "tagged"}:
        defaults["mode"] = row["mode"]
        found_vlan, vlan = _interface_untagged_vlan(runner, device, row)
        if found_vlan:
            defaults["untagged_vlan"] = vlan
    existing_interface = runner._lookup_interface(device, row["name"])
    if row["type"] == "lag" and existing_interface is not None:
        existing_cable = getattr(existing_interface, "cable", None)
        if existing_cable is not None:
            runner.logger.log_warning(
                f"Removing existing cable from LAG interface `{row['name']}` on device `{device.name}` before updating interface type.",
                obj=runner.sync,
            )
            existing_cable.delete()
            forget_lookup_object(runner, existing_interface)
    if row.get("lag"):
        if row["lag"] == row["name"]:
            raise ForwardSearchError(
                f"Interface `{row['name']}` on device `{device.name}` cannot be its own LAG parent.",
                model_string="dcim.interface",
                context={
                    "device": device.name,
                    "name": row["name"],
                    "lag": row["lag"],
                },
                data=row,
            )
        create_values = {
            "device": device,
            "name": row["lag"],
            "type": "lag",
            "enabled": True,
            "mtu": None,
            "description": "",
            "speed": None,
        }
        update_values = {
            "device": device,
            "name": row["lag"],
            "type": "lag",
            # Do NOT force enabled here: the LAG parent's own interface row carries
            # Forward's operStatus-derived enabled. Forcing True on the member-ensure
            # update fights that row (parent often reports operStatus DOWN), so every
            # sync flip-flops enabled True<->False — perpetual churn. Existence +
            # type=lag is all the member needs; enabled follows the parent's own row.
        }
        lag, _ = runner._upsert_row(
            "dcim.interface",
            Interface,
            row={"device": device, "name": row["lag"]},
            create_values=create_values,
            update_values=update_values,
            coalesce_sets=runner._coalesce_sets_for(
                "dcim.interface",
                [("device", "name")],
            ),
        )
        defaults["lag"] = lag
    runner._upsert_values_from_defaults(
        "dcim.interface",
        Interface,
        values=defaults,
        coalesce_sets=runner._coalesce_sets_for(
            "dcim.interface",
            [("device", "name")],
        ),
    )
