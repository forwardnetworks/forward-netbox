from django.core.exceptions import ObjectDoesNotExist

from ..exceptions import ForwardDependencySkipError
from ..exceptions import ForwardSearchError


def delete_extras_taggeditem(runner, row):
    from dcim.models import Device
    from extras.models import Tag

    device = Device.objects.filter(name=row.get("device")).order_by("pk").first()
    tag = Tag.objects.filter(slug=row.get("tag_slug")).order_by("pk").first()
    if device is None or tag is None:
        return False
    if tag not in device.tags.all():
        return False
    device.tags.remove(tag)
    return True


def delete_dcim_interface(runner, row):
    from dcim.models import Device
    from dcim.models import Interface

    device = Device.objects.filter(name=row.get("device")).order_by("pk").first()
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
    from dcim.models import Device
    from extras.models import Tag

    try:
        device = Device.objects.get(name=row["device"])
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
    device.tags.add(tag)


def apply_dcim_macaddress(runner, row):
    from dcim.models import Device
    from dcim.models import Interface
    from dcim.models import MACAddress

    try:
        device = Device.objects.get(name=row["device"])
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
        raise ForwardSearchError(
            f"Unable to find interface {row['interface']} on device {device.name} for MAC assignment.",
            model_string="dcim.macaddress",
            context={"device": device.name, "interface": row["interface"]},
            data=row,
        )
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


def apply_dcim_interface(runner, row):
    from dcim.models import Device
    from dcim.models import Interface

    try:
        device = Device.objects.get(name=row["device"])
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
        "mtu": row.get("mtu") or None,
        "description": row.get("description") or "",
        "speed": row.get("speed") or None,
    }
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
        lag, _ = runner._upsert_values_from_defaults(
            "dcim.interface",
            Interface,
            values={
                "device": device,
                "name": row["lag"],
                "type": "lag",
                "enabled": True,
                "mtu": None,
                "description": "",
                "speed": None,
            },
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
