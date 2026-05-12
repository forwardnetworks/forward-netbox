from django.core.exceptions import ObjectDoesNotExist

from ..exceptions import ForwardDependencySkipError
from ..exceptions import ForwardSearchError
from ..exceptions import ForwardSyncDataError


def delete_dcim_device(runner, row):
    from dcim.models import Device

    return runner._delete_by_coalesce(
        Device,
        [runner._coalesce_lookup(row, "name")],
    )


def delete_dcim_virtualchassis(runner, row):
    from dcim.models import VirtualChassis

    name = row.get("vc_name") or row.get("name")
    if not name:
        return False
    return runner._delete_by_coalesce(VirtualChassis, [{"name": name}])


def apply_dcim_virtualchassis(runner, row):
    from dcim.models import Device
    from dcim.models import VirtualChassis

    vc_name = row.get("vc_name") or row.get("name")
    vc_values = {
        "name": vc_name,
        "domain": row.get("vc_domain", row.get("domain", "")),
    }
    vc, _ = runner._upsert_values_from_defaults(
        "dcim.virtualchassis",
        VirtualChassis,
        values=vc_values,
        coalesce_sets=runner._coalesce_sets_for(
            "dcim.virtualchassis",
            [("name",)],
        ),
    )
    if row.get("device"):
        try:
            device = Device.objects.get(name=row["device"])
        except ObjectDoesNotExist as exc:
            key = (row["device"],)
            if runner._dependency_failed("dcim.device", key):
                raise ForwardDependencySkipError(
                    f"Skipping virtual chassis assignment because dependency `dcim.device` failed for {key}.",
                    model_string="dcim.virtualchassis",
                    context={"device": row["device"]},
                    data=row,
                ) from exc
            raise ForwardSearchError(
                f"Unable to find device `{row['device']}` for virtual chassis assignment.",
                model_string="dcim.virtualchassis",
                context={"device": row["device"]},
                data=row,
            ) from exc
        if not row.get("vc_position"):
            raise ForwardSyncDataError(
                "Virtual chassis assignment requires `vc_position`; update the Forward NQE map before syncing.",
                model_string="dcim.virtualchassis",
                context={"device": row["device"], "virtual_chassis": vc_name},
                data=row,
            )
        position_conflict = (
            Device.objects.filter(virtual_chassis=vc, vc_position=row["vc_position"])
            .exclude(pk=device.pk)
            .order_by("name")
            .first()
        )
        if position_conflict is not None:
            raise ForwardSyncDataError(
                f"Virtual chassis `{vc_name}` already has device `{position_conflict.name}` at position `{row['vc_position']}`.",
                model_string="dcim.virtualchassis",
                context={
                    "device": row["device"],
                    "virtual_chassis": vc_name,
                    "vc_position": row["vc_position"],
                    "conflicting_device": position_conflict.name,
                },
                data=row,
            )
        defaults = {"virtual_chassis": vc, "vc_position": row["vc_position"]}
        Device.objects.filter(pk=device.pk).update(**defaults)
    return vc


def apply_dcim_device(runner, row):
    from dcim.models import Device

    site = runner._ensure_site({"name": row["site"], "slug": row["site_slug"]})
    role = runner._ensure_role(
        {"name": row["role"], "slug": row["role_slug"], "color": row["role_color"]}
    )
    device_type = runner._ensure_device_type(
        {
            "manufacturer": row["manufacturer"],
            "manufacturer_slug": row["manufacturer_slug"],
            "slug": row["device_type_slug"],
            "model": row["device_type"],
            **({"part_number": row["part_number"]} if "part_number" in row else {}),
        }
    )
    platform = None
    if row.get("platform"):
        platform = runner._ensure_platform(
            {
                "name": row["platform"],
                "manufacturer": row["manufacturer"],
                "manufacturer_slug": row["manufacturer_slug"],
                "slug": row["platform_slug"],
            }
        )

    defaults = {
        "name": row["name"],
        "site": site,
        "role": role,
        "device_type": device_type,
        "platform": platform,
        "serial": row.get("serial", ""),
        "status": row["status"],
    }
    if row.get("virtual_chassis") and row.get("vc_position"):
        defaults["virtual_chassis"] = runner._apply_dcim_virtualchassis(
            {"name": row["virtual_chassis"]}
        )
        defaults["vc_position"] = row["vc_position"]
    elif row.get("virtual_chassis"):
        runner._record_aggregated_skip_warning(
            model_string="dcim.device",
            reason="virtual-chassis-without-position",
            warning_message=(
                "Skipping incomplete virtual chassis assignment on device "
                f"`{row['name']}` because the row has `virtual_chassis` but no "
                "`vc_position`. True virtual chassis membership should be emitted "
                "by the `dcim.virtualchassis` map."
            ),
        )

    runner._upsert_values_from_defaults(
        "dcim.device",
        Device,
        values=defaults,
        coalesce_sets=runner._coalesce_sets_for(
            "dcim.device",
            [("name",)],
        ),
    )
