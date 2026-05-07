from django.core.exceptions import ObjectDoesNotExist

from ..exceptions import ForwardDependencySkipError
from ..exceptions import ForwardQueryError
from ..exceptions import ForwardSearchError


def delete_dcim_inventoryitem(runner, row):
    from dcim.models import Device
    from dcim.models import InventoryItem

    device = Device.objects.filter(name=row.get("device")).order_by("pk").first()
    if device is None or not row.get("name"):
        return False
    return runner._delete_by_coalesce(
        InventoryItem,
        [
            {
                "device": device,
                "name": row["name"],
                "part_id": row.get("part_id") or "",
                "serial": row.get("serial") or "",
            },
            {
                "device": device,
                "name": row["name"],
                "part_id": row.get("part_id") or "",
            },
            {"device": device, "name": row["name"]},
        ],
    )


def delete_dcim_module(runner, row):
    from dcim.models import Device
    from dcim.models import Module

    device = Device.objects.filter(name=row.get("device")).order_by("pk").first()
    if device is None or not row.get("module_bay"):
        return False
    module_bay = runner._lookup_module_bay(device, row["module_bay"])
    if module_bay is None:
        return False
    return runner._delete_by_coalesce(
        Module,
        [{"device": device, "module_bay": module_bay}],
    )


def apply_dcim_inventoryitem(runner, row):
    from dcim.models import Device
    from dcim.models import InventoryItem

    try:
        device = Device.objects.get(name=row["device"])
    except ObjectDoesNotExist as exc:
        key = (row["device"],)
        if runner._dependency_failed("dcim.device", key):
            raise ForwardDependencySkipError(
                f"Skipping inventory item because dependency `dcim.device` failed for {key}.",
                model_string="dcim.inventoryitem",
                context={"device": row["device"], "name": row.get("name")},
                data=row,
            ) from exc
        raise ForwardSearchError(
            f"Unable to find device `{row['device']}` for inventory item `{row.get('name')}`.",
            model_string="dcim.inventoryitem",
            context={"device": row["device"], "name": row.get("name")},
            data=row,
        ) from exc
    if runner.sync.is_model_enabled("dcim.module") and runner._is_module_native_inventory_row(row):
        return None if delete_dcim_inventoryitem(runner, row) else False
    manufacturer = None
    if row.get("manufacturer"):
        manufacturer = runner._ensure_manufacturer(
            {"name": row["manufacturer"], "slug": row["manufacturer_slug"]}
        )
    role = runner._ensure_inventory_item_role(row)
    runner._upsert_values_from_defaults(
        "dcim.inventoryitem",
        InventoryItem,
        values={
            "device": device,
            "name": row["name"],
            "label": row.get("label") or "",
            "part_id": row.get("part_id") or "",
            "serial": row.get("serial") or "",
            "asset_tag": row.get("asset_tag") or None,
            "status": row["status"],
            "role": role,
            "manufacturer": manufacturer,
            "discovered": row["discovered"],
            "description": row.get("description") or "",
        },
        coalesce_sets=runner._coalesce_sets_for(
            "dcim.inventoryitem",
            [
                ("device", "name", "part_id", "serial"),
                ("device", "name", "part_id"),
                ("device", "name"),
            ],
        ),
    )


def apply_dcim_module(runner, row):
    from dcim.models import Device
    from dcim.models import Module

    try:
        device = Device.objects.get(name=row["device"])
    except ObjectDoesNotExist as exc:
        key = (row["device"],)
        if runner._dependency_failed("dcim.device", key):
            raise ForwardDependencySkipError(
                f"Skipping module because dependency `dcim.device` failed for {key}.",
                model_string="dcim.module",
                context={
                    "device": row["device"],
                    "module_bay": row.get("module_bay"),
                },
                data=row,
            ) from exc
        raise ForwardSearchError(
            f"Unable to find device `{row['device']}` for module `{row.get('module_bay')}`.",
            model_string="dcim.module",
            context={
                "device": row["device"],
                "module_bay": row.get("module_bay"),
            },
            data=row,
        ) from exc

    if not row.get("module_bay"):
        runner._record_aggregated_skip_warning(
            model_string="dcim.module",
            reason="missing-module-bay",
            warning_message=(
                f"Skipping module row because no module bay was provided for "
                f"`{device.name}`."
            ),
        )
        return False
    module_bay = runner._ensure_module_bay(device, row)
    module_type = runner._ensure_module_type(row)
    runner._upsert_values_from_defaults(
        "dcim.module",
        Module,
        values={
            "device": device,
            "module_bay": module_bay,
            "module_type": module_type,
            "status": row["status"],
            "serial": row.get("serial") or "",
            "asset_tag": row.get("asset_tag") or None,
        },
        coalesce_sets=runner._coalesce_sets_for(
            "dcim.module",
            [("device", "module_bay")],
        ),
    )
