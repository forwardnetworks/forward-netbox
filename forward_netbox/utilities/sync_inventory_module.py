from django.core.exceptions import ObjectDoesNotExist

from ..exceptions import ForwardDependencySkipError
from ..exceptions import ForwardSearchError


def delete_dcim_inventoryitem(runner, row):
    from dcim.models import InventoryItem

    device = runner._lookup_device_by_name(row.get("device"))
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


# Component collections NetBox instantiates from a module type's templates, as
# (template accessor, component accessor) pairs. Mirrors the loop in
# `dcim.models.Module.save`.
_MODULE_COMPONENT_TEMPLATES = (
    ("consoleporttemplates", "consoleports"),
    ("consoleserverporttemplates", "consoleserverports"),
    ("interfacetemplates", "interfaces"),
    ("powerporttemplates", "powerports"),
    ("poweroutlettemplates", "poweroutlets"),
    ("rearporttemplates", "rearports"),
    ("frontporttemplates", "frontports"),
    ("modulebaytemplates", "modulebays"),
)


def _unadoptable_component_names(device, module_type):
    """Component names this module type would create that are already claimed.

    NetBox adopts an existing component only when it belongs to no module
    (`module__isnull=True`). One already assigned to a different module is
    neither adoptable nor replaceable, so instantiating the template violates
    the per-device unique name constraint. Returns the colliding names.
    """
    blocking = []
    for template_attribute, component_attribute in _MODULE_COMPONENT_TEMPLATES:
        templates = getattr(module_type, template_attribute, None)
        components = getattr(device, component_attribute, None)
        if templates is None or components is None:
            continue
        claimed = set(
            components.filter(module__isnull=False).values_list("name", flat=True)
        )
        if not claimed:
            continue
        for name in templates.all().values_list("name", flat=True):
            if name in claimed:
                blocking.append(f"{component_attribute}:{name}")
    return blocking


def delete_dcim_module(runner, row):
    from dcim.models import Module

    device = runner._lookup_device_by_name(row.get("device"))
    if device is None or not row.get("module_bay"):
        return False
    module_bay = runner._lookup_module_bay(device, row["module_bay"])
    if module_bay is None:
        return False
    return runner._delete_by_coalesce(
        Module,
        [{"device": device, "module_bay": module_bay}],
    )


MODULE_NATIVE_ROW_NOT_COMPARABLE = "module-native-row-not-comparable"


def apply_dcim_inventoryitem(runner, row, *, preview=False):
    """Upsert the inventory item, or - with ``preview`` - classify and write.

    ``preview`` returns ``"creates"``, ``"updates"``, ``"unchanged"``, ``False``
    for a row this declines, or ``MODULE_NATIVE_ROW_NOT_COMPARABLE``.

    The writes are all behind ``runner.`` calls - ``_ensure_manufacturer``,
    ``_ensure_inventory_item_role`` and ``_upsert_values_from_defaults`` - so
    unlike cables the preview runner's firewall covers them and this flag only
    has to handle the one branch that is not an upsert at all.

    That branch is why the sentinel exists. When ``dcim.module`` is enabled a
    module-native row is DELETED rather than upserted, and the comparison
    contract has no slot for a delete: the report computes drift as
    ``creates + updates`` and counts deletes separately. Calling it an update
    would double-count it against that separate accounting; calling it
    unchanged would be a confident zero; calling it rejected would report a
    perfectly good row as unusable. So the row says it cannot be compared and
    the caller declines the whole model, which keeps its upper bound.
    """
    from dcim.models import InventoryItem

    try:
        device = runner._get_device_by_name(row["device"])
    except ObjectDoesNotExist as exc:
        key = (row["device"],)
        if runner._dependency_failed("dcim.device", key):
            raise ForwardDependencySkipError(
                f"Skipping inventory item because dependency `dcim.device` failed for {key}.",
                model_string="dcim.inventoryitem",
                dependency="dcim.device",
                context={"device": row["device"], "name": row.get("name")},
                data=row,
            ) from exc
        raise ForwardSearchError(
            f"Unable to find device `{row['device']}` for inventory item `{row.get('name')}`.",
            model_string="dcim.inventoryitem",
            context={"device": row["device"], "name": row.get("name")},
            data=row,
        ) from exc
    if runner.sync.is_model_enabled(
        "dcim.module"
    ) and runner._is_module_native_inventory_row(row):
        if preview:
            return MODULE_NATIVE_ROW_NOT_COMPARABLE
        return None if delete_dcim_inventoryitem(runner, row) else False
    manufacturer = None
    if row.get("manufacturer"):
        manufacturer = runner._ensure_manufacturer(
            {"name": row["manufacturer"], "slug": row["manufacturer_slug"]}
        )
    role = runner._ensure_inventory_item_role(row)
    _, created = runner._upsert_values_from_defaults(
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
    if preview:
        if created:
            return "creates"
        return "updates" if runner.last_upsert_would_change else "unchanged"


def apply_dcim_module(runner, row, *, preview=False):
    """Apply the module, or - with ``preview`` - classify and write nothing.

    Every write on this path is behind a ``runner.`` call, so the firewall
    covers them: ``_ensure_module_bay`` (upserts a ModuleBay),
    ``_ensure_module_type`` (upserts a ModuleType and a Manufacturer beneath
    it) and ``_upsert_values_from_defaults``.

    That last one is the dangerous one, and the reason a returned-early preview
    would not have been enough on its own. It passes
    ``create_instance_attrs={"_adopt_components": True}``, and NetBox core's
    ``Module.save()`` then instantiates the module type's component templates -
    console ports, interfaces, power ports and the rest - as real rows on the
    device. The preview override never saves, so none of that happens, and
    ``create_instance_attrs`` is accepted and ignored precisely because it only
    matters to a save.

    An absent bay or module type classifies the row as a create rather than
    being resolved: a module cannot already exist in a bay NetBox does not
    have, or with a type it does not have.
    """
    from dcim.models import Module

    try:
        device = runner._get_device_by_name(row["device"])
    except ObjectDoesNotExist as exc:
        key = (row["device"],)
        if runner._dependency_failed("dcim.device", key):
            raise ForwardDependencySkipError(
                f"Skipping module because dependency `dcim.device` failed for {key}.",
                model_string="dcim.module",
                dependency="dcim.device",
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
            sample=device.name,
        )
        return False
    module_bay = runner._ensure_module_bay(device, row)
    module_type = runner._ensure_module_type(row)
    if preview and (module_bay is None or module_type is None):
        # The preview runner resolves rather than creates, so a `None` here
        # means NetBox has no such bay or type. The module cannot already exist
        # under one it does not have, which makes this unambiguously a create -
        # and short-circuiting avoids a coalesce lookup on a null dependency
        # matching some unrelated row.
        return "creates"
    blocking = _unadoptable_component_names(device, module_type)
    if blocking:
        # `_adopt_components` cannot cover this. NetBox builds its adoption
        # candidates as `device.<components>.filter(module__isnull=True)`, so a
        # component already claimed by ANOTHER module is invisible to it: the
        # template instantiates a second one with the same name and the database
        # rejects it (`dcim_consoleport_unique_device_name` and friends). That
        # failed the row, and a failed row blocks baseline promotion.
        #
        # Reported and skipped instead. The device keeps the module it already
        # has, the collision is visible, and the sync converges everything else.
        runner._record_aggregated_skip_warning(
            model_string="dcim.module",
            reason="component-claimed-by-another-module",
            warning_message=(
                f"Skipping module in bay `{module_bay.name}` on `{device.name}`: "
                f"its module type would create {len(blocking)} component(s) whose "
                "names are already used on the device by a different module, "
                "which NetBox cannot adopt."
            ),
            sample=device.name,
        )
        return False
    _, created = runner._upsert_values_from_defaults(
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
        # Forward syncs device interfaces (and other components) independently of
        # modules, so a module type's component templates collide by name with the
        # already-present interfaces. Adopt the existing components into the module
        # instead of recreating them, which would raise a unique-constraint
        # IntegrityError (dcim_interface_unique_device_name, etc.).
        create_instance_attrs={"_adopt_components": True},
    )
    if preview:
        if created:
            return "creates"
        return "updates" if runner.last_upsert_would_change else "unchanged"
