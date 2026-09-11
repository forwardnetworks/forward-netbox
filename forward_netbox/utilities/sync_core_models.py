from dcim.models import DeviceRole
from dcim.models import DeviceType
from dcim.models import Manufacturer
from dcim.models import Platform
from dcim.models import Site


def delete_dcim_site(runner, row):
    return runner._delete_by_coalesce(
        Site,
        [
            runner._coalesce_lookup(row, "slug"),
            runner._coalesce_lookup(row, "name"),
        ],
    )


def delete_dcim_manufacturer(runner, row):
    return runner._delete_by_coalesce(
        Manufacturer,
        [
            runner._coalesce_lookup(row, "slug"),
            runner._coalesce_lookup(row, "name"),
        ],
    )


def delete_dcim_devicerole(runner, row):
    return runner._delete_by_coalesce(
        DeviceRole,
        [
            runner._coalesce_lookup(row, "slug"),
            runner._coalesce_lookup(row, "name"),
        ],
    )


def delete_dcim_platform(runner, row):
    return runner._delete_by_coalesce(
        Platform,
        [
            runner._coalesce_lookup(row, "slug"),
            runner._coalesce_lookup(row, "name"),
        ],
    )


def delete_dcim_devicetype(runner, row):
    manufacturer = None
    if row.get("manufacturer_slug"):
        manufacturer = runner._get_unique_or_raise(
            Manufacturer, {"slug": row["manufacturer_slug"]}
        )
    if manufacturer is None and row.get("manufacturer"):
        manufacturer = runner._get_unique_or_raise(
            Manufacturer, {"name": row["manufacturer"]}
        )
    if manufacturer is None:
        return False

    return runner._delete_by_coalesce(
        DeviceType,
        [
            (
                {"manufacturer": manufacturer, "slug": row["slug"]}
                if row.get("slug")
                else {}
            ),
            (
                {"manufacturer": manufacturer, "model": row["model"]}
                if row.get("model")
                else {}
            ),
        ],
    )


def apply_dcim_site(runner, row):
    runner._ensure_site(row)


def apply_dcim_manufacturer(runner, row):
    runner._ensure_manufacturer(row)


def _leaf_outcome(runner, obj):
    """One flat row, one object: created, changed, or unchanged."""
    if obj is None:
        return "creates"
    return "updates" if runner.last_upsert_would_change else "unchanged"


def apply_dcim_platform(runner, row, *, preview=False):
    if preview:
        # The preview runner's `_ensure_platform` is a bare lookup (the device
        # classification must never create a platform), so classify through
        # the plan and the runner's read-only upsert instead.
        from dcim.models import Platform

        lookups, create_values, update_values = runner._platform_upsert_plan(
            row, manufacturer_authoritative=True
        )
        platform, _ = runner._coalesce_upsert(
            "dcim.platform",
            Platform,
            coalesce_lookups=lookups,
            create_values=create_values,
            update_values=update_values,
        )
        return _leaf_outcome(runner, platform)
    return runner._ensure_platform(row, manufacturer_authoritative=True)


def apply_dcim_devicerole(runner, row, *, preview=False):
    role = runner._ensure_role(row)
    if preview:
        return _leaf_outcome(runner, role)
    return role


def apply_dcim_devicetype(runner, row):
    runner._ensure_device_type(row)
