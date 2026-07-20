from django.core.exceptions import ObjectDoesNotExist

from ..exceptions import ForwardDependencySkipError
from ..exceptions import ForwardSearchError
from ..exceptions import ForwardSyncDataError


def delete_dcim_device(runner, row):
    from dcim.models import Device
    from dcim.models import Site

    name = str(row.get("name") or "").strip()
    if not name:
        return False

    site = None
    if row.get("site_slug"):
        site = runner._get_unique_or_raise(Site, {"slug": row["site_slug"]})
    elif row.get("site"):
        site = runner._get_unique_or_raise(Site, {"name": row["site"]})

    if (row.get("site_slug") or row.get("site")) and site is None:
        return False

    return runner._delete_by_coalesce(
        Device,
        [{"name": name, "site": site} if site is not None else {"name": name}],
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
        return False

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
            device = runner._get_device_by_name(row["device"])
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
        if (
            device.virtual_chassis_id == vc.pk
            and device.vc_position == row["vc_position"]
        ):
            return vc
        Device.objects.filter(pk=device.pk).update(**defaults)
    return vc


def _scope_tags_enabled(runner):
    """True when the opt-in ``apply_device_scope_tags`` source parameter is set."""
    source_parameters = getattr(runner.sync.source, "parameters", None) or {}
    return bool(source_parameters.get("apply_device_scope_tags"))


def record_device_identity_candidate(runner, device):
    candidates = getattr(runner, "_device_identity_candidates", None)
    if candidates is None:
        candidates = runner._device_identity_candidates = set()
    candidates.add((str(device.name or "").strip(), device.pk))


def _ensure_scope_tag(runner, name):
    """Resolve (and cache) the NetBox Tag for one include-tag name, ensuring it
    exists. Returns None for an unslugifiable name."""
    cache = getattr(runner, "_scope_tag_objs", None)
    if cache is None:
        cache = runner._scope_tag_objs = {}
    if name in cache:
        return cache[name]

    from django.utils.text import slugify
    from extras.models import Tag

    slug = slugify(name) or slugify(name.replace(".", "-"))
    if not slug:
        cache[name] = None
        return None
    tag, _ = runner._upsert_values_from_defaults(
        "extras.taggeditem",
        Tag,
        values={"name": name, "slug": slug, "color": "9e9e9e"},
        coalesce_sets=[("slug",), ("name",)],
    )
    cache[name] = tag
    return tag


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
            },
            manufacturer_authoritative=bool(
                row.get("platform_manufacturer_authoritative")
            ),
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

    device, _ = runner._upsert_values_from_defaults(
        "dcim.device",
        Device,
        values=defaults,
        coalesce_sets=runner._coalesce_sets_for(
            "dcim.device",
            [("name", "site")],
        ),
    )
    record_device_identity_candidate(runner, device)

    if _scope_tags_enabled(runner):
        from .sync_interface import _device_add_tag

        # Stage positive assignments in the inventory branch. Removals are
        # materialized on main after merge from the union of generation-stamped
        # sync claims, so a stale branch can never remove another sync's tag.
        matched_names = runner._scope_matched_tags.get(row["name"], [])
        wanted = {
            tag
            for tag in (_ensure_scope_tag(runner, name) for name in matched_names)
            if tag is not None
        }
        for tag in wanted:
            _device_add_tag(runner, device, tag)
    return True
