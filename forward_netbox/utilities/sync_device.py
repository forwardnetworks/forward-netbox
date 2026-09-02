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

    lookup = {"name": name, "site": site} if site is not None else {"name": name}
    from netbox_branching.contextvars import active_branch

    branch = active_branch.get()
    if branch is not None:
        from django.db import transaction
        from netbox.context import current_request

        from ..models import ForwardDeviceIdentity
        from .apply_engine_bulk import emit_branch_object_changes
        from .bulk_delete import collector_delete_without_model_signals

        device = runner._get_unique_or_raise(Device, lookup)
        if device is None:
            return False
        from django.db.models.deletion import ProtectedError

        from .sync_primitives import protected_delete_skip

        try:
            with transaction.atomic(using=branch.connection_name):
                if not emit_branch_object_changes((), (), [device]):
                    raise RuntimeError(
                        "Branch device deletion requires an attributed request context."
                    )
                request_token = current_request.set(None)
                try:
                    collector_delete_without_model_signals(
                        Device.objects.using(branch.connection_name).filter(
                            pk=device.pk
                        ),
                        signal_free_models=frozenset(),
                        # Identity provenance lives in main until merge
                        # finalization. Ignore only that sidecar while
                        # collecting branch-local cascades; every scope,
                        # parent, and operator-owned relation retains normal
                        # PROTECT.
                        ignored_related_models={ForwardDeviceIdentity},
                    )
                finally:
                    current_request.reset(request_token)
        except ProtectedError as exc:
            # The branch collector raises straight past `delete_by_coalesce`,
            # so this was the one delete path whose ProtectedError reached the
            # generic handler and was recorded as a FAILED row. A failed row
            # blocks baseline promotion permanently, which puts the whole
            # drift report back to "Not measured" - a strictly worse outcome
            # than the delete simply not happening. A customer hit it on 2.8.2:
            # `dcim.device row processing failed (ProtectedError).`, naming
            # neither what held the device nor which device it was.
            raise protected_delete_skip(
                Device, exc, obj=device, context=lookup
            ) from exc
        return True

    return runner._delete_by_coalesce(
        Device,
        [lookup],
    )


def delete_dcim_virtualchassis(runner, row):
    from dcim.models import VirtualChassis

    name = row.get("vc_name") or row.get("name")
    if not name:
        return False
    return runner._delete_by_coalesce(VirtualChassis, [{"name": name}])


def apply_dcim_virtualchassis(runner, row, *, preview=False):
    """Ensure the chassis, then the member device's position in it.

    ``preview`` classifies without writing. This is the production path for
    the model: syncs run in a branch, and the bulk engine defers to this
    adapter whenever a branch is active, so the preview mirrors THIS function's
    decisions rather than the bulk path's. A chassis the row would create is a
    create; a member whose chassis or position differs is an update; a member
    already in place takes the chassis's own upsert verdict. The two-phase
    shape that made the bulk path decline to answer - the second phase reads
    the first's pk - does not arise here, because an absent chassis means the
    membership cannot exist yet and the row is a create outright.
    """
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
    if preview and vc is None:
        # Chassis absent, so the apply would create it - and any membership
        # below it. One create under this model.
        return "creates"
    if row.get("device"):
        try:
            device = runner._get_device_by_name(row["device"])
        except ObjectDoesNotExist as exc:
            key = (row["device"],)
            if runner._dependency_failed("dcim.device", key):
                raise ForwardDependencySkipError(
                    f"Skipping virtual chassis assignment because dependency `dcim.device` failed for {key}.",
                    model_string="dcim.virtualchassis",
                    dependency="dcim.device",
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
            if preview:
                return "updates" if runner.last_upsert_would_change else "unchanged"
            return vc
        if preview:
            # The membership write below is a direct ORM update - the one write
            # in this function not behind a runner call - so the preview must
            # answer before it.
            return "updates"
        Device.objects.filter(pk=device.pk).update(**defaults)
    if preview:
        return "updates" if runner.last_upsert_would_change else "unchanged"
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

    device, created = runner._upsert_values_from_defaults(
        "dcim.device",
        Device,
        values=defaults,
        coalesce_sets=runner._coalesce_sets_for(
            "dcim.device",
            [("name", "site")],
        ),
    )
    record_device_identity_candidate(runner, device)
    if not created and getattr(device, "pk", None) is not None:
        # This path CAN move a device between sites: an operator-configured
        # name-only coalesce matches by name and the upsert saves the new
        # site, and `save()` revalidates nothing below the device. Revalidate
        # its interfaces here, at the point the state is made.
        from .interface_vlan_audit import clear_cross_site_untagged_vlans

        clear_cross_site_untagged_vlans(runner, [device.pk])

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
