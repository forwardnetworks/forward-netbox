# Optional netbox-dlm (Device Lifecycle Management) integration.
#
# Maps Forward's end-of-life analysis to the netbox-dlm plugin's models:
#   - SoftwareVersion  <- device.platform.osSupport (per platform + OS version)
#   - HardwareNotice   <- device.platform.components[].support (per DeviceType)
#   - DeviceSoftware   <- device.platform.osVersion (one row per device)
#   - CVE              <- network.cveDatabase.cves (global catalog)
#   - Vulnerability    <- device.cveFindings (one row per device + CVE)
#
# netbox-dlm's README expects DeviceSoftware to be populated by external sync
# tooling; this adapter is that tooling. All writes go through the standard
# runner upsert/delete primitives so branch staging, diffs, and prune behave
# exactly like every other model.
from datetime import date

from django.core.exceptions import ObjectDoesNotExist
from django.db import transaction

from ..exceptions import ForwardDependencySkipError


def _parse_date(value):
    """ISO date string -> date, so values compare equal to stored DateFields
    (a str value never equals a date and would update the row every sync)."""
    if isinstance(value, date) or value in (None, ""):
        return value or None
    try:
        return date.fromisoformat(str(value))
    except ValueError:
        return None


def _parse_float(value):
    if value in (None, ""):
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _dlm_model(runner, model_name, model_string):
    return runner._optional_model("netbox_dlm", model_name, model_string)


def _lookup_platform(runner, row, model_string, object_label):
    from dcim.models import Platform

    slug = str(row.get("platform_slug") or "").strip()
    name = str(row.get("platform") or "").strip()
    platform = None
    if slug:
        platform = runner._get_unique_or_raise(Platform, {"slug": slug})
    if platform is None and name:
        platform = runner._get_unique_or_raise(Platform, {"name": name})
    if platform is None:
        raise ForwardDependencySkipError(
            f"Skipping {object_label} because platform `{name or slug}` is not "
            "in NetBox yet.",
            model_string=model_string,
            dependency="dcim.platform",
            context={"platform": name or slug},
            data=row,
        )
    return platform


def _lookup_device_type(runner, row, model_string, object_label):
    from dcim.models import DeviceType

    slug = str(row.get("device_type_slug") or "").strip()
    model = str(row.get("device_type") or "").strip()
    device_type = None
    if slug:
        device_type = runner._get_unique_or_raise(DeviceType, {"slug": slug})
    if device_type is None and model:
        device_type = runner._get_unique_or_raise(DeviceType, {"model": model})
    if device_type is None:
        raise ForwardDependencySkipError(
            f"Skipping {object_label} because device type `{model or slug}` is "
            "not in NetBox yet. Enable device-type sync; if you run the "
            "alias-aware device query, use the 'Forward DLM Hardware Notices "
            "with NetBox Aliases' map so notices look up the same name.",
            model_string=model_string,
            dependency="dcim.devicetype",
            context={"device_type": model or slug},
            data=row,
        )
    return device_type


def _lookup_device(runner, row, model_string, object_label):
    try:
        return runner._get_device_by_name(row["name"])
    except ObjectDoesNotExist as exc:
        key = (row["name"],)
        if runner._dependency_failed("dcim.device", key):
            raise ForwardDependencySkipError(
                f"Skipping {object_label} because dependency `dcim.device` "
                f"failed for {key}.",
                model_string=model_string,
                dependency="dcim.device",
                context={"device": row["name"]},
                data=row,
            ) from exc
        raise ForwardDependencySkipError(
            f"Skipping {object_label} because device `{row['name']}` is not "
            "in the current NetBox branch.",
            model_string=model_string,
            dependency="dcim.device",
            context={"device": row["name"]},
            data=row,
        ) from exc


def ensure_dlm_software_version(runner, row, *, with_dates=True, create=True):
    SoftwareVersion = _dlm_model(
        runner, "SoftwareVersion", "netbox_dlm.softwareversion"
    )
    platform = _lookup_platform(
        runner, row, "netbox_dlm.softwareversion", "DLM software version"
    )
    values = {
        "platform": platform,
        "version": str(row.get("version") or "").strip(),
    }
    if with_dates:
        end_of_support = _parse_date(row.get("end_of_support"))
        if end_of_support:
            values["end_of_support"] = end_of_support
        if row.get("documentation_url"):
            values["documentation_url"] = row["documentation_url"]
    values = runner._model_field_values(SoftwareVersion, values)
    if with_dates:
        if not create:
            existing = runner._get_unique_or_raise(
                SoftwareVersion,
                {"platform": platform, "version": values.get("version")},
            )
            if existing is None:
                return None
        software_version, _ = runner._upsert_values_from_defaults(
            "netbox_dlm.softwareversion",
            SoftwareVersion,
            values=values,
            coalesce_sets=[("platform", "version")],
        )
    else:
        # Create-if-missing only: never overwrite end-of-life dates the
        # versions map already applied.
        software_version, _ = runner._coalesce_update_or_create(
            SoftwareVersion,
            coalesce_lookups=[{"platform": platform, "version": values.get("version")}],
            create_values=values,
            update_values={},
        )
    return software_version


def _preview_outcome(runner, created):
    """Classify one upserted DLM row from what the shimmed upsert reported.

    Every DLM write goes through `_upsert_values_from_defaults` or
    `_coalesce_update_or_create`, both of which the preview runner overrides
    with a lookup that records whether the apply would have written. So all
    five of these functions classify identically and share this rather than
    hand-rolling it five times and drifting apart.
    """
    if created:
        return "creates"
    return "updates" if runner.last_upsert_would_change else "unchanged"


def apply_netbox_dlm_softwareversion(runner, row, *, preview=False):
    # DeviceSoftware is authoritative for which versions belong in NetBox. The
    # catalog map only enriches versions that already have a device-scoped
    # basis, preventing versions from out-of-scope Forward devices appearing as
    # zero-device DLM rows.
    existing = ensure_dlm_software_version(runner, row, with_dates=True, create=False)
    if preview:
        # `create=False` means the apply never creates this row: the catalogue
        # map only enriches versions that already have a device-scoped basis.
        # So an absent one is a row the apply declines, not a create.
        if existing is None:
            return False
        return _preview_outcome(runner, False)
    return existing or False


def apply_netbox_dlm_hardwarenotice(runner, row, *, preview=False):
    HardwareNotice = _dlm_model(runner, "HardwareNotice", "netbox_dlm.hardwarenotice")
    device_type = _lookup_device_type(
        runner, row, "netbox_dlm.hardwarenotice", "DLM hardware notice"
    )
    values = {"device_type": device_type}
    for field in ("end_of_support", "end_of_security_patches", "end_of_sw_releases"):
        parsed = _parse_date(row.get(field))
        if parsed:
            values[field] = parsed
    if row.get("documentation_url"):
        values["documentation_url"] = row["documentation_url"]
    values = runner._model_field_values(HardwareNotice, values)
    notice, created = runner._upsert_values_from_defaults(
        "netbox_dlm.hardwarenotice",
        HardwareNotice,
        values=values,
        coalesce_sets=[("device_type",)],
    )
    if preview:
        return _preview_outcome(runner, created)
    return notice


def ensure_dlm_device_software(runner, row):
    cache_key = (
        str(row.get("name") or "").strip(),
        str(row.get("platform_slug") or "").strip(),
        str(row.get("version") or "").strip(),
    )
    cache = getattr(runner, "_dlm_device_software_cache", None)
    if not isinstance(cache, dict):
        cache = runner._dlm_device_software_cache = {}
    if cache_key in cache:
        return cache[cache_key]

    DeviceSoftware = _dlm_model(runner, "DeviceSoftware", "netbox_dlm.devicesoftware")
    device = _lookup_device(
        runner, row, "netbox_dlm.devicesoftware", "DLM device software"
    )
    # The device-scoped map is authoritative for SoftwareVersion existence and
    # carries lifecycle dates when Forward has them. This keeps creation and the
    # DeviceSoftware association in the same transaction/branch.
    software_version = ensure_dlm_software_version(runner, row, with_dates=True)
    values = runner._model_field_values(
        DeviceSoftware,
        {"device": device, "software_version": software_version},
    )
    device_software, _ = runner._upsert_values_from_defaults(
        "netbox_dlm.devicesoftware",
        DeviceSoftware,
        values=values,
        coalesce_sets=[("device",)],
    )
    cache[cache_key] = (device, software_version, device_software)
    return cache[cache_key]


def apply_netbox_dlm_devicesoftware(runner, row, *, preview=False):
    _, software_version, device_software = ensure_dlm_device_software(runner, row)
    if preview:
        # The preview runner resolves where the apply creates, so a `None`
        # anywhere in the chain means NetBox does not have it and the row is a
        # create. `ensure_dlm_device_software` memoises its result per
        # (device, platform, version), so a repeated row is answered from the
        # cache exactly as it is in the apply.
        if software_version is None or device_software is None:
            return "creates"
        return _preview_outcome(runner, False)
    return device_software


def _lookup_inventory_item(runner, row):
    from dcim.models import InventoryItem

    device_name = str(row.get("device") or "").strip()
    item_name = str(row.get("inventory_item") or "").strip()
    try:
        device = runner._get_device_by_name(device_name)
    except ObjectDoesNotExist as exc:
        raise ForwardDependencySkipError(
            "Skipping DLM inventory item software because its device is not "
            "in the current NetBox branch.",
            model_string="netbox_dlm.inventoryitemsoftware",
            dependency="dcim.inventoryitem",
            context={"dependency": "dcim.inventoryitem"},
            data=row,
        ) from exc
    inventory_item = runner._get_unique_or_raise(
        InventoryItem, {"device": device, "name": item_name}
    )
    if inventory_item is None:
        raise ForwardDependencySkipError(
            "Skipping DLM inventory item software because the target inventory "
            "item is not in the current NetBox branch. Enable Forward CIMC "
            "Endpoint Inventory alongside this map.",
            model_string="netbox_dlm.inventoryitemsoftware",
            dependency="dcim.inventoryitem",
            context={"dependency": "dcim.inventoryitem"},
            data=row,
        )
    return inventory_item


def ensure_dlm_inventory_item_role_platform(runner, inventory_item, row):
    """Ensure the role-wide platform mapping required by InventoryItemSoftware."""
    role = inventory_item.role
    if role is None:
        raise ForwardDependencySkipError(
            "Skipping DLM inventory item software because the target inventory "
            "item has no role.",
            model_string="netbox_dlm.inventoryitemsoftware",
            dependency="dcim.inventoryitemrole",
            context={"dependency": "dcim.inventoryitemrole"},
            data=row,
        )
    cache_key = (role.pk, str(row.get("platform_slug") or "").strip())
    cache = getattr(runner, "_dlm_inventory_item_role_platform_cache", None)
    if not isinstance(cache, dict):
        cache = runner._dlm_inventory_item_role_platform_cache = {}
    if cache_key in cache:
        return cache[cache_key]

    platform = runner._ensure_platform(
        {"name": row["platform"], "slug": row["platform_slug"]}
    )
    InventoryItemRolePlatform = _dlm_model(
        runner,
        "InventoryItemRolePlatform",
        "netbox_dlm.inventoryitemroleplatform",
    )
    if platform is None:
        # Preview only: the real `_ensure_platform` upserts and never returns
        # None. An absent platform means the mapping cannot exist yet, and
        # looking it up by role alone would match the role's mapping to some
        # OTHER platform - the absent-parent trap the ACI and routing slices
        # both hit. Cache nothing: the answer is "create", per row.
        return None, None
    mapping, created = runner._upsert_values_from_defaults(
        "netbox_dlm.inventoryitemroleplatform",
        InventoryItemRolePlatform,
        values=runner._model_field_values(
            InventoryItemRolePlatform,
            {"role": role, "platform": platform},
        ),
        coalesce_sets=[("role",)],
    )
    cache[cache_key] = (platform, mapping)
    # The mapping's own verdict, for a preview of `inventoryitemroleplatform`.
    # Kept beside the cache rather than in it, because two callers unpack the
    # cached pair. A cache hit on a later row means the apply writes nothing
    # for that row, which is what "unchanged" says.
    outcomes = getattr(runner, "_dlm_inventory_item_role_platform_outcomes", None)
    if not isinstance(outcomes, dict):
        outcomes = runner._dlm_inventory_item_role_platform_outcomes = {}
    outcomes[cache_key] = (
        "creates"
        if mapping is None
        else (
            "updates"
            if getattr(runner, "last_upsert_would_change", False)
            else "unchanged"
        )
    )
    return cache[cache_key]


def apply_netbox_dlm_inventoryitemsoftware(runner, row, *, preview=False):
    """Record which firmware an inventory item runs, or classify it.

    The chain the DLM slice left unaudited: `_lookup_inventory_item` is two
    reads (`_get_device_by_name`, `_get_unique_or_raise`) that raise a
    dependency skip when absent; `ensure_dlm_inventory_item_role_platform` is
    a platform ensure and one upsert, both behind runner calls the preview
    overrides; `ensure_dlm_software_version` is the same upsert the
    `softwareversion` slice already previews. The leaf is one upsert keyed on
    the inventory item, so the leaf rule applies: a software version the row
    would create is `softwareversion`'s drift, and this row is an update if it
    exists (its version pointer changes) and a create if it does not.
    """
    InventoryItemSoftware = _dlm_model(
        runner, "InventoryItemSoftware", "netbox_dlm.inventoryitemsoftware"
    )
    # Resolve the opt-in parent first: disabled CIMC endpoint inventory must
    # count as a dependency skip, not create an unattached DLM catalogue row.
    inventory_item = _lookup_inventory_item(runner, row)
    platform, _ = ensure_dlm_inventory_item_role_platform(runner, inventory_item, row)
    if platform is None:
        # Preview only, see the ensure. The platform - and so the version - is
        # a create; this row is whatever it is without them.
        existing = runner._get_unique_or_raise(
            InventoryItemSoftware, {"inventory_item": inventory_item}
        )
        return "updates" if existing is not None else "creates"
    software_version = ensure_dlm_software_version(
        runner,
        {**row, "platform": platform.name, "platform_slug": platform.slug},
        with_dates=False,
    )
    inventory_item_software, created = runner._upsert_values_from_defaults(
        "netbox_dlm.inventoryitemsoftware",
        InventoryItemSoftware,
        values=runner._model_field_values(
            InventoryItemSoftware,
            {
                "inventory_item": inventory_item,
                "software_version": software_version,
            },
        ),
        coalesce_sets=[("inventory_item",)],
    )
    if preview:
        return _preview_outcome(runner, created)
    return inventory_item_software


def apply_netbox_dlm_inventoryitemroleplatform(runner, row, *, preview=False):
    """The mapping is an atomic side effect of InventoryItemSoftware rows."""
    inventory_item = _lookup_inventory_item(runner, row)
    _, mapping = ensure_dlm_inventory_item_role_platform(runner, inventory_item, row)
    if preview:
        role = inventory_item.role
        cache_key = (role.pk, str(row.get("platform_slug") or "").strip())
        # The apply writes each (role, platform) mapping ONCE per run and
        # answers every later row for the same key from its cache. The preview
        # reports the verdict once, on the first row, and "unchanged" for the
        # rest - whatever the first verdict was, including the absent-platform
        # case where the ensure never reaches its upsert or its cache.
        seen = getattr(runner, "_dlm_role_platform_preview_seen", None)
        if not isinstance(seen, set):
            seen = runner._dlm_role_platform_preview_seen = set()
        if cache_key in seen:
            return "unchanged"
        seen.add(cache_key)
        outcomes = getattr(runner, "_dlm_inventory_item_role_platform_outcomes", {})
        return outcomes.get(cache_key, "creates")
    return mapping


def ensure_dlm_cve(runner, row):
    """Create-if-missing CVE by unique cve_id. update_values is empty so this
    never clobbers the rich catalog row the cve map applies first (matches the
    ensure_dlm_software_version safety net used by device software)."""
    CVE = _dlm_model(runner, "CVE", "netbox_dlm.cve")
    cve_id = str(row.get("cve_id") or "").strip()
    cve, _ = runner._coalesce_update_or_create(
        CVE,
        coalesce_lookups=[{"cve_id": cve_id}],
        create_values={"cve_id": cve_id},
        update_values={},
    )
    return cve


def apply_netbox_dlm_cve(runner, row, *, preview=False):
    CVE = _dlm_model(runner, "CVE", "netbox_dlm.cve")
    values = {
        "cve_id": str(row.get("cve_id") or "").strip(),
        "name": str(row.get("name") or "").strip(),
        "description": str(row.get("description") or ""),
        "severity": str(row.get("severity") or "").strip(),
    }
    published_date = _parse_date(row.get("published_date"))
    if published_date is not None:
        values["published_date"] = published_date
    link = str(row.get("link") or "").strip()
    if link:
        values["link"] = link
    for field_name in ("cvss_score", "cvss_v2_score", "cvss_v3_score"):
        score = _parse_float(row.get(field_name))
        if score is not None:
            values[field_name] = score
    values = runner._model_field_values(CVE, values)
    cve, created = runner._upsert_values_from_defaults(
        "netbox_dlm.cve",
        CVE,
        values=values,
        coalesce_sets=[("cve_id",)],
    )
    if preview:
        return _preview_outcome(runner, created)
    return cve


def apply_netbox_dlm_vulnerability(runner, row, *, preview=False):
    """Record the finding, or - with ``preview`` - classify and write nothing.

    The deepest DLM chain: it ensures a DeviceSoftware (which itself ensures a
    SoftwareVersion), a CVE, then the Vulnerability, then adds the catalogue
    M2M. Every one of those writes goes through a ``runner.`` call the preview
    runner overrides - except the last.

    ``cve.affected_software.add`` is an M2M write reached directly, not through
    the runner, so the firewall neither sees nor stops it. Same shape as
    ``device.tags.add`` in the tagged-item path, and the same reason this
    function takes a flag: that write is skipped here, by name.
    """
    Vulnerability = _dlm_model(runner, "Vulnerability", "netbox_dlm.vulnerability")
    # Ensure both required FK targets exist even when the cve / software-version
    # maps are not enabled for this sync. DeviceSoftware is ensured as well, so
    # a vulnerability-only import cannot leave a zero-device SoftwareVersion.
    device, software_version, _ = ensure_dlm_device_software(runner, row)
    cve = ensure_dlm_cve(runner, row)
    if preview and (cve is None or software_version is None):
        # A finding cannot already exist against a CVE or a software version
        # NetBox does not have, so it is unambiguously a create - and
        # short-circuiting avoids a coalesce lookup on null FKs matching an
        # unrelated row.
        return "creates"
    values = runner._model_field_values(
        Vulnerability,
        {"cve": cve, "software_version": software_version, "device": device},
    )
    vulnerability, created = runner._upsert_values_from_defaults(
        "netbox_dlm.vulnerability",
        Vulnerability,
        values=values,
        coalesce_sets=[("cve", "software_version", "device")],
    )
    if preview:
        # The M2M below is NOT reflected in this verdict, deliberately. It is a
        # catalogue-level relation between CVE and SoftwareVersion, not part of
        # this row's identity, and `.add()` on an existing link is a no-op - so
        # counting a row as drifted because the link is missing would report
        # drift that the Vulnerability row itself does not have. The link is
        # repaired by the apply either way.
        return _preview_outcome(runner, created)
    # netbox-dlm exposes the catalog-level CVE <-> SoftwareVersion relation
    # separately from device-scoped Vulnerability instances. Forward's finding
    # supplies direct evidence for both; authoritative full workloads remove the
    # relation when the last in-scope finding disappears.
    cve.affected_software.add(software_version)
    return vulnerability


def delete_netbox_dlm_softwareversion(runner, row):
    SoftwareVersion = _dlm_model(
        runner, "SoftwareVersion", "netbox_dlm.softwareversion"
    )
    from dcim.models import Platform

    platform = runner._get_unique_or_raise(
        Platform, {"slug": str(row.get("platform_slug") or "").strip()}
    )
    if platform is None:
        return False
    return runner._delete_by_coalesce(
        SoftwareVersion,
        [{"platform": platform, "version": str(row.get("version") or "").strip()}],
    )


def delete_netbox_dlm_hardwarenotice(runner, row):
    HardwareNotice = _dlm_model(runner, "HardwareNotice", "netbox_dlm.hardwarenotice")
    from dcim.models import DeviceType

    device_type = runner._get_unique_or_raise(
        DeviceType, {"slug": str(row.get("device_type_slug") or "").strip()}
    )
    if device_type is None:
        return False
    return runner._delete_by_coalesce(HardwareNotice, [{"device_type": device_type}])


def delete_netbox_dlm_devicesoftware(runner, row):
    DeviceSoftware = _dlm_model(runner, "DeviceSoftware", "netbox_dlm.devicesoftware")
    device = runner._lookup_device_by_name(row.get("name"))
    if device is None:
        return False
    return runner._delete_by_coalesce(DeviceSoftware, [{"device": device}])


def delete_netbox_dlm_inventoryitemsoftware(runner, row):
    InventoryItemSoftware = _dlm_model(
        runner, "InventoryItemSoftware", "netbox_dlm.inventoryitemsoftware"
    )
    try:
        inventory_item = _lookup_inventory_item(runner, row)
    except ForwardDependencySkipError:
        return False
    return runner._delete_by_coalesce(
        InventoryItemSoftware, [{"inventory_item": inventory_item}]
    )


def delete_netbox_dlm_inventoryitemroleplatform(runner, row):
    # No query targets this side-effect model, so source deletion never owns
    # the role-wide mapping. Keeping it prevents one item disappearing from
    # removing the platform contract required by other items of that role.
    return False


def delete_netbox_dlm_cve(runner, row):
    CVE = _dlm_model(runner, "CVE", "netbox_dlm.cve")
    cve_id = str(row.get("cve_id") or "").strip()
    cve = runner._get_unique_or_raise(CVE, {"cve_id": cve_id})
    if cve is None:
        return False
    # Vulnerabilities are authoritative device findings and block deletion.
    # affected_software is derived from those findings, so it must not preserve
    # an otherwise orphaned CVE indefinitely.
    if cve.vulnerabilities.exists():
        return False
    cve.affected_software.clear()
    return runner._delete_by_coalesce(CVE, [{"cve_id": cve_id}])


def delete_netbox_dlm_vulnerability(runner, row):
    Vulnerability = _dlm_model(runner, "Vulnerability", "netbox_dlm.vulnerability")
    from dcim.models import Platform

    CVE = _dlm_model(runner, "CVE", "netbox_dlm.cve")
    SoftwareVersion = _dlm_model(
        runner, "SoftwareVersion", "netbox_dlm.softwareversion"
    )
    cve = runner._get_unique_or_raise(
        CVE, {"cve_id": str(row.get("cve_id") or "").strip()}
    )
    platform = runner._get_unique_or_raise(
        Platform, {"slug": str(row.get("platform_slug") or "").strip()}
    )
    software_version = None
    if platform is not None:
        software_version = runner._get_unique_or_raise(
            SoftwareVersion,
            {"platform": platform, "version": str(row.get("version") or "").strip()},
        )
    device = runner._lookup_device_by_name(row.get("name"))
    if cve is None or software_version is None or device is None:
        return False
    with transaction.atomic(using=Vulnerability.objects.db):
        deleted = runner._delete_by_coalesce(
            Vulnerability,
            [{"cve": cve, "software_version": software_version, "device": device}],
        )
        if (
            deleted
            and not Vulnerability.objects.filter(
                cve=cve,
                software_version=software_version,
            ).exists()
        ):
            cve.affected_software.remove(software_version)
        return deleted
