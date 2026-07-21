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
                context={"device": row["name"]},
                data=row,
            ) from exc
        raise ForwardDependencySkipError(
            f"Skipping {object_label} because device `{row['name']}` is not "
            "in the current NetBox branch.",
            model_string=model_string,
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


def apply_netbox_dlm_softwareversion(runner, row):
    # DeviceSoftware is authoritative for which versions belong in NetBox. The
    # catalog map only enriches versions that already have a device-scoped
    # basis, preventing versions from out-of-scope Forward devices appearing as
    # zero-device DLM rows.
    return (
        ensure_dlm_software_version(runner, row, with_dates=True, create=False) or False
    )


def apply_netbox_dlm_hardwarenotice(runner, row):
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
    notice, _ = runner._upsert_values_from_defaults(
        "netbox_dlm.hardwarenotice",
        HardwareNotice,
        values=values,
        coalesce_sets=[("device_type",)],
    )
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


def apply_netbox_dlm_devicesoftware(runner, row):
    _, _, device_software = ensure_dlm_device_software(runner, row)
    return device_software


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


def apply_netbox_dlm_cve(runner, row):
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
    cve, _ = runner._upsert_values_from_defaults(
        "netbox_dlm.cve",
        CVE,
        values=values,
        coalesce_sets=[("cve_id",)],
    )
    return cve


def apply_netbox_dlm_vulnerability(runner, row):
    Vulnerability = _dlm_model(runner, "Vulnerability", "netbox_dlm.vulnerability")
    # Ensure both required FK targets exist even when the cve / software-version
    # maps are not enabled for this sync. DeviceSoftware is ensured as well, so
    # a vulnerability-only import cannot leave a zero-device SoftwareVersion.
    device, software_version, _ = ensure_dlm_device_software(runner, row)
    cve = ensure_dlm_cve(runner, row)
    values = runner._model_field_values(
        Vulnerability,
        {"cve": cve, "software_version": software_version, "device": device},
    )
    vulnerability, _ = runner._upsert_values_from_defaults(
        "netbox_dlm.vulnerability",
        Vulnerability,
        values=values,
        coalesce_sets=[("cve", "software_version", "device")],
    )
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
