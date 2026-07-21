from dcim.models import Device
from dcim.models import DeviceType
from dcim.models import Platform
from django.apps import apps
from django.db import DatabaseError
from django.db.models import Exists
from django.db.models import OuterRef
from django.db.models import Q

DEFAULT_SAMPLE_LIMIT = 25


def _stale_endpoint_device_types(*, include_samples, sample_limit):
    candidates = (
        DeviceType.objects.annotate(
            _has_devices=Exists(Device.objects.filter(device_type_id=OuterRef("pk")))
        )
        .filter(_has_devices=False)
        .filter(
            Q(manufacturer__slug__iexact="opengear", model__contains=",")
            | Q(
                manufacturer__slug__iexact="avocent",
                model__icontains=" - version:",
            )
        )
        .order_by("manufacturer__name", "model")
    )
    result = {"candidate_count": candidates.count()}
    if include_samples:
        result["sample"] = list(
            candidates.values("manufacturer__name", "model")[:sample_limit]
        )
    return result


def _empty_dlm_summary(*, status, reason=""):
    return {
        "available": False,
        "status": status,
        "reason": reason,
        "software_versions": {
            "total": None,
            "without_devices": None,
            "protected_without_devices": None,
            "unreferenced_without_devices": None,
        },
        "cves": {
            "total": None,
            "with_vulnerabilities": None,
            "with_affected_software": None,
            "unlinked": None,
        },
    }


def _dlm_summary(*, include_samples, sample_limit):
    if not apps.is_installed("netbox_dlm"):
        return _empty_dlm_summary(status="not_installed")

    try:
        SoftwareVersion = apps.get_model("netbox_dlm", "SoftwareVersion")
        DeviceSoftware = apps.get_model("netbox_dlm", "DeviceSoftware")
        SoftwareImageFile = apps.get_model("netbox_dlm", "SoftwareImageFile")
        ValidatedSoftware = apps.get_model("netbox_dlm", "ValidatedSoftware")
        CVE = apps.get_model("netbox_dlm", "CVE")
        Vulnerability = apps.get_model("netbox_dlm", "Vulnerability")
    except LookupError:
        return _empty_dlm_summary(status="model_unavailable")

    try:
        without_devices = SoftwareVersion.objects.annotate(
            _has_devices=Exists(
                DeviceSoftware.objects.filter(software_version_id=OuterRef("pk"))
            )
        ).filter(_has_devices=False)
        classified = without_devices.annotate(
            _has_cves=Exists(CVE.objects.filter(affected_software=OuterRef("pk"))),
            _has_vulnerabilities=Exists(
                Vulnerability.objects.filter(software_version_id=OuterRef("pk"))
            ),
            _has_image_files=Exists(
                SoftwareImageFile.objects.filter(software_version_id=OuterRef("pk"))
            ),
            _has_validated_rules=Exists(
                ValidatedSoftware.objects.filter(software_version_id=OuterRef("pk"))
            ),
        )
        unreferenced = classified.filter(
            _has_cves=False,
            _has_vulnerabilities=False,
            _has_image_files=False,
            _has_validated_rules=False,
        )
        unreferenced_count = unreferenced.count()
        without_devices_count = without_devices.count()

        cves = CVE.objects.annotate(
            _has_vulnerabilities=Exists(
                Vulnerability.objects.filter(cve_id=OuterRef("pk"))
            ),
            _has_affected_software=Exists(
                SoftwareVersion.objects.filter(cves=OuterRef("pk"))
            ),
        )
        dlm = {
            "available": True,
            "status": "available",
            "reason": "",
            "software_versions": {
                "total": SoftwareVersion.objects.count(),
                "without_devices": without_devices_count,
                "protected_without_devices": (
                    without_devices_count - unreferenced_count
                ),
                "unreferenced_without_devices": unreferenced_count,
            },
            "cves": {
                "total": cves.count(),
                "with_vulnerabilities": cves.filter(_has_vulnerabilities=True).count(),
                "with_affected_software": cves.filter(
                    _has_affected_software=True
                ).count(),
                "unlinked": cves.filter(
                    _has_vulnerabilities=False,
                    _has_affected_software=False,
                ).count(),
            },
        }
        if include_samples:
            sample_fields = (
                "platform__name",
                "version",
                "_has_cves",
                "_has_vulnerabilities",
                "_has_image_files",
                "_has_validated_rules",
            )
            protected_sample = []
            protected = classified.exclude(
                _has_cves=False,
                _has_vulnerabilities=False,
                _has_image_files=False,
                _has_validated_rules=False,
            )
            for row in protected.order_by("platform__name", "version").values(
                *sample_fields
            )[:sample_limit]:
                reasons = [
                    label
                    for field, label in (
                        ("_has_cves", "CVE"),
                        ("_has_vulnerabilities", "vulnerability"),
                        ("_has_image_files", "image file"),
                        ("_has_validated_rules", "validated rule"),
                    )
                    if row.pop(field)
                ]
                row["protected_by"] = ", ".join(reasons)
                protected_sample.append(row)
            dlm["software_versions"]["protected_sample"] = protected_sample
            dlm["software_versions"]["unreferenced_sample"] = list(
                unreferenced.order_by("platform__name", "version").values(
                    "platform__name", "version"
                )[:sample_limit]
            )
        return dlm
    except DatabaseError:
        return _empty_dlm_summary(
            status="database_unavailable",
            reason="netbox_dlm tables are not migrated or are unavailable",
        )


def compute_upgrade_reconciliation(
    *, include_samples=False, sample_limit=DEFAULT_SAMPLE_LIMIT
):
    """Return local, read-only post-upgrade catalog reconciliation evidence.

    Global NetBox catalog objects do not record Forward-source ownership. This
    function deliberately classifies possible stale artifacts but never
    deletes them. Samples are opt-in so support bundles contain aggregate
    evidence without customer inventory values.
    """
    sample_limit = max(1, min(int(sample_limit), DEFAULT_SAMPLE_LIMIT))
    return {
        "read_only": True,
        "scope": "global_netbox_catalog",
        "platforms": {
            "total": Platform.objects.count(),
            "with_manufacturer": Platform.objects.filter(
                manufacturer__isnull=False
            ).count(),
            "without_manufacturer": Platform.objects.filter(
                manufacturer__isnull=True
            ).count(),
        },
        "stale_endpoint_device_types": _stale_endpoint_device_types(
            include_samples=include_samples,
            sample_limit=sample_limit,
        ),
        "dlm": _dlm_summary(
            include_samples=include_samples,
            sample_limit=sample_limit,
        ),
    }
