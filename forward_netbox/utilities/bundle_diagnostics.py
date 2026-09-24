"""The support-bundle half of every question a customer investigation asked.

A deployment's uncovered count jumped after an upgrade, endpoint devices began
arriving with full attributes, eleven models showed pending removals, and a few
rows failed on primary-IP pointers. Answering that took a hand-written shell
script run on the customer's NetBox, because the bundle - the one artifact that
exists so nobody has to ask for screenshots or a shell - carried none of it:
no breakdown of what the uncovered devices are, no timeline of when they became
uncovered, no view of which query each map actually runs, nothing about the rows
behind an ingestion issue.

This module is that script's content, redacted to the bundle's rule: counts,
primary keys, catalog names (manufacturer, platform, role, device type) and
shapes; never device names, site names or tag values. Every section is computed
with database aggregation and is independent - one failing section records its
exception class and the rest still ship.

`test_bundle_diagnostics_parity` pins the section list, so a future
investigation that needs a new question adds a section here rather than another
script.
"""

from __future__ import annotations

import hashlib
import re
from datetime import timedelta

from django.db.models import Count
from django.db.models.functions import TruncDate
from django.utils import timezone
from rq.timeouts import JobTimeoutException

WINDOW_DAYS = 3
TREND_JOBS = 60
UNCOVERED_TAG = "forward-uncovered"
# Change history records tags by NAME, not slug (NetBox `serialize_object`).
UNCOVERED_TAG_MARKS = {"forward-uncovered", "Forward Uncovered"}
CONSOLE_VENDOR_WORDS = ("avocent", "opengear", "cyclades", "alterpath", "vertiv")

SECTIONS = (
    "uncovered",
    "uncovered_tag_timeline",
    "scope_trend",
    "device_renames",
    "recent_devices",
    "console_servers",
    "nqe_map_bindings",
    "issue_references",
    "inventory_items",
    "duplicate_device_names",
)


def _counts(qs, field, limit=40):
    return [
        {
            "value": row[field] if row[field] not in (None, "") else "-",
            "count": row["n"],
        }
        for row in qs.values(field)
        .annotate(n=Count("pk", distinct=True))
        .order_by("-n")[:limit]
    ]


def _device_breakdown(qs):
    return {
        "count": qs.count(),
        "by_manufacturer": _counts(qs, "device_type__manufacturer__name"),
        "by_platform": _counts(qs, "platform__name"),
        "by_role": _counts(qs, "role__name"),
        "by_device_type": _counts(qs, "device_type__model"),
        "by_status": _counts(qs, "status"),
        # Site NAMES can carry street addresses; the pk locates it on request.
        "by_site_pk": _counts(qs, "site_id"),
        "by_created_day": [
            {"day": str(row["day"]), "count": row["n"]}
            for row in qs.annotate(day=TruncDate("created"))
            .values("day")
            .annotate(n=Count("pk", distinct=True))
            .order_by("day")
        ],
    }


def _uncovered(sync):
    from dcim.models import Device

    from ..models import ForwardDeviceIdentity

    qs = Device.objects.filter(tags__slug=UNCOVERED_TAG).distinct()
    ids = list(qs.values_list("pk", flat=True))
    bound = ForwardDeviceIdentity.objects.filter(device_id__in=ids)
    this_sync = set(bound.filter(sync=sync).values_list("device_id", flat=True))
    any_sync = set(bound.values_list("device_id", flat=True))
    out = _device_breakdown(Device.objects.filter(pk__in=ids))
    out["identity"] = {
        "bound_to_this_sync": len(this_sync),
        "bound_to_other_sync_only": len(any_sync - this_sync),
        "unbound": len(set(ids) - any_sync),
    }
    return out


def _tag_slugs(data):
    tags = (data or {}).get("tags") or []
    out = set()
    for tag in tags:
        if isinstance(tag, dict):
            out.add(str(tag.get("slug") or tag.get("name") or ""))
        else:
            out.add(str(tag))
    return out


def _uncovered_tag_timeline(sync):
    from core.models import ObjectChange
    from dcim.models import Device
    from django.contrib.contenttypes.models import ContentType

    since = timezone.now() - timedelta(days=WINDOW_DAYS)
    ct = ContentType.objects.get_for_model(Device)
    gained, lost = {}, {}
    rows = (
        ObjectChange.objects.filter(changed_object_type=ct, time__gte=since)
        .values_list("time", "prechange_data", "postchange_data")
        .iterator(chunk_size=2000)
    )
    for time, pre, post in rows:
        before, after = _tag_slugs(pre), _tag_slugs(post)
        hour = time.strftime("%Y-%m-%d %H:00")
        if UNCOVERED_TAG_MARKS & (after - before):
            gained[hour] = gained.get(hour, 0) + 1
        if UNCOVERED_TAG_MARKS & (before - after):
            lost[hour] = lost.get(hour, 0) + 1
    return {
        "window_days": WINDOW_DAYS,
        "gained_by_hour": sorted(gained.items()),
        "lost_by_hour": sorted(lost.items()),
    }


def _scope_trend(sync):
    from core.choices import JobStatusChoices
    from core.models import Job
    from django.contrib.contenttypes.models import ContentType

    from ..models import ForwardSync
    from .scope_reconciliation import stored_scope_report

    ct = ContentType.objects.get_for_model(ForwardSync)
    out = []
    jobs = Job.objects.filter(
        object_type=ct, object_id=sync.pk, status=JobStatusChoices.STATUS_COMPLETED
    ).order_by("-completed")[: TREND_JOBS * 3]
    for job in jobs:
        payload, _generated_at, error = stored_scope_report(job)
        if not payload and not error:
            continue
        out.append(
            {
                "job_pk": job.pk,
                "job": job.name,
                "completed": job.completed.isoformat() if job.completed else None,
                "error_class": error.split(":", 1)[0][:80] if error else "",
                "counts": {
                    key: value
                    for key, value in (payload or {}).items()
                    if isinstance(value, (int, float)) and not isinstance(value, bool)
                },
            }
        )
        if len(out) >= TREND_JOBS:
            break
    return out


def _device_renames(sync):
    from core.models import ObjectChange
    from dcim.models import Device
    from django.contrib.contenttypes.models import ContentType

    since = timezone.now() - timedelta(days=WINDOW_DAYS)
    ct = ContentType.objects.get_for_model(Device)
    total, case_only, by_hour = 0, 0, {}
    for time, pre, post in (
        ObjectChange.objects.filter(changed_object_type=ct, time__gte=since)
        .values_list("time", "prechange_data", "postchange_data")
        .iterator(chunk_size=2000)
    ):
        before = (pre or {}).get("name")
        after = (post or {}).get("name")
        if before and after and before != after:
            total += 1
            if before.lower() == after.lower():
                case_only += 1
            hour = time.strftime("%Y-%m-%d %H:00")
            by_hour[hour] = by_hour.get(hour, 0) + 1
    return {
        "window_days": WINDOW_DAYS,
        "renamed": total,
        # Renames that only changed letter case: the device-identity fix aligns
        # a stored name to Forward's spelling, and this says how many it did.
        "case_only": case_only,
        "by_hour": sorted(by_hour.items()),
    }


def _recent_devices(sync):
    from dcim.models import Device

    since = timezone.now() - timedelta(days=WINDOW_DAYS)
    out = _device_breakdown(Device.objects.filter(created__gte=since))
    out["window_days"] = WINDOW_DAYS
    return out


def _console_servers(sync):
    from dcim.models import Device
    from django.db.models import Q

    match = Q()
    for word in CONSOLE_VENDOR_WORDS:
        match |= Q(device_type__manufacturer__name__icontains=word) | Q(
            platform__name__icontains=word
        )
    qs = Device.objects.filter(match).distinct()
    out = _device_breakdown(qs)
    annotated = qs.annotate(iface_n=Count("interfaces", distinct=True))
    out["with_interfaces"] = annotated.filter(iface_n__gt=0).count()
    out["without_interfaces"] = annotated.filter(iface_n=0).count()
    out["uncovered_tagged"] = qs.filter(tags__slug=UNCOVERED_TAG).count()
    return out


def _leaf(path):
    return str(path or "").rstrip("/").rsplit("/", 1)[-1]


def _sha(value):
    return hashlib.sha256(str(value or "").encode("utf-8")).hexdigest() if value else ""


def _nqe_map_bindings(sync):
    from ..models import ForwardNQEMap

    out = []
    enabled_models = None
    try:
        enabled_models = {
            m.model_string
            for m in sync.get_maps()
            if sync.is_model_enabled(m.model_string)
        }
    except JobTimeoutException:
        raise
    except Exception:  # noqa: BLE001 - the binding list is still useful without it
        enabled_models = None
    for m in ForwardNQEMap.objects.select_related("netbox_model").order_by("pk"):
        drift = getattr(m, "last_live_drift", None) or {}
        out.append(
            {
                "pk": m.pk,
                "map": m.name,
                "model": m.model_string,
                "enabled": bool(m.enabled),
                "model_enabled_on_sync": (
                    m.model_string in enabled_models
                    if enabled_models is not None
                    else None
                ),
                "built_in": bool(m.built_in),
                "execution_mode": getattr(m, "execution_mode", ""),
                "query_id": m.query_id or "",
                "commit_id": m.commit_id or "",
                # A repository path embeds the org's own folder names.
                "query_path_leaf": _leaf(m.query_path),
                "query_path_sha256": _sha(m.query_path),
                # A raw query can carry customer names in literals; its identity
                # and size answer "is this the stock query" without the text.
                "raw_query_sha256": _sha(m.query),
                "raw_query_length": len(m.query or ""),
                "parameter_keys": sorted((m.parameters or {}).keys()),
                "live_drift": {
                    "checked_at": (
                        m.last_live_drift_at.isoformat()
                        if getattr(m, "last_live_drift_at", None)
                        else None
                    ),
                    "status": drift.get("status"),
                    "severity": drift.get("severity"),
                    "parameter_signature_matches": drift.get(
                        "parameter_signature_matches"
                    ),
                    "missing_parameters": drift.get("missing_parameters"),
                    "unexpected_parameters": drift.get("unexpected_parameters"),
                    "source_matches_bundled": drift.get("source_matches_bundled"),
                },
            }
        )
    return out


def _issue_references(sync):
    from dcim.models import Device
    from ipam.models import VRF

    from ..models import ForwardDeviceIdentity

    ingestion = sync.last_ingestion
    if ingestion is None:
        return {"ingestion_pk": None, "devices": [], "vrfs": []}
    device_pks, vrf_pks = set(), set()
    for model, message, raw in ingestion.issues.values_list(
        "model", "message", "raw_data"
    ):
        pks = {int(pk) for pk in re.findall(r"#(\d+)", message or "")}
        pks |= {int(pk) for pk in re.findall(r"pk (\d+)", message or "")}
        if isinstance(raw, dict) and raw.get("netbox_pk"):
            try:
                pks.add(int(raw["netbox_pk"]))
            except (TypeError, ValueError):
                pass
        if (model or "") == "ipam.vrf":
            vrf_pks |= pks
        elif (model or "").startswith(("dcim.", "ipam.ipaddress")):
            device_pks |= pks
    identities = {}
    for device_id, sync_id in ForwardDeviceIdentity.objects.filter(
        device_id__in=device_pks
    ).values_list("device_id", "sync_id"):
        identities.setdefault(device_id, set()).add(sync_id)
    devices = []
    for row in Device.objects.filter(pk__in=device_pks).values(
        "pk",
        "status",
        "created",
        "site_id",
        "role__name",
        "platform__name",
        "device_type__manufacturer__name",
        "primary_ip4_id",
        "primary_ip6_id",
        "oob_ip_id",
    ):
        bound = identities.get(row["pk"], set())
        devices.append(
            {
                **{k: v for k, v in row.items() if k != "created"},
                "created": row["created"].isoformat() if row["created"] else None,
                "identity_this_sync": sync.pk in bound,
                "identity_other_sync": bool(bound - {sync.pk}),
            }
        )
    vrfs = []
    for vrf in VRF.objects.filter(pk__in=vrf_pks):
        references = {}
        for relation in vrf._meta.related_objects:
            try:
                n = getattr(vrf, relation.get_accessor_name()).count()
            except JobTimeoutException:
                raise
            except Exception:  # noqa: BLE001 - an unreadable relation is skipped
                continue
            if n:
                references[relation.related_model._meta.label_lower] = n
        vrfs.append({"pk": vrf.pk, "references": references})
    return {"ingestion_pk": ingestion.pk, "devices": devices, "vrfs": vrfs}


def _inventory_items(sync):
    from dcim.models import InventoryItem

    qs = InventoryItem.objects.all()
    return {
        "total": qs.count(),
        "by_manufacturer": _counts(qs, "manufacturer__name"),
        "by_role": _counts(qs, "role__name"),
        "on_uncovered_devices": qs.filter(device__tags__slug=UNCOVERED_TAG)
        .distinct()
        .count(),
        "by_created_day": [
            {"day": str(row["day"]), "count": row["n"]}
            for row in qs.annotate(day=TruncDate("created"))
            .values("day")
            .annotate(n=Count("pk"))
            .order_by("-day")[:30]
        ],
    }


def _duplicate_device_names(sync):
    """Devices sharing a name (case-insensitive), redacted.

    A device whose site label changes between syncs can be looked up under
    its OLD site, miss, and get created again under the new one instead of
    updated - the stale copy then drifts out of tag scope and becomes
    uncovered. This is the mechanism the count, alone, cannot show: two rows
    with the same name is the signature, whichever site relabeling produced
    it. Names and tag values are never exported; site is a pk, and whether
    the group's tag SETS differ (not what they are) is what confirms one
    copy fell out of scope while the other did not.
    """
    from collections import defaultdict

    from dcim.models import Device

    groups = defaultdict(list)
    qs = (
        Device.objects.all()
        .select_related("site", "role", "platform", "device_type__manufacturer")
        .prefetch_related("tags")
    )
    for device in qs:
        groups[(device.name or "").strip().casefold()].append(device)

    out = []
    for key, devices in groups.items():
        if len(devices) < 2:
            continue
        rows = []
        tag_sets = []
        for device in devices:
            tags = frozenset(tag.slug for tag in device.tags.all())
            tag_sets.append(tags)
            rows.append(
                {
                    "pk": device.pk,
                    "site_pk": device.site_id,
                    "role": getattr(device.role, "name", None),
                    "platform": getattr(device.platform, "name", None),
                    "manufacturer": getattr(
                        getattr(device.device_type, "manufacturer", None), "name", None
                    ),
                    "device_type": getattr(device.device_type, "model", None),
                    "status": device.status,
                    "created": device.created.isoformat() if device.created else None,
                    "tag_count": len(tags),
                    "uncovered": UNCOVERED_TAG in tags,
                }
            )
        by_site = defaultdict(int)
        for row in rows:
            by_site[row["site_pk"]] += 1
        out.append(
            {
                "count": len(rows),
                "distinct_sites": len(by_site),
                "same_site_repeats": {
                    str(site_pk): n for site_pk, n in by_site.items() if n > 1
                },
                # True when the copies do NOT all carry the same tags - the
                # shape of "one is in scope, the other fell out of it".
                "tag_sets_differ": len(set(tag_sets)) > 1,
                "devices": rows,
            }
        )
    out.sort(key=lambda group: -group["count"])
    return {
        "distinct_duplicated_names": len(out),
        "total_duplicate_rows": sum(group["count"] for group in out),
        "multi_site_groups": sum(1 for group in out if group["distinct_sites"] > 1),
        "tag_mismatch_groups": sum(1 for group in out if group["tag_sets_differ"]),
        "groups": out,
    }


_BUILDERS = {
    "uncovered": _uncovered,
    "uncovered_tag_timeline": _uncovered_tag_timeline,
    "scope_trend": _scope_trend,
    "device_renames": _device_renames,
    "recent_devices": _recent_devices,
    "console_servers": _console_servers,
    "nqe_map_bindings": _nqe_map_bindings,
    "issue_references": _issue_references,
    "inventory_items": _inventory_items,
    "duplicate_device_names": _duplicate_device_names,
}


def bundle_diagnostics(sync) -> dict:
    """Every section, each isolated: a failure records its class, never masks."""
    out = {"errors": {}}
    for name in SECTIONS:
        try:
            out[name] = _BUILDERS[name](sync)
        except JobTimeoutException:
            raise
        except Exception as exc:  # noqa: BLE001 - one section must not sink the bundle
            out[name] = None
            out["errors"][name] = type(exc).__name__
    return out
