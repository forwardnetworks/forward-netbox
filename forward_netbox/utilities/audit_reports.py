# Operator audit reports: the nine read-only audits that until 2.9.6 existed
# only as management commands, each rendered on the sync page.
#
# Two shapes, chosen by whether the audit talks to Forward. An audit that
# only reads the NetBox database runs on the GET and renders live. An audit
# that issues Forward calls runs as a button job (`audit_<key>` in
# `BUTTON_JOB_SPECS`) and the page renders the latest completed job's
# payload, so a GET never spends an NQE execution and never blocks on
# Forward. Both shapes present through the same `sections` contract - a
# summary table, zero or more row tables, and an optional remediation - so
# one template renders every audit.
from dataclasses import dataclass

from django.apps import apps
from django.contrib.contenttypes.models import ContentType
from django.utils.translation import gettext_lazy as _


@dataclass
class AuditReport:
    key: str
    title: str
    description: str
    forward_backed: bool
    run: object  # callable(sync, client) -> payload (client is None when not forward_backed)
    present: object  # callable(payload) -> sections
    job_suffix: str = ""

    @property
    def kind(self):
        return f"audit_{self.key}"


def _sections(summary, tables=(), remediation="", note=""):
    return {
        "summary": [{"label": str(label), "value": value} for label, value in summary],
        "tables": [
            {
                "title": str(table["title"]),
                "columns": [str(column) for column in table["columns"]],
                "rows": table["rows"],
                "total": table.get("total", len(table["rows"])),
            }
            for table in tables
            if table
        ],
        "remediation": str(remediation or ""),
        "note": str(note or ""),
    }


# --- Forward-backed audits (button jobs) --------------------------------------


def _run_primary_ip(sync, client):
    from .primary_ip_audit import audit_primary_ip_resolution

    return audit_primary_ip_resolution(sync, client, sample_limit=25)


def _present_primary_ip(payload):
    return _sections(
        [
            (_("Snapshot"), payload.get("snapshot_id")),
            (_("Devices with a management tag"), payload.get("mgmt_tagged_devices")),
            (_("Resolvable to a primary IP"), payload.get("resolvable")),
            (_("Unresolved"), payload.get("unresolved")),
            (
                _("Unresolved: device not in NetBox"),
                payload.get("unresolved_device_not_in_netbox"),
            ),
            (
                _("Unresolved: management interface not matched"),
                payload.get("unresolved_interface_not_matched"),
            ),
            (
                _("Unresolved: interface has no IP"),
                payload.get("unresolved_interface_present_no_ip"),
            ),
        ],
        [
            {
                "title": _("Devices not in NetBox"),
                "columns": [_("Device")],
                "rows": [
                    [name] for name in payload.get("example_device_not_in_netbox") or []
                ],
                "total": payload.get("unresolved_device_not_in_netbox") or 0,
            },
            {
                "title": _("Management interface not matched"),
                "columns": [_("Device"), _("Tags")],
                "rows": [
                    [entry[0], ", ".join(map(str, entry[1]))]
                    for entry in payload.get("example_interface_not_matched") or []
                    if isinstance(entry, (list, tuple)) and len(entry) >= 2
                ],
                "total": payload.get("unresolved_interface_not_matched") or 0,
            },
            {
                "title": _("Interface present but carries no IP"),
                "columns": [_("Device"), _("Interface"), _("Tags")],
                "rows": [
                    [entry[0], entry[1], ", ".join(map(str, entry[2]))]
                    for entry in payload.get("example_interface_present_no_ip") or []
                    if isinstance(entry, (list, tuple)) and len(entry) >= 3
                ],
                "total": payload.get("unresolved_interface_present_no_ip") or 0,
            },
        ],
        note=_(
            "An unresolved device is a NetBox-side gap: the device, its "
            "management interface, or that interface's address is missing, so "
            "the sync cannot assign a primary IP from the tag."
        ),
    )


def _run_global_ipam(sync, client):
    from .logging import SyncLogging
    from .scope_ipam_audit import audit_global_ipam_scope

    return audit_global_ipam_scope(sync, client, SyncLogging(), sample_limit=25)


def _present_global_ipam(payload):
    results = payload.get("results") or []
    return _sections(
        [
            (
                _("Models audited"),
                ", ".join(payload.get("models_audited") or []) or "-",
            ),
            (_("Stale objects"), payload.get("total_stale")),
        ],
        [
            {
                "title": _("Per model"),
                "columns": [
                    _("Model"),
                    _("Forward rows"),
                    _("NetBox objects"),
                    _("Unmatchable"),
                    _("Stale"),
                    _("Sample"),
                ],
                "rows": [
                    [
                        result.get("model"),
                        result.get("forward_rows"),
                        result.get("netbox_count"),
                        result.get("unmatchable_count"),
                        result.get("stale_count"),
                        ", ".join(map(str, result.get("stale_sample") or [])),
                    ]
                    for result in results
                ],
            }
        ],
        note=_(
            "Network-global IPAM (VRFs, VLANs, prefixes) is never pruned by the "
            "sync. A stale object is one the latest Forward fetch no longer "
            "reports; the Tag delete-eligible IPAM action marks them for "
            "review in NetBox."
        ),
    )


def _run_stale_hardware_notices(sync, client):
    from .dlm_notice_audit import emitted_device_type_slugs
    from .dlm_notice_audit import fetch_emitted_hardware_notice_rows
    from .dlm_notice_audit import stale_hardware_notices

    rows, fetch_error = fetch_emitted_hardware_notice_rows(sync)
    if rows is None:
        return {"available": False, "reason": fetch_error, "stale_notice_count": 0}
    return stale_hardware_notices(emitted_device_type_slugs(rows), sample_limit=50)


def _present_stale_hardware_notices(payload):
    if not payload.get("available", True):
        return _sections(
            [(_("Available"), False), (_("Reason"), payload.get("reason"))]
        )
    return _sections(
        [
            (
                _("Device types Forward emits notices for"),
                payload.get("forward_emitted_device_types"),
            ),
            (_("Stale notices"), payload.get("stale_notice_count")),
        ],
        [
            {
                "title": _("Stale notices"),
                "columns": [_("Notice")],
                "rows": [
                    [str(notice)] for notice in payload.get("stale_notices") or []
                ],
                "total": payload.get("stale_notice_count") or 0,
            }
        ],
        note=_(
            "A stale notice belongs to a device type the hardware-notice query "
            "no longer emits. Nothing else refreshes or removes it; the Prune "
            "stale hardware notices action deletes them."
        ),
    )


def _run_apply_identity(sync, client):
    from .apply_identity_audit import audit_apply_identity

    return audit_apply_identity(sync, sample_limit=15)


def _present_apply_identity(payload):
    results = payload.get("results") or []
    return _sections(
        [
            (
                _("Models audited"),
                ", ".join(payload.get("models_audited") or []) or "-",
            ),
            (
                _("Churn suspects"),
                ", ".join(payload.get("churn_suspect_models") or []) or "-",
            ),
        ],
        [
            {
                "title": _("Per model"),
                "columns": [
                    _("Model"),
                    _("Forward rows"),
                    _("NetBox objects"),
                    _("Would create"),
                    _("Would delete"),
                    _("Churn suspect"),
                ],
                "rows": [
                    [
                        result.get("model"),
                        result.get("forward_rows"),
                        result.get("netbox_count"),
                        result.get("would_create_count"),
                        result.get("would_delete_count"),
                        bool(result.get("churn_suspect")),
                    ]
                    for result in results
                ],
            },
            {
                "title": _("Near-identical pairs"),
                "columns": [
                    _("Model"),
                    _("NetBox key"),
                    _("Forward key"),
                    _("Difference"),
                ],
                "rows": [
                    [
                        result.get("model"),
                        pair.get("netbox_key") if isinstance(pair, dict) else pair[0],
                        pair.get("forward_key") if isinstance(pair, dict) else pair[1],
                        pair.get("diff") if isinstance(pair, dict) else pair[2],
                    ]
                    for result in results
                    for pair in result.get("churn_pairs") or []
                ],
            },
        ],
        note=_(
            "A churn suspect is a model where the sync would create and delete "
            "near-identical rows on every run: its identity key disagrees "
            "between Forward and NetBox (case, whitespace, a slug rule)."
        ),
    )


def _run_apic_cimc_readiness(sync, client):
    from .apic_cimc_readiness import audit_apic_cimc_readiness

    return audit_apic_cimc_readiness(sync, client)


def _present_apic_cimc_readiness(payload):
    return _sections(
        [
            (_("Snapshot selector"), payload.get("snapshot_selector")),
            (_("APIC devices"), payload.get("apic_device_count")),
            (_("With controller detail"), payload.get("with_controller_detail")),
            (_("With the eqptCh custom command"), payload.get("with_eqptch_command")),
            (_("Completed with eqptCh"), payload.get("completed_with_eqptch")),
            (_("CIMC inventory ready"), bool(payload.get("cimc_inventory_ready"))),
        ],
        remediation=payload.get("remediation") or "",
    )


def _run_fast_baseline_preflight(sync, client):
    from .fast_baseline import fast_baseline_preflight

    return fast_baseline_preflight(sync=sync, client=client)


def _present_fast_baseline_preflight(payload):
    context = payload.get("context") or {}
    rows = []
    if isinstance(context, dict):
        for key, value in sorted(context.items()):
            if key in {"allowlist", "workloads"}:
                continue
            rows.append(
                [
                    key,
                    value if not isinstance(value, (list, dict)) else str(value)[:300],
                ]
            )
    return _sections(
        [
            (_("Eligible"), bool(payload.get("eligible"))),
            (_("Reason"), payload.get("reason_code") or "-"),
            (
                _("Workload fetch performed"),
                bool(payload.get("workload_fetch_performed")),
            ),
            (_("Workload fetch seconds"), payload.get("workload_fetch_seconds")),
        ],
        [
            {
                "title": _("Decision context"),
                "columns": [_("Key"), _("Value")],
                "rows": rows,
            }
        ],
        note=_(
            "The fast baseline loads a first sync into an empty NetBox directly "
            "instead of staging and merging every row. This is the same proof "
            "the sync makes, run without writing anything."
        ),
    )


# --- Database-only audits (rendered on the GET) -------------------------------


def _run_device_name_ambiguity(sync, client=None):
    from .ownership import device_name_ambiguity_report

    return device_name_ambiguity_report(sync)


def _present_device_name_ambiguity(payload):
    devices = payload.get("devices") or {}
    return _sections(
        [
            (_("Duplicated names"), payload.get("duplicated_names")),
            (_("Ambiguous names"), len(payload.get("ambiguous_names") or [])),
            (
                _("Resolved by an existing binding"),
                len(payload.get("resolved_by_existing_binding") or []),
            ),
        ],
        [
            {
                "title": _("Ambiguous names"),
                "columns": [
                    _("Name"),
                    _("Device"),
                    _("Site"),
                    _("Status"),
                    _("Bound to"),
                ],
                "rows": [
                    [
                        name,
                        {"device_id": entry.get("device_id")},
                        entry.get("site"),
                        entry.get("status"),
                        entry.get("bound_to_source_key") or "-",
                    ]
                    for name in sorted(devices)
                    for entry in devices[name]
                ],
            }
        ],
        note=_(
            "The sync holds a name that matches more than one unbound NetBox "
            "device rather than guess. Bind or rename one of each pair."
        ),
    )


def _run_routing_dangling(sync, client=None):
    from .routing_dangling_audit import audit_routing_dangling_rows

    return audit_routing_dangling_rows()


def _present_routing_dangling(payload):
    if payload.get("skipped"):
        return _sections([(_("Skipped"), payload["skipped"])])
    dangling = payload.get("dangling") or {}
    return _sections(
        [
            (_(label), dangling.get(key))
            for key, label in (
                ("bgprouter", "Dangling BGP routers"),
                ("bgpscope", "Scopes under them"),
                ("bgpaddressfamily", "Address families under them"),
                ("bgppeer", "Peers under them"),
            )
        ],
        [
            {
                "title": _("Dangling router primary keys"),
                "columns": [_("BGP router pk")],
                "rows": [[pk] for pk in payload.get("sample_router_pks") or []],
                "total": dangling.get("bgprouter") or 0,
            }
        ],
        note=payload.get("note") or "",
    )


def _run_interface_untagged_vlans(sync, client=None):
    from .interface_vlan_audit import audit_interface_untagged_vlans

    return audit_interface_untagged_vlans(sample_limit=100)


def _present_interface_untagged_vlans(payload):
    def rows(entries):
        return [
            [
                {"device_id": entry.get("device_id"), "label": entry.get("device")},
                {
                    "interface_id": entry.get("interface_id"),
                    "label": entry.get("interface"),
                },
                entry.get("mode"),
                (
                    f"{entry.get('vlan')} ({entry.get('vlan_vid')})"
                    if entry.get("vlan") is not None
                    else None
                ),
                entry.get("device_site"),
                entry.get("vlan_site"),
            ]
            for entry in entries or []
        ]

    columns = [
        _("Device"),
        _("Interface"),
        _("Mode"),
        _("Untagged VLAN"),
        _("Device site"),
        _("VLAN site"),
    ]
    remediation = " ".join(
        text
        for text in (
            payload.get("cross_site_remediation"),
            payload.get("no_mode_remediation"),
        )
        if text
    )
    return _sections(
        [
            (_("Cross-site untagged VLANs"), payload.get("cross_site_count")),
            (_("Untagged VLAN with no mode"), payload.get("no_mode_count")),
        ],
        [
            {
                "title": _("Cross-site"),
                "columns": columns,
                "rows": rows(payload.get("cross_site")),
                "total": payload.get("cross_site_count") or 0,
            },
            {
                "title": _("No mode"),
                "columns": columns,
                "rows": rows(payload.get("no_mode")),
                "total": payload.get("no_mode_count") or 0,
            },
        ],
        remediation=remediation,
        note=_(
            "NetBox refuses an interface whose untagged VLAN belongs to another "
            "site, or that carries an untagged VLAN with no 802.1Q mode; the "
            "sync reports those interfaces as failures until they are fixed."
        ),
    )


AUDIT_REPORTS = {
    report.key: report
    for report in (
        AuditReport(
            key="primary_ip",
            title=_("Primary IP resolution"),
            description=_(
                "How many management-tagged devices resolve to a primary IP, "
                "and why the rest do not."
            ),
            forward_backed=True,
            job_suffix="audit primary IP resolution",
            run=_run_primary_ip,
            present=_present_primary_ip,
        ),
        AuditReport(
            key="global_ipam",
            title=_("Stale global IPAM"),
            description=_(
                "VRFs, VLANs and prefixes in NetBox that the latest Forward "
                "fetch no longer reports."
            ),
            forward_backed=True,
            job_suffix="audit stale global IPAM",
            run=_run_global_ipam,
            present=_present_global_ipam,
        ),
        AuditReport(
            key="stale_hardware_notices",
            title=_("Stale DLM hardware notices"),
            description=_(
                "Hardware notices whose device type Forward no longer emits."
            ),
            forward_backed=True,
            job_suffix="audit stale hardware notices",
            run=_run_stale_hardware_notices,
            present=_present_stale_hardware_notices,
        ),
        AuditReport(
            key="apply_identity",
            title=_("Apply-identity churn"),
            description=_(
                "Models whose Forward and NetBox identity keys disagree, so "
                "every run would create and delete near-identical rows."
            ),
            forward_backed=True,
            job_suffix="audit apply identity",
            run=_run_apply_identity,
            present=_present_apply_identity,
        ),
        AuditReport(
            key="apic_cimc_readiness",
            title=_("APIC CIMC readiness"),
            description=_(
                "Whether the snapshot's APIC devices carry the command the "
                "CIMC inventory map reads."
            ),
            forward_backed=True,
            job_suffix="audit APIC CIMC readiness",
            run=_run_apic_cimc_readiness,
            present=_present_apic_cimc_readiness,
        ),
        AuditReport(
            key="fast_baseline_preflight",
            title=_("Fast-baseline eligibility"),
            description=_(
                "Whether a first sync into this NetBox would take the fast "
                "baseline, and the exact rejection when it would not."
            ),
            forward_backed=True,
            job_suffix="audit fast-baseline eligibility",
            run=_run_fast_baseline_preflight,
            present=_present_fast_baseline_preflight,
        ),
        AuditReport(
            key="device_name_ambiguity",
            title=_("Ambiguous device names"),
            description=_(
                "NetBox names matching more than one unbound device, which the "
                "sync holds rather than guesses."
            ),
            forward_backed=False,
            run=_run_device_name_ambiguity,
            present=_present_device_name_ambiguity,
        ),
        AuditReport(
            key="routing_dangling",
            title=_("Dangling routing rows"),
            description=_(
                "netbox-routing BGP rows whose device reference points at a "
                "device that no longer exists."
            ),
            forward_backed=False,
            run=_run_routing_dangling,
            present=_present_routing_dangling,
        ),
        AuditReport(
            key="interface_untagged_vlans",
            title=_("Interface untagged VLANs"),
            description=_(
                "Interfaces NetBox would refuse on their untagged VLAN: "
                "cross-site, or an untagged VLAN with no mode."
            ),
            forward_backed=False,
            run=_run_interface_untagged_vlans,
            present=_present_interface_untagged_vlans,
        ),
    )
}
FORWARD_BACKED_AUDITS = tuple(
    report for report in AUDIT_REPORTS.values() if report.forward_backed
)


def audit_report_or_none(key):
    return AUDIT_REPORTS.get(str(key or "").strip())


def latest_audit_job(sync, report):
    """The newest job for a Forward-backed audit, whatever its status."""
    from core.models import Job

    from ..models import ForwardSync

    return (
        Job.objects.filter(
            object_type=ContentType.objects.get_for_model(ForwardSync),
            object_id=sync.pk,
            name=f"{sync.name} - {report.job_suffix}",
        )
        .order_by("-created")
        .first()
    )


def run_audit_job(job, report):
    """Job body shared by every Forward-backed audit."""
    from .diagnostics import exception_type
    from .diagnostics import safe_operation_failure
    from .json_safe import json_safe_value
    from ..models import ForwardSync

    sync = ForwardSync.objects.get(pk=job.object_id)
    try:
        client = sync.source.get_client()
        payload = report.run(sync, client)
        job.data = {"audit": report.key, "payload": json_safe_value(payload)}
        job.save(update_fields=["data"])
    except Exception as exc:
        job.data = {
            "audit": report.key,
            "error": safe_operation_failure(f"Forward audit ({report.key})", exc),
            "error_type": exception_type(exc),
        }
        job.save(update_fields=["data"])
        raise


def audit_report_context(sync, report):
    """What the report page renders: sections plus provenance."""
    if not report.forward_backed:
        payload = report.run(sync, None)
        return {
            "sections": report.present(payload),
            "job": None,
            "generated_live": True,
            "error": "",
        }
    job = latest_audit_job(sync, report)
    data = getattr(job, "data", None) or {}
    if job is None or "payload" not in data:
        return {
            "sections": None,
            "job": job,
            "generated_live": False,
            "error": data.get("error", ""),
        }
    return {
        "sections": report.present(data["payload"]),
        "job": job,
        "generated_live": False,
        "error": "",
    }


def routing_installed():
    return apps.is_installed("netbox_routing")
