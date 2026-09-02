"""Fail-closed direct-to-main loader for a first Forward baseline.

This is not an incremental merge engine.  It reuses the normal validated
workloads and apply engines, but only while the relevant NetBox inventory is
empty and the installation has no prior Forward baseline evidence.  The whole
load and its durable Forward finalization share one PostgreSQL transaction.
"""

import time
from dataclasses import dataclass
from importlib.metadata import PackageNotFoundError
from importlib.metadata import version as distribution_version

from django.apps import apps
from django.conf import settings
from django.contrib.contenttypes.models import ContentType
from django.db import connection
from django.db import transaction
from django.utils import timezone

from .. import config as forward_config
from ..choices import ForwardSyncStatusChoices
from .version_series import series_matches
from .validated_runtime import VALIDATED_BRANCHING_SERIES
from .validated_runtime import VALIDATED_NETBOX_SERIES
from .validated_runtime import VALIDATED_OPTIONAL_DISTRIBUTION_NAMES
from .validated_runtime import VALIDATED_OPTIONAL_DISTRIBUTIONS
from .validated_runtime import VALIDATED_PLUGIN_APPS

FAST_BASELINE_SPEC_VERSION = 1
FAST_BASELINE_ADVISORY_LOCK_ID = 0x46574442415345
FAST_BASELINE_MODEL_SPEC_VERSIONS = {
    "dcim.site": 1,
    "dcim.manufacturer": 1,
    "dcim.devicerole": 1,
    "dcim.platform": 1,
    "dcim.devicetype": 1,
    "dcim.device": 1,
    "dcim.interface": 4,
    "dcim.inventoryitem": 5,
    "dcim.macaddress": 2,
    "dcim.cable": 3,
    "ipam.vrf": 1,
    "ipam.prefix": 3,
    "ipam.ipaddress": 3,
    "netbox_dlm.softwareversion": 1,
    "netbox_dlm.hardwarenotice": 1,
    "netbox_dlm.devicesoftware": 1,
    "netbox_dlm.cve": 1,
    "netbox_dlm.vulnerability": 3,
    "dcim.module": 1,
    "extras.taggeditem": 1,
    "ipam.fhrpgroup": 1,
    "ipam.vlan": 1,
    "netbox_routing.bgpaddressfamily": 1,
    "netbox_routing.bgppeer": 1,
    "netbox_routing.bgppeeraddressfamily": 1,
    "netbox_routing.ospfarea": 1,
    "netbox_routing.ospfinstance": 1,
    "netbox_routing.ospfinterface": 1,
}
FAST_BASELINE_ALLOWED_MODELS = frozenset(FAST_BASELINE_MODEL_SPEC_VERSIONS)
FAST_BASELINE_OMITTED_EVIDENCE = (
    "branch",
    "branch_event",
    "source_object_change",
    "source_change_diff",
    "destination_object_change",
    "applied_change",
    "branch_rollback",
)


# Every field a direct-loaded model requires, recorded against the runtime this
# engine was proven on.
#
# The fast baseline `bulk_create`s straight into main, bypassing branch audit
# and the per-object save path, and it populates denormalized columns by hand
# (`_device_id`, `_rack_id`, `_site_id`, `_location_id`). Its other contracts are
# read live — the search index comes from `get_indexer(model).fields`, so a newly
# indexed field is picked up automatically — but nothing noticed a *column*
# appearing on a model it writes.
#
# The check is deliberately about REQUIRED fields, not every field. A new
# optional column (nullable, blank, or defaulted) is written correctly by
# `bulk_create` without our help, which is why netbox-dlm 0.5.0 adding
# `SoftwareVersion.release_designation` is fine. A new *required* column is one
# the loader would leave unset, so it fails closed instead.
# The three hierarchical models lost `level`/`lft`/`rght`/`tree_id` in NetBox
# 4.7: ltree replaced django-mptt and the columns were dropped. Their `path`
# replacement is NOT required - database triggers maintain it - so `bulk_create`
# no longer has to fabricate tree bookkeeping at all. Read off the live runtime
# rather than hand-edited, because this contract exists to fail closed when a
# column appears, and a guess would defeat it.
FAST_BASELINE_REQUIRED_FIELD_CONTRACT = {
    "dcim.cable": (),
    "dcim.device": ("device_type", "role", "site"),
    "dcim.devicerole": ("name", "slug"),
    "dcim.devicetype": ("manufacturer", "model", "slug"),
    "dcim.interface": ("device", "name", "type"),
    "dcim.inventoryitem": ("device", "name"),
    "dcim.macaddress": ("mac_address",),
    "dcim.manufacturer": ("name", "slug"),
    "dcim.module": ("device", "module_bay", "module_type"),
    "dcim.platform": ("name", "slug"),
    "dcim.site": ("name", "slug"),
    "extras.taggeditem": ("content_type", "object_id", "tag"),
    "ipam.fhrpgroup": ("group_id", "protocol"),
    "ipam.ipaddress": ("address",),
    "ipam.prefix": ("prefix",),
    "ipam.vlan": ("name", "vid"),
    "ipam.vrf": ("name",),
    "netbox_dlm.cve": ("cve_id",),
    "netbox_dlm.devicesoftware": ("device", "software_version"),
    "netbox_dlm.hardwarenotice": (),
    "netbox_dlm.softwareversion": ("platform", "version"),
    "netbox_dlm.vulnerability": ("cve", "software_version"),
    "netbox_routing.bgpaddressfamily": ("address_family", "scope"),
    "netbox_routing.bgppeer": ("peer",),
    "netbox_routing.bgppeeraddressfamily": ("address_family",),
    "netbox_routing.ospfarea": ("area_id",),
    "netbox_routing.ospfinstance": ("device", "name", "process_id", "router_id"),
    "netbox_routing.ospfinterface": ("area", "instance", "interface"),
}


def _required_field_names(model):
    """Fields `bulk_create` cannot fill in for us."""
    from django.db import models as django_models

    names = []
    for field in model._meta.concrete_fields:
        if field.auto_created or field.null or field.has_default() or field.blank:
            continue
        if isinstance(field, django_models.DateTimeField) and (
            field.auto_now or field.auto_now_add
        ):
            continue
        names.append(field.name)
    return tuple(sorted(names))


def fast_baseline_field_contract_drift():
    """Models whose required-field set no longer matches the recorded contract."""
    from django.apps import apps

    drift = []
    for label, expected in sorted(FAST_BASELINE_REQUIRED_FIELD_CONTRACT.items()):
        try:
            model = apps.get_model(label)
        except LookupError:
            # An optional plugin that is not installed cannot be loaded either.
            continue
        actual = _required_field_names(model)
        if actual != tuple(expected):
            drift.append(
                {
                    "model": label,
                    "expected": list(expected),
                    "actual": list(actual),
                }
            )
    return drift


@dataclass(frozen=True)
class FastBaselineDecision:
    enabled: bool
    reason_code: str
    context: dict


def _enabled(parameters, key):
    value = parameters.get(key, False)
    if isinstance(value, str):
        return value.strip().lower() in {"true", "1", "yes", "on"}
    return bool(value)


def _distribution_version(distribution):
    try:
        return distribution_version(distribution)
    except PackageNotFoundError:
        return None


def fast_baseline_runtime_tuple():
    release = getattr(settings, "RELEASE", None)
    netbox_version = getattr(release, "version", None) or getattr(
        settings, "VERSION", ""
    )
    # Derived, never repeated: a distribution EXPECTED by the decision below
    # but not probed here reads as ABSENT and fails the match exactly as a
    # wrong version would. Two lists cannot disagree if there is only one.
    optional = VALIDATED_OPTIONAL_DISTRIBUTION_NAMES
    return {
        "netbox": str(netbox_version or ""),
        "branching": _distribution_version("netboxlabs-netbox-branching"),
        "forward_netbox": str(forward_config.version),
        "optional_plugins": {name: _distribution_version(name) for name in optional},
        "plugin_apps": sorted(getattr(settings, "PLUGINS", ()) or ()),
    }


def _runtime_decision():
    actual = fast_baseline_runtime_tuple()
    expected = {
        "netbox_series": VALIDATED_NETBOX_SERIES,
        "branching_series": VALIDATED_BRANCHING_SERIES,
        "forward_netbox": "2.9.2",
        # Each optional distribution lists every version validated against this
        # engine, not a single pin. An exact pin meant a customer upgrading one
        # optional plugin silently lost the fast baseline — no error, just a
        # first sync that takes hours instead of minutes — because the whole
        # tuple stopped matching.
        "optional_plugins": VALIDATED_OPTIONAL_DISTRIBUTIONS,
        "plugin_apps": sorted(VALIDATED_PLUGIN_APPS),
    }
    mismatched = (
        not series_matches(actual["netbox"], expected["netbox_series"])
        or not series_matches(actual["branching"], expected["branching_series"])
        or actual["forward_netbox"] != expected["forward_netbox"]
        or actual["plugin_apps"] != expected["plugin_apps"]
        or any(
            actual["optional_plugins"].get(name) not in versions
            for name, versions in expected["optional_plugins"].items()
        )
    )
    if mismatched:
        return FastBaselineDecision(
            False,
            "unsupported_runtime_tuple",
            {
                "expected": {
                    **expected,
                    "optional_plugins": {
                        name: sorted(versions)
                        for name, versions in expected["optional_plugins"].items()
                    },
                },
                "actual": actual,
            },
        )
    if connection.vendor != "postgresql":
        return FastBaselineDecision(False, "postgresql_required", {})
    # Version series alone cannot tell a harmless patch from one that adds a
    # column this loader would leave unset, so check the models themselves.
    drift = fast_baseline_field_contract_drift()
    if drift:
        return FastBaselineDecision(
            False,
            "model_field_contract_mismatch",
            {"drift": drift},
        )
    return FastBaselineDecision(True, "supported_runtime_tuple", actual)


def _workload_models(workloads):
    return frozenset(str(workload.model_string) for workload in workloads)


def _selected_models(sync, workloads):
    return _workload_models(workloads) | frozenset(sync.enabled_models())


def _unsupported_delete_workloads(workloads):
    from .workload_normalization import (
        CVE_WITHOUT_IN_SCOPE_VULNERABILITY_DELETE_CONTRACT,
    )

    unsupported = []
    for workload in workloads:
        if not workload.delete_rows:
            continue
        contract = str(getattr(workload, "derived_delete_contract", "") or "")
        cve_ids = [str(row.get("cve_id") or "").strip() for row in workload.delete_rows]
        upsert_cve_ids = {
            str(row.get("cve_id") or "").strip() for row in workload.upsert_rows
        }
        supported = (
            str(workload.model_string) == "netbox_dlm.cve"
            and str(workload.sync_mode) == "full"
            and contract == CVE_WITHOUT_IN_SCOPE_VULNERABILITY_DELETE_CONTRACT
            and int(getattr(workload, "derived_delete_count", 0) or 0)
            == len(workload.delete_rows)
            and all(cve_ids)
            and len(cve_ids) == len(set(cve_ids))
            and not (set(cve_ids) & upsert_cve_ids)
        )
        if not supported:
            unsupported.append(
                {
                    "model": str(workload.model_string),
                    "count": len(workload.delete_rows),
                    "contract": contract,
                    "derived_count": int(
                        getattr(workload, "derived_delete_count", 0) or 0
                    ),
                }
            )
    return unsupported


def fast_baseline_static_decision(*, sync, workloads, model_results=None):
    """Check immutable/request facts before an ingestion or table lock exists."""
    parameters = dict(getattr(sync, "parameters", {}) or {})
    if not _enabled(parameters, "enable_fast_baseline_load"):
        return FastBaselineDecision(False, "disabled_by_default", {})
    if not bool(getattr(sync, "auto_merge", False)):
        return FastBaselineDecision(False, "auto_merge_required", {})
    if not _enabled(parameters, "enable_bulk_orm"):
        return FastBaselineDecision(False, "bulk_orm_required", {})
    if _enabled(parameters, "set_primary_ip_from_mgmt_tag"):
        return FastBaselineDecision(False, "primary_ip_overlay_not_supported", {})
    if any(str(workload.sync_mode) != "full" for workload in workloads):
        return FastBaselineDecision(False, "full_workloads_required", {})
    unsupported_deletes = _unsupported_delete_workloads(workloads)
    if unsupported_deletes:
        return FastBaselineDecision(
            False,
            "delete_rows_not_supported",
            {"workloads": unsupported_deletes},
        )
    failed_models = sorted(
        {
            str(result.get("model") or "")
            for result in (model_results or [])
            if int(result.get("failure_count") or 0) > 0
        }
        - {""}
    )
    if failed_models:
        return FastBaselineDecision(
            False,
            "model_result_failure_present",
            {"models": failed_models},
        )
    models = _selected_models(sync, workloads)
    unsupported = sorted(models - FAST_BASELINE_ALLOWED_MODELS)
    if unsupported:
        return FastBaselineDecision(
            False,
            "model_not_allowlisted",
            {
                "unsupported": unsupported,
                "allowlist": sorted(FAST_BASELINE_ALLOWED_MODELS),
            },
        )
    from .fast_baseline_models import fast_baseline_workload_contract

    rows_enabled, reason_code, row_context = fast_baseline_workload_contract(
        sync, workloads
    )
    if not rows_enabled:
        return FastBaselineDecision(False, reason_code, row_context)
    runtime = _runtime_decision()
    if not runtime.enabled:
        return runtime
    return FastBaselineDecision(
        True,
        "static_contract_satisfied",
        {
            **runtime.context,
            "spec_version": FAST_BASELINE_SPEC_VERSION,
            "models": sorted(models),
            "model_specs": {
                model: FAST_BASELINE_MODEL_SPEC_VERSIONS[model]
                for model in sorted(models)
            },
        },
    )


def _target_models(model_strings):
    resolved = []
    for model_string in sorted(model_strings):
        app_label, model_name = model_string.split(".", 1)
        model = apps.get_model(app_label, model_name)
        if model is None:
            return None, model_string
        resolved.append(model)
    return resolved, ""


def _lock_target_tables(models, side_models=()):
    table_names = {model._meta.db_table for model in (*models, *side_models)}
    # Lock local M2M through tables because DLM vulnerability materializes the
    # CVE/SoftwareVersion relation as part of the baseline contract.
    for model in models:
        for field in model._meta.local_many_to_many:
            table_names.add(field.remote_field.through._meta.db_table)
    for app_label, model_name in (
        ("dcim", "CableTermination"),
        ("dcim", "CablePath"),
        ("dcim", "InventoryItemRole"),
        ("netbox_dlm", "DeviceSoftware"),
        ("netbox_dlm", "SoftwareVersion"),
        ("netbox_dlm", "CVE"),
    ):
        model = apps.get_model(app_label, model_name)
        if model is not None:
            table_names.add(model._meta.db_table)
            for field in model._meta.local_many_to_many:
                table_names.add(field.remote_field.through._meta.db_table)
    # Serialize branch provisioning and Forward baseline creation with the
    # empty-state decision. Normal syncs do not take our advisory lock, so the
    # table locks are the race barrier that prevents a competing branch or
    # ingestion from appearing after the checks.
    from netbox_branching.models import Branch

    from ..models import (
        ForwardContributorBaseline,
        ForwardDeviceIdentity,
        ForwardIngestion,
        ForwardWorkloadState,
    )

    table_names.update(
        model._meta.db_table
        for model in (
            Branch,
            ForwardIngestion,
            ForwardWorkloadState,
            ForwardContributorBaseline,
            ForwardDeviceIdentity,
        )
    )
    quote = connection.ops.quote_name
    with connection.cursor() as cursor:
        cursor.execute(
            "LOCK TABLE "
            + ", ".join(quote(name) for name in sorted(table_names))
            + " IN SHARE ROW EXCLUSIVE MODE"
        )
    return tuple(sorted(table_names))


def _side_models(model_strings):
    specs = []
    if "dcim.inventoryitem" in model_strings:
        specs.extend(
            (
                ("dcim", "InventoryItemRole"),
                ("dcim", "Manufacturer"),
            )
        )
    if "dcim.module" in model_strings:
        specs.extend(
            (
                ("dcim", "Manufacturer"),
                ("dcim", "ModuleBay"),
                ("dcim", "ModuleType"),
            )
        )
    if "dcim.cable" in model_strings:
        specs.extend((("dcim", "CableTermination"), ("dcim", "CablePath")))
    if "extras.taggeditem" in model_strings:
        specs.append(("extras", "Tag"))
    if "ipam.fhrpgroup" in model_strings:
        specs.extend(
            (
                ("ipam", "FHRPGroupAssignment"),
                ("ipam", "IPAddress"),
                ("ipam", "VRF"),
            )
        )
    if "ipam.vlan" in model_strings:
        specs.append(("dcim", "Site"))
    if any(model.startswith("netbox_routing.bgp") for model in model_strings):
        specs.extend(
            (
                ("ipam", "ASN"),
                ("ipam", "IPAddress"),
                ("ipam", "VRF"),
                ("netbox_routing", "BGPRouter"),
                ("netbox_routing", "BGPScope"),
            )
        )
    if "netbox_routing.bgppeeraddressfamily" in model_strings:
        specs.extend(
            (
                ("netbox_routing", "BGPPeer"),
                ("netbox_routing", "BGPAddressFamily"),
            )
        )
    if any(model.startswith("netbox_routing.ospf") for model in model_strings):
        specs.append(("ipam", "VRF"))
    if "netbox_routing.ospfinterface" in model_strings:
        specs.extend(
            (
                ("netbox_routing", "OSPFArea"),
                ("netbox_routing", "OSPFInstance"),
            )
        )
    if "netbox_dlm.vulnerability" in model_strings:
        specs.extend(
            (
                ("netbox_dlm", "SoftwareVersion"),
                ("netbox_dlm", "DeviceSoftware"),
                ("netbox_dlm", "CVE"),
            )
        )
    models = []
    seen = set()
    for app_label, model_name in specs:
        model = apps.get_model(app_label, model_name)
        if model is not None and model not in seen:
            seen.add(model)
            models.append(model)
    return models


def _dynamic_hook_decision(models):
    from extras.models import CustomField, EventRule
    from netbox.config import get_config

    content_types = [ContentType.objects.get_for_model(model) for model in models]
    for model, content_type in zip(models, content_types, strict=True):
        # NetBox 4.7 returns a list, so `.order_by()` is gone too; sort here.
        # 4.7 also omits fields whose stored data a background job is still
        # updating, which is the answer this gate wants: a field mid-rewrite
        # is not one the fast path may assume is settled.
        custom_fields = sorted(
            CustomField.objects.get_for_model(model), key=lambda field: field.name
        )
        model_string = f"{model._meta.app_label}.{model._meta.model_name}"
        # The exact supported plugin migration owns this optional Device object
        # field. It is populated only by the generation-guarded post-baseline
        # parent overlay; target Device rows start with an empty value on both
        # current and fast paths. No operator-defined field is admitted.
        allowed_parent_field = (
            model_string == "dcim.device"
            and len(custom_fields) == 1
            and custom_fields[0].name == "forward_parent_device"
            and custom_fields[0].type == "object"
            and not custom_fields[0].required
            and custom_fields[0].related_object_type_id == content_type.pk
        )
        if (model_string == "dcim.device" and not allowed_parent_field) or (
            model_string != "dcim.device" and custom_fields
        ):
            return FastBaselineDecision(
                False,
                "custom_field_definition_present",
                {
                    "model": model_string,
                    "fields": [field.name for field in custom_fields],
                },
            )
        if EventRule.objects.filter(enabled=True, object_types=content_type).exists():
            return FastBaselineDecision(
                False,
                "enabled_event_rule_present",
                {"model": f"{model._meta.app_label}.{model._meta.model_name}"},
            )
    config = get_config()
    validators = getattr(config, "CUSTOM_VALIDATORS", {}) or getattr(
        settings, "CUSTOM_VALIDATORS", {}
    )
    protection_rules = getattr(config, "PROTECTION_RULES", {}) or {}
    model_strings = {
        f"{model._meta.app_label}.{model._meta.model_name}" for model in models
    }
    for configured, reason in (
        (validators, "dynamic_custom_validator_present"),
        (protection_rules, "dynamic_protection_rule_present"),
    ):
        for name, value in getattr(configured, "items", lambda: ())():
            if str(name).lower() in model_strings and value:
                return FastBaselineDecision(
                    False,
                    reason,
                    {"model": str(name).lower()},
                )
    return FastBaselineDecision(True, "no_dynamic_hooks", {})


def fast_baseline_locked_decision(*, sync, workloads, model_results=None):
    """Recheck all mutable eligibility facts while target writes are blocked.

    The caller must be inside ``transaction.atomic()``.  This function takes a
    transaction advisory lock plus table locks before inspecting emptiness.
    """
    if not connection.in_atomic_block:
        raise RuntimeError("Fast-baseline eligibility requires an atomic transaction.")
    static = fast_baseline_static_decision(
        sync=sync,
        workloads=workloads,
        model_results=model_results,
    )
    if not static.enabled:
        return static
    with connection.cursor() as cursor:
        cursor.execute(
            "SELECT pg_advisory_xact_lock(%s)", [FAST_BASELINE_ADVISORY_LOCK_ID]
        )

    models, unresolved = _target_models(_selected_models(sync, workloads))
    if unresolved:
        return FastBaselineDecision(
            False, "target_model_unavailable", {"model": unresolved}
        )
    model_strings = _selected_models(sync, workloads)
    side_models = _side_models(model_strings)
    locked_tables = _lock_target_tables(models, side_models)

    from netbox_branching.choices import BranchStatusChoices
    from netbox_branching.models import Branch

    competing = Branch.objects.exclude(
        status__in=(BranchStatusChoices.MERGED, BranchStatusChoices.FAILED)
    ).exists()
    if competing:
        return FastBaselineDecision(False, "competing_branch_present", {})

    from ..models import (
        ForwardContributorBaseline,
        ForwardDeviceIdentity,
        ForwardIngestion,
        ForwardWorkloadState,
    )

    if ForwardIngestion.objects.exists():
        return FastBaselineDecision(False, "prior_ingestion_present", {})
    if ForwardWorkloadState.objects.exists():
        return FastBaselineDecision(False, "prior_workload_baseline_present", {})
    if ForwardContributorBaseline.objects.exists():
        return FastBaselineDecision(False, "prior_contributor_baseline_present", {})
    if ForwardDeviceIdentity.objects.exists():
        return FastBaselineDecision(False, "prior_device_identity_present", {})

    nonempty = []
    for model in models:
        if model._default_manager.exists():
            nonempty.append(f"{model._meta.app_label}.{model._meta.model_name}")
    for model in side_models:
        if model._default_manager.exists():
            nonempty.append(f"{model._meta.app_label}.{model._meta.model_name}")
    if nonempty:
        return FastBaselineDecision(
            False, "target_table_not_empty", {"models": sorted(nonempty)}
        )
    hooks = _dynamic_hook_decision([*models, *side_models])
    if not hooks.enabled:
        return hooks
    return FastBaselineDecision(
        True,
        "eligible_empty_baseline",
        {**static.context, "locked_tables": list(locked_tables)},
    )


def _statistics_counts(logger):
    statistics = dict(getattr(logger, "log_data", {}).get("statistics") or {})
    applied = sum(int(values.get("applied") or 0) for values in statistics.values())
    failed = sum(int(values.get("failed") or 0) for values in statistics.values())
    skipped = sum(int(values.get("skipped") or 0) for values in statistics.values())
    unchanged = sum(int(values.get("unchanged") or 0) for values in statistics.values())
    return {
        "applied": applied,
        "failed": failed,
        "skipped": skipped,
        "unchanged": unchanged,
        "models": statistics,
    }


def fast_baseline_preflight(*, sync, client=None, logger=None):
    """Run the complete read-only eligibility proof before an operator sync.

    The proof executes the configured Forward workload so row contracts and
    delete/full semantics can be checked. It writes no ingestion, branch, or
    target row. The real load repeats every mutable check under the same locks.
    """
    from .logging import SyncLogging
    from .query_fetch import ForwardQueryFetcher

    logger = logger or SyncLogging()
    initial = fast_baseline_static_decision(sync=sync, workloads=[])
    if not initial.enabled:
        return {
            "eligible": False,
            "reason_code": initial.reason_code,
            "context": initial.context,
            "workload_fetch_performed": False,
        }

    client = client or sync.source.get_client()
    fetcher = ForwardQueryFetcher(sync, client, logger)
    try:
        fetch_started = time.perf_counter()
        context = fetcher.resolve_context()
        workloads = fetcher.fetch_workloads(
            context,
            include_diagnostics=False,
        )
        workload_fetch_seconds = time.perf_counter() - fetch_started
        if not workloads:
            return {
                "eligible": False,
                "reason_code": "no_workloads_returned",
                "context": {
                    "snapshot_id": context.snapshot_id,
                    "model_result_count": len(fetcher.model_results),
                },
                "workload_fetch_performed": True,
                "workload_fetch_seconds": round(workload_fetch_seconds, 6),
                "eligibility_proof_seconds": 0.0,
            }
        model_results = [result.as_dict() for result in fetcher.model_results]
        proof_started = time.perf_counter()
        static = fast_baseline_static_decision(
            sync=sync,
            workloads=workloads,
            model_results=model_results,
        )
        if not static.enabled:
            decision = static
        else:
            with transaction.atomic():
                decision = fast_baseline_locked_decision(
                    sync=sync,
                    workloads=workloads,
                    model_results=model_results,
                )
        eligibility_proof_seconds = time.perf_counter() - proof_started
        return {
            "eligible": decision.enabled,
            "reason_code": decision.reason_code,
            "context": decision.context,
            "workload_fetch_performed": True,
            "workload_fetch_seconds": round(workload_fetch_seconds, 6),
            "eligibility_proof_seconds": round(eligibility_proof_seconds, 6),
            "snapshot_id": context.snapshot_id,
            "workload_count": len(workloads),
            "models": sorted(_workload_models(workloads)),
            "estimated_changes": sum(
                int(workload.estimated_changes or 0) for workload in workloads
            ),
            "model_result_count": len(fetcher.model_results),
        }
    finally:
        fetcher.close_pending_contributor_relations()


def run_fast_baseline_load(executor, *, context, workloads, fetcher):
    """Apply one eligible baseline directly to main and finalize it durably."""
    from .branch_budget import build_branch_plan
    from .branch_lifecycle import run_item_direct_to_main
    from .branching import build_branch_request
    from .ingestion_merge import _complete_post_merge_bookkeeping
    from .workload_state import stage_workload_states

    with transaction.atomic():
        decision = fast_baseline_locked_decision(
            sync=executor.sync,
            workloads=workloads,
            model_results=executor.last_model_results,
        )
        if not decision.enabled:
            return None, decision

        request = build_branch_request(executor.user)
        ingestion = executor._create_ingestion(
            context.as_dict(), change_request_id=request.id
        )
        staged_contributor_relations = fetcher.stage_pending_contributor_baseline(
            ingestion,
            context,
        )
        staged_states = stage_workload_states(
            ingestion,
            fetcher.pending_workload_states,
        )
        plan = build_branch_plan(
            workloads,
            max_changes_per_staging_item=executor.sync.get_max_changes_per_staging_item(),
            oversized_bucket_policy="warn",
        )
        total = len(plan)
        context_dict = context.as_dict()
        for item in plan:
            run_item_direct_to_main(
                executor,
                item,
                context_dict,
                ingestion,
                total_plan_items=total,
            )

        ingestion.sync_mode = executor._sync_mode()
        ingestion.model_results = executor.last_model_results
        counts = _statistics_counts(executor.logger)
        side_changes = int(getattr(ingestion, "_fast_baseline_side_changes", 0) or 0)
        logical_applied = counts["applied"] + side_changes
        snapshot_info = dict(ingestion.snapshot_info or {})
        snapshot_info["fast_baseline_load"] = {
            "engine": "direct_main_bulk_apply",
            "spec_version": FAST_BASELINE_SPEC_VERSION,
            "eligibility_reason": decision.reason_code,
            "runtime": fast_baseline_runtime_tuple(),
            "model_specs": decision.context.get("model_specs", {}),
            "model_engines": dict(
                sorted(getattr(executor, "_fast_baseline_model_engines", {}).items())
            ),
            "adapter_row_counts": dict(
                sorted(
                    getattr(ingestion, "_fast_baseline_adapter_row_counts", {}).items()
                )
            ),
            "omitted_evidence": list(FAST_BASELINE_OMITTED_EVIDENCE),
            "staged_workload_state_count": int(staged_states),
            "staged_contributor_relation_count": int(staged_contributor_relations),
            "statistics": counts,
            "side_changes": side_changes,
            "omitted_proven_noop_deletes": int(
                getattr(ingestion, "_fast_baseline_omitted_proven_noop_deletes", 0) or 0
            ),
            "logical_applied_changes": logical_applied,
        }
        applied_at = timezone.now()
        ingestion.snapshot_info = snapshot_info
        ingestion.merge_applied_at = applied_at
        ingestion.save(
            update_fields=[
                "sync_mode",
                "model_results",
                "snapshot_info",
                "merge_applied_at",
            ]
        )
        # On an empty destination every successful workload mutation is a
        # create. Sync-time failed/skipped rows remain visible in statistics and
        # issues; merge-failure count remains zero, matching a successful merge.
        ingestion.record_change_totals(
            applied=logical_applied,
            failed=0,
            created=logical_applied,
            updated=0,
            deleted=0,
        )
        _complete_post_merge_bookkeeping(
            ingestion,
            context={"mark_baseline_ready": True},
            remove_branch=False,
        )

    executor.sync.status = ForwardSyncStatusChoices.COMPLETED
    executor.logger.log_success(
        "Fast baseline load completed without per-row branch or audit evidence.",
        obj=ingestion,
    )
    return ingestion, decision
