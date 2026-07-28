#!/usr/bin/env python
"""Disposable customer-shaped first-baseline benchmark.

Fixture construction is intentionally outside the timed window.  The timed
window starts immediately before ingestion/branch creation and ends after the
same durable post-merge bookkeeping used by production.
"""
import argparse
import gc
import hashlib
import json
import logging
import os
import resource
import sys
import threading
import time
from collections import Counter
from pathlib import Path
from types import SimpleNamespace

sys.path.insert(0, "/opt/netbox/netbox")
os.environ.setdefault("DJANGO_SETTINGS_MODULE", "netbox.settings")

import django  # noqa: E402

django.setup()

from core.models import ObjectChange  # noqa: E402
from dcim.models import (  # noqa: E402
    Cable,
    Interface,
    InventoryItem,
    MACAddress,
    Site,
)
from django.apps import apps  # noqa: E402
from django.conf import settings  # noqa: E402
from django.contrib.auth import get_user_model  # noqa: E402
from django.db import connections, transaction  # noqa: E402
from ipam.models import (  # noqa: E402
    IPAddress,
    Prefix,
)
from netbox.context import current_request  # noqa: E402
from netbox_branching.models import (  # noqa: E402
    AppliedChange,
    Branch,
    ChangeDiff,
)

from forward_netbox.choices import FORWARD_SUPPORTED_MODELS  # noqa: E402
from forward_netbox.models import (  # noqa: E402
    ForwardContributorBaseline,
    ForwardDeviceIdentity,
    ForwardIngestion,
    ForwardSource,
    ForwardSync,
    ForwardWorkloadState,
)
from forward_netbox.utilities.branch_budget import (  # noqa: E402
    BranchWorkload,
    build_branch_plan,
)
from forward_netbox.utilities.branch_lifecycle import run_item_in_branch  # noqa: E402
from forward_netbox.utilities.branching import build_branch_request  # noqa: E402
from forward_netbox.utilities.executor_base import ForwardExecutorBase  # noqa: E402
from forward_netbox.utilities.fast_baseline import run_fast_baseline_load  # noqa: E402
from forward_netbox.utilities.logging import SyncLogging  # noqa: E402
from forward_netbox.utilities.sync_contracts import (  # noqa: E402
    default_coalesce_fields_for_model,
)
from forward_netbox.utilities.workload_state import stage_workload_states  # noqa: E402

CONFIRMATION = "fnb-fast-baseline-disposable"
ACTUAL_COUNTS = {
    "dcim.device": 3_400,
    "dcim.interface": 535_777,
    "dcim.macaddress": 277_915,
    "dcim.inventoryitem": 82_572,
    "netbox_dlm.vulnerability": 70_230,
    "ipam.ipaddress": 51_944,
    "ipam.prefix": 34_388,
    "dcim.cable": 23_083,
}
ACTUAL_LOGICAL_CHANGES = 1_173_589
REFERENCE_COUNTS = {
    "dcim.site": 34,
    "dcim.manufacturer": 10,
    "dcim.devicerole": 5,
    "dcim.platform": 10,
    "dcim.devicetype": 100,
}
LOAD_TABLES = (
    "dcim_cable",
    "dcim_cablepath",
    "dcim_cabletermination",
    "dcim_device",
    "dcim_interface",
    "dcim_inventoryitem",
    "dcim_inventoryitemrole",
    "dcim_macaddress",
    "dcim_manufacturer",
    "dcim_platform",
    "dcim_site",
    "dcim_devicerole",
    "dcim_devicetype",
    "extras_cachedvalue",
    "ipam_ipaddress",
    "ipam_prefix",
    "netbox_dlm_cve",
    "netbox_dlm_cve_affected_software",
    "netbox_dlm_devicesoftware",
    "netbox_dlm_softwareversion",
    "netbox_dlm_vulnerability",
)


def scaled_count(value, scale, *, minimum=1):
    return max(minimum, round(value * scale))


def mac_for(value):
    value += 1
    return f"02:{(value >> 32) & 0xFF:02X}:{(value >> 24) & 0xFF:02X}:{(value >> 16) & 0xFF:02X}:{(value >> 8) & 0xFF:02X}:{value & 0xFF:02X}"


def ensure_disposable_database(confirmation):
    database = settings.DATABASES["default"]
    if confirmation != CONFIRMATION or database.get("HOST") not in {
        "postgres",
        "fb-postgres",
    }:
        raise SystemExit(
            "Refusing to run: pass the disposable confirmation and use the "
            "isolated Docker PostgreSQL host named 'postgres'."
        )


class QueryCounter:
    def __init__(self):
        self.execute_calls = 0
        self.by_verb = Counter()

    def __call__(self, execute, sql, params, many, context):
        self.execute_calls += 1
        verb = str(sql).lstrip().split(None, 1)[0].upper() if sql else "UNKNOWN"
        self.by_verb[verb] += 1
        return execute(sql, params, many, context)


class RSSMonitor:
    def __init__(self):
        self.baseline = self._current()
        self.peak = self.baseline
        self.stop_event = threading.Event()
        self.thread = threading.Thread(target=self._run, daemon=True)

    @staticmethod
    def _current():
        with open("/proc/self/statm", encoding="utf-8") as handle:
            _, resident, *_ = map(int, handle.read().split())
        return resident * os.sysconf("SC_PAGE_SIZE")

    def _run(self):
        while not self.stop_event.wait(0.01):
            self.peak = max(self.peak, self._current())

    def __enter__(self):
        self.thread.start()
        return self

    def __exit__(self, exc_type, exc, traceback):
        self.stop_event.set()
        self.thread.join()
        self.peak = max(self.peak, self._current())


class BenchmarkLogging(SyncLogging):
    def __init__(self, *, sample_interval):
        super().__init__()
        self.sample_interval = max(1, int(sample_interval))
        self.started = None
        self.applied_total = 0
        self.next_sample = self.sample_interval
        self.rate_samples = []
        self.phase = "setup"
        self.last_sample_applied = 0
        self.last_sample_elapsed = 0.0

    def start_sampling(self):
        self.started = time.perf_counter()

    def set_phase(self, phase):
        self.phase = str(phase)

    def increment_statistics(self, model_string, *, outcome="applied", amount=1):
        super().increment_statistics(model_string, outcome=outcome, amount=amount)
        if outcome != "applied" or self.started is None:
            return
        self.applied_total += max(0, int(amount or 0))
        if self.applied_total < self.next_sample:
            return
        elapsed = time.perf_counter() - self.started
        interval_rows = self.applied_total - self.last_sample_applied
        interval_seconds = elapsed - self.last_sample_elapsed
        self.rate_samples.append(
            {
                "phase": self.phase,
                "model": model_string,
                "applied": self.applied_total,
                "elapsed_seconds": elapsed,
                "overall_rows_per_second": self.applied_total / elapsed,
                "interval_rows_per_second": interval_rows / interval_seconds,
            }
        )
        self.last_sample_applied = self.applied_total
        self.last_sample_elapsed = elapsed
        while self.next_sample <= self.applied_total:
            self.next_sample += self.sample_interval


def cpu_snapshot():
    usage = resource.getrusage(resource.RUSAGE_SELF)
    return usage.ru_utime, usage.ru_stime


def wal_lsn():
    with connections["default"].cursor() as cursor:
        cursor.execute("SELECT pg_current_wal_lsn()")
        return str(cursor.fetchone()[0])


def wal_bytes(before, after):
    with connections["default"].cursor() as cursor:
        cursor.execute("SELECT pg_wal_lsn_diff(%s, %s)", [after, before])
        return int(cursor.fetchone()[0])


def _secondary_index_definitions():
    with connections["default"].cursor() as cursor:
        cursor.execute(
            """
            SELECT quote_ident(indexname), indexdef
            FROM pg_indexes i
            JOIN pg_class c ON c.relname = i.indexname
            JOIN pg_index x ON x.indexrelid = c.oid
            LEFT JOIN pg_constraint k ON k.conindid = c.oid
            WHERE i.schemaname = 'public'
              AND i.tablename = ANY(%s)
              AND NOT x.indisunique
              AND k.oid IS NULL
            ORDER BY i.tablename, i.indexname
            """,
            [list(LOAD_TABLES)],
        )
        return cursor.fetchall()


def _drop_secondary_indexes():
    definitions = _secondary_index_definitions()
    with connections["default"].cursor() as cursor:
        for quoted_name, _ in definitions:
            cursor.execute(f"DROP INDEX {quoted_name}")
    return definitions


def _restore_secondary_indexes(definitions):
    with connections["default"].cursor() as cursor:
        for _, definition in definitions:
            cursor.execute(definition)


def _set_autovacuum(enabled):
    value = "true" if enabled else "false"
    quote = connections["default"].ops.quote_name
    with connections["default"].cursor() as cursor:
        for table in LOAD_TABLES:
            cursor.execute(
                f"ALTER TABLE {quote(table)} SET (autovacuum_enabled = {value})"
            )


def interface_identity(index, device_count):
    device_index = index % device_count
    ordinal = index // device_count
    return f"fb-device-{device_index:05d}", f"Ethernet{ordinal}"


def build_workloads(scale):
    counts = {
        model: scaled_count(count, scale, minimum=(2 if model == "dcim.device" else 1))
        for model, count in ACTUAL_COUNTS.items()
    }
    reference_counts = {
        model: scaled_count(count, scale) for model, count in REFERENCE_COUNTS.items()
    }
    device_count = counts["dcim.device"]
    interface_count = max(counts["dcim.interface"], counts["dcim.cable"] * 2)
    counts["dcim.interface"] = interface_count

    sites = [
        {"name": f"FB Site {index}", "slug": f"fb-site-{index}"}
        for index in range(reference_counts["dcim.site"])
    ]
    manufacturers = [
        {"name": f"FB Manufacturer {index}", "slug": f"fb-manufacturer-{index}"}
        for index in range(reference_counts["dcim.manufacturer"])
    ]
    roles = [
        {"name": f"FB Role {index}", "slug": f"fb-role-{index}", "color": "607d8b"}
        for index in range(reference_counts["dcim.devicerole"])
    ]
    platforms = [
        {"name": f"FB Platform {index}", "slug": f"fb-platform-{index}"}
        for index in range(reference_counts["dcim.platform"])
    ]
    device_types = []
    for index in range(reference_counts["dcim.devicetype"]):
        manufacturer_index = index % len(manufacturers)
        device_types.append(
            {
                "manufacturer": manufacturers[manufacturer_index]["name"],
                "manufacturer_slug": manufacturers[manufacturer_index]["slug"],
                "model": f"FB Device Type {index}",
                "slug": f"fb-device-type-{index}",
            }
        )
    devices = []
    for index in range(device_count):
        site = sites[index % len(sites)]
        manufacturer = manufacturers[index % len(manufacturers)]
        role = roles[index % len(roles)]
        platform = platforms[index % len(platforms)]
        device_type = device_types[index % len(device_types)]
        devices.append(
            {
                "name": f"fb-device-{index:05d}",
                "manufacturer": manufacturer["name"],
                "manufacturer_slug": manufacturer["slug"],
                "device_type": device_type["model"],
                "device_type_slug": device_type["slug"],
                "site": site["name"],
                "site_slug": site["slug"],
                "role": role["name"],
                "role_slug": role["slug"],
                "role_color": role["color"],
                "platform": platform["name"],
                "platform_slug": platform["slug"],
                "status": "active",
            }
        )
    interfaces = []
    for index in range(interface_count):
        device_name, interface_name = interface_identity(index, device_count)
        interfaces.append(
            {
                "device": device_name,
                "name": interface_name,
                "type": "1000base-t",
                "enabled": True,
            }
        )
    inventory = []
    inventory_role_names = (
        "TRANSCEIVER",
        "POWER SUPPLY",
        "FAN MODULE",
        "CHASSIS",
        "MOTHERBOARD",
    )
    for index in range(counts["dcim.inventoryitem"]):
        device_index = index % device_count
        ordinal = index // device_count
        manufacturer = manufacturers[device_index % len(manufacturers)]
        role_name = inventory_role_names[index % len(inventory_role_names)]
        role_slug = role_name.lower().replace(" ", "-")
        inventory.append(
            {
                "device": f"fb-device-{device_index:05d}",
                "name": f"Component {ordinal}",
                "part_id": f"PID-{ordinal % 500:04d}",
                "serial": f"SER-{index:09d}",
                "status": "active",
                "discovered": True,
                "manufacturer": manufacturer["name"],
                "manufacturer_slug": manufacturer["slug"],
                "role": role_name,
                "role_slug": role_slug,
                "role_color": "607d8b",
                "part_type": role_name,
                "module_component": False,
            }
        )
    ip_addresses = []
    for index in range(counts["ipam.ipaddress"]):
        device_name, interface_name = interface_identity(index, device_count)
        second = 64 + ((index // 65_536) % 64)
        third = (index // 256) % 256
        fourth = index % 256
        ip_addresses.append(
            {
                "device": device_name,
                "interface": interface_name,
                "address": f"100.{second}.{third}.{fourth}/32",
                "status": "active",
                "vrf": None,
            }
        )
    prefixes = []
    for index in range(counts["ipam.prefix"]):
        second = 16 + ((index // 65_536) % 16)
        third = (index // 256) % 256
        fourth = index % 256
        prefixes.append(
            {
                "prefix": f"172.{second}.{third}.{fourth}/32",
                "vrf": None,
                "status": "active",
            }
        )
    macs = []
    for index in range(counts["dcim.macaddress"]):
        device_name, interface_name = interface_identity(index, device_count)
        macs.append(
            {
                "device": device_name,
                "interface": interface_name,
                "mac": mac_for(index),
            }
        )
    cables = []
    for index in range(counts["dcim.cable"]):
        device_name, interface_name = interface_identity(index * 2, device_count)
        remote_device, remote_interface = interface_identity(
            index * 2 + 1, device_count
        )
        cables.append(
            {
                "device": device_name,
                "interface": interface_name,
                "remote_device": remote_device,
                "remote_interface": remote_interface,
                "status": "connected",
            }
        )
    vulnerabilities = []
    for index in range(counts["netbox_dlm.vulnerability"]):
        device_index = index % device_count
        platform = platforms[device_index % len(platforms)]
        version_slot = device_index % 10
        vulnerabilities.append(
            {
                "name": f"fb-device-{device_index:05d}",
                "cve_id": f"CVE-2099-{index + 1:07d}",
                "platform": platform["name"],
                "platform_slug": platform["slug"],
                "version": f"v{version_slot}",
            }
        )

    rows_by_model = {
        "dcim.site": sites,
        "dcim.manufacturer": manufacturers,
        "dcim.devicerole": roles,
        "dcim.platform": platforms,
        "dcim.devicetype": device_types,
        "ipam.prefix": prefixes,
        "dcim.device": devices,
        "dcim.interface": interfaces,
        "dcim.inventoryitem": inventory,
        "ipam.ipaddress": ip_addresses,
        "dcim.macaddress": macs,
        "dcim.cable": cables,
        "netbox_dlm.vulnerability": vulnerabilities,
    }
    workloads = [
        BranchWorkload(
            model_string=model,
            label=model,
            upsert_rows=rows,
            sync_mode="full",
            coalesce_fields=[
                list(fields) for fields in default_coalesce_fields_for_model(model)
            ],
        )
        for model, rows in rows_by_model.items()
        if rows
    ]
    source_rows = sum(len(rows) for rows in rows_by_model.values())
    return workloads, counts, reference_counts, source_rows


def create_runtime(*, engine, scale, logger):
    user = get_user_model().objects.create_user(
        username=f"fb-{engine}-{time.time_ns()}"
    )
    source = ForwardSource.objects.create(
        name=f"fb-{engine}-source-{time.time_ns()}",
        type="saas",
        url="https://fwd.app",
        status="ready",
        parameters={"network_id": "synthetic"},
    )
    model_parameters = {model: False for model in FORWARD_SUPPORTED_MODELS}
    for model in (
        "dcim.site",
        "dcim.manufacturer",
        "dcim.devicerole",
        "dcim.platform",
        "dcim.devicetype",
        "ipam.prefix",
        "dcim.device",
        "dcim.interface",
        "dcim.inventoryitem",
        "ipam.ipaddress",
        "dcim.macaddress",
        "dcim.cable",
        "netbox_dlm.vulnerability",
    ):
        model_parameters[model] = True
    sync = ForwardSync.objects.create(
        name=f"fb-{engine}-sync-{time.time_ns()}",
        source=source,
        user=user,
        auto_merge=True,
        parameters={
            **model_parameters,
            "snapshot_id": "latestProcessed",
            "auto_merge": True,
            "enable_bulk_orm": True,
            "enable_fast_baseline_load": engine == "fast",
            "max_changes_per_staging_item": 10_000,
        },
    )
    executor = ForwardExecutorBase(sync, client=None, logger_=logger, user=user)
    return user, sync, executor


def runtime_context(scale, workloads):
    model_results = [
        {
            "model": workload.model_string,
            "sync_mode": "full",
            "row_count": len(workload.upsert_rows),
        }
        for workload in workloads
    ]
    context = SimpleNamespace(
        as_dict=lambda: {
            "snapshot_selector": "latestProcessed",
            "snapshot_id": f"synthetic-customer-{scale}",
            "snapshot_info": {"fixture": "anonymized-customer-shape", "scale": scale},
            "snapshot_metrics": {},
            "scoped_matched_tags": {},
        }
    )
    return context, model_results


def run_fast(executor, context, workloads):
    executor.logger.set_phase("fast_load")
    executor.last_model_results = runtime_context(1, workloads)[1]
    fetcher = SimpleNamespace(
        pending_workload_states=[],
        stage_pending_contributor_baseline=lambda ingestion, context: 0,
    )
    ingestion, decision = run_fast_baseline_load(
        executor,
        context=context,
        workloads=workloads,
        fetcher=fetcher,
    )
    if ingestion is None:
        raise AssertionError(
            {"fast_baseline_rejected": decision.reason_code, **decision.context}
        )
    return ingestion, {"eligibility": decision.reason_code}


def run_current(executor, context, workloads, user):
    executor.last_model_results = runtime_context(1, workloads)[1]
    request = build_branch_request(user)
    ingestion = executor._create_ingestion(
        context.as_dict(), change_request_id=request.id
    )
    stage_workload_states(ingestion, [])
    branch = Branch(name=f"customer current baseline {time.time_ns()}")
    provision_started = time.perf_counter()
    branch.save(provision=False)
    branch.provision(user=user)
    provision_seconds = time.perf_counter() - provision_started
    branch.refresh_from_db()
    ingestion.branch = branch
    ingestion.save(update_fields=["branch"])
    plan = build_branch_plan(
        workloads,
        max_changes_per_staging_item=executor.sync.get_max_changes_per_staging_item(),
        oversized_bucket_policy="warn",
    )
    executor.logger.set_phase("current_stage")
    stage_started = time.perf_counter()
    for item in plan:
        run_item_in_branch(
            executor,
            item,
            context.as_dict(),
            ingestion,
            branch,
            total_plan_items=len(plan),
        )
    stage_seconds = time.perf_counter() - stage_started
    logical_changes = (
        branch.get_unmerged_changes()
        .values("changed_object_type_id", "changed_object_id")
        .distinct()
        .count()
    )
    executor.logger.set_phase("current_merge")
    merge_started = time.perf_counter()
    ingestion.sync_merge(remove_branch=True)
    merge_seconds = time.perf_counter() - merge_started
    return ingestion, {
        "branch_provision_seconds": provision_seconds,
        "stage_seconds": stage_seconds,
        "merge_seconds": merge_seconds,
        "observed_logical_changes": logical_changes,
    }


def final_counts():
    Vulnerability = apps.get_model("netbox_dlm", "Vulnerability")
    return {
        "dcim.site": Site.objects.count(),
        "dcim.interface": Interface.objects.count(),
        "dcim.inventoryitem": InventoryItem.objects.count(),
        "dcim.macaddress": MACAddress.objects.count(),
        "dcim.cable": Cable.objects.count(),
        "ipam.ipaddress": IPAddress.objects.count(),
        "ipam.prefix": Prefix.objects.count(),
        "netbox_dlm.vulnerability": Vulnerability.objects.count(),
    }


def side_counts():
    from dcim.models import CablePath, CableTermination, InventoryItemRole
    from extras.models import CachedValue

    return {
        "dcim.cabletermination": CableTermination.objects.count(),
        "dcim.cablepath": CablePath.objects.count(),
        "dcim.inventoryitemrole": InventoryItemRole.objects.count(),
        "extras.cachedvalue": CachedValue.objects.count(),
        "netbox_dlm.softwareversion": apps.get_model(
            "netbox_dlm", "SoftwareVersion"
        ).objects.count(),
        "netbox_dlm.devicesoftware": apps.get_model(
            "netbox_dlm", "DeviceSoftware"
        ).objects.count(),
        "netbox_dlm.cve": apps.get_model("netbox_dlm", "CVE").objects.count(),
        "netbox_dlm.cve_affected_software": apps.get_model(
            "netbox_dlm", "CVE"
        ).affected_software.through.objects.count(),
    }


def _queryset_fingerprint(queryset):
    digest = hashlib.sha256()
    count = 0
    for row in queryset.iterator(chunk_size=5_000):
        digest.update(
            json.dumps(row, default=str, separators=(",", ":")).encode("utf-8")
        )
        digest.update(b"\n")
        count += 1
    return {"count": count, "sha256": digest.hexdigest()}


def semantic_fingerprints():
    """Bounded-memory canonical state proof, excluding volatile timestamps."""
    from dcim.models import (
        CablePath,
        CableTermination,
        Device,
        DeviceRole,
        DeviceType,
        InventoryItemRole,
        Manufacturer,
        Platform,
    )
    from extras.models import CachedValue

    Vulnerability = apps.get_model("netbox_dlm", "Vulnerability")
    SoftwareVersion = apps.get_model("netbox_dlm", "SoftwareVersion")
    DeviceSoftware = apps.get_model("netbox_dlm", "DeviceSoftware")
    CVE = apps.get_model("netbox_dlm", "CVE")
    through = CVE.affected_software.through
    querysets = {
        "site": Site.objects.order_by("slug").values_list(
            "name", "slug", "status", "description"
        ),
        "manufacturer": Manufacturer.objects.order_by("slug").values_list(
            "name", "slug", "description"
        ),
        "device_role": DeviceRole.objects.order_by("slug").values_list(
            "name", "slug", "color", "vm_role"
        ),
        "platform": Platform.objects.order_by("slug").values_list(
            "name", "slug", "manufacturer__slug", "description"
        ),
        "device_type": DeviceType.objects.order_by("slug").values_list(
            "manufacturer__slug", "model", "slug", "part_number"
        ),
        "device": Device.objects.order_by("name").values_list(
            "name",
            "site__slug",
            "role__slug",
            "device_type__slug",
            "platform__slug",
            "status",
            "serial",
            "asset_tag",
        ),
        "interface": Interface.objects.order_by("device__name", "name").values_list(
            "device__name",
            "name",
            "label",
            "type",
            "enabled",
            "mtu",
            "description",
            "_path_id",
        ),
        "inventory": InventoryItem.objects.order_by(
            "device__name", "name", "part_id", "serial"
        ).values_list(
            "device__name",
            "name",
            "label",
            "description",
            "status",
            "role__slug",
            "manufacturer__slug",
            "part_id",
            "serial",
            "asset_tag",
            "discovered",
            "parent_id",
            "component_type_id",
            "component_id",
            "_site__slug",
            "_location_id",
            "_rack_id",
            "lft",
            "rght",
            "tree_id",
            "level",
        ),
        "inventory_role": InventoryItemRole.objects.order_by("slug").values_list(
            "name", "slug", "color", "description", "comments"
        ),
        "mac": MACAddress.objects.order_by("mac_address").values_list(
            "mac_address",
            "assigned_object_type__app_label",
            "assigned_object_type__model",
            "assigned_object_id",
            "description",
        ),
        "ip_address": IPAddress.objects.order_by("address").values_list(
            "address",
            "status",
            "vrf__name",
            "assigned_object_type__app_label",
            "assigned_object_type__model",
            "assigned_object_id",
        ),
        "prefix": Prefix.objects.order_by("prefix").values_list(
            "prefix", "status", "vrf__name", "_depth", "_children"
        ),
        "cable": Cable.objects.order_by("id").values_list(
            "id", "status", "type", "label", "color", "profile"
        ),
        "cable_termination": CableTermination.objects.order_by(
            "cable_id", "cable_end"
        ).values_list(
            "cable_id",
            "cable_end",
            "termination_type__app_label",
            "termination_type__model",
            "termination_id",
            "_device__name",
            "_site__slug",
            "_location_id",
            "_rack_id",
        ),
        "cable_path": CablePath.objects.order_by("id").values_list(
            "id", "path", "is_active", "is_complete", "is_split", "_nodes"
        ),
        "software_version": SoftwareVersion.objects.order_by(
            "platform__slug", "version"
        ).values_list(
            "platform__slug",
            "version",
            "alias",
            "release_date",
            "end_of_support",
            "long_term_support",
            "documentation_url",
            "comments",
        ),
        "device_software": DeviceSoftware.objects.order_by("device__name").values_list(
            "device__name",
            "software_version__platform__slug",
            "software_version__version",
        ),
        "cve": CVE.objects.order_by("cve_id").values_list(
            "cve_id", "name", "description", "status", "severity"
        ),
        "vulnerability": Vulnerability.objects.order_by(
            "device__name", "cve__cve_id"
        ).values_list(
            "device__name",
            "cve__cve_id",
            "software_version__platform__slug",
            "software_version__version",
            "status",
            "comments",
        ),
        "cve_affected_software": through.objects.order_by("pk").values_list(
            *[
                field.attname
                for field in through._meta.fields
                if field.primary_key is False
            ]
        ),
        "cached_value": CachedValue.objects.order_by(
            "object_type__app_label", "object_type__model", "object_id", "field"
        ).values_list(
            "object_type__app_label",
            "object_type__model",
            "object_id",
            "field",
            "type",
            "value",
            "weight",
        ),
    }
    return {
        name: _queryset_fingerprint(queryset) for name, queryset in querysets.items()
    }


def measure(args):
    ensure_disposable_database(args.confirm_disposable_database)
    if ForwardIngestion.objects.exists() or any(
        model.objects.exists() for model in (Site, Interface, InventoryItem, MACAddress)
    ):
        raise SystemExit("Disposable database is not empty; reset it before this cell.")
    logging.disable(logging.WARNING)
    current_request.set(None)
    workloads, requested_counts, reference_counts, source_rows = build_workloads(
        args.scale
    )
    sample_interval = max(1_000, source_rows // 100)
    logger = BenchmarkLogging(sample_interval=sample_interval)
    user, sync, executor = create_runtime(
        engine=args.engine,
        scale=args.scale,
        logger=logger,
    )
    context, model_results = runtime_context(args.scale, workloads)
    executor.last_model_results = model_results
    expected_logical_changes = scaled_count(ACTUAL_LOGICAL_CHANGES, args.scale)

    gc.collect()
    counter = QueryCounter()
    connections["default"].ensure_connection()
    before_cpu = cpu_snapshot()
    before_wal = wal_lsn()
    logger.start_sampling()
    with connections["default"].execute_wrapper(counter), RSSMonitor() as rss:
        started = time.perf_counter()
        if args.remedy == "synchronous_commit_off":
            with connections["default"].cursor() as cursor:
                cursor.execute("SET synchronous_commit TO off")
        elif args.remedy == "autovacuum_off":
            if args.engine != "fast":
                raise SystemExit("The selected remedy is fast-engine only.")
            _set_autovacuum(False)
        definitions = []
        if args.remedy == "drop_secondary_indexes":
            if args.engine != "fast":
                raise SystemExit("The selected remedy is fast-engine only.")
            definitions = _drop_secondary_indexes()
        try:
            if args.remedy == "defer_constraints":
                if args.engine != "fast":
                    raise SystemExit("The selected remedy is fast-engine only.")
                with transaction.atomic():
                    with connections["default"].cursor() as cursor:
                        cursor.execute("SET CONSTRAINTS ALL DEFERRED")
                    ingestion, phases = run_fast(executor, context, workloads)
            elif args.engine == "fast":
                ingestion, phases = run_fast(executor, context, workloads)
            else:
                ingestion, phases = run_current(executor, context, workloads, user)
        finally:
            if args.remedy == "synchronous_commit_off":
                with connections["default"].cursor() as cursor:
                    cursor.execute("SET synchronous_commit TO on")
            elif args.remedy == "autovacuum_off":
                _set_autovacuum(True)
            if definitions:
                _restore_secondary_indexes(definitions)
        wall_seconds = time.perf_counter() - started
    after_wal = wal_lsn()
    after_cpu = cpu_snapshot()
    ingestion.refresh_from_db()
    sync.refresh_from_db()
    observed_counts = final_counts()
    observed_side_counts = side_counts()
    fingerprints = semantic_fingerprints()
    verification = {
        "baseline_ready": ingestion.baseline_ready,
        "sync_status": sync.status,
        "branch_id": ingestion.branch_id,
        "issues": ingestion.issues.count(),
        "target_counts": observed_counts,
        "side_counts": observed_side_counts,
        "semantic_fingerprints": fingerprints,
        "ingestion_change_totals": {
            "applied": ingestion.applied_change_count,
            "failed": ingestion.failed_change_count,
            "created": ingestion.created_change_count,
            "updated": ingestion.updated_change_count,
            "deleted": ingestion.deleted_change_count,
        },
        "model_results": ingestion.model_results,
        "fast_baseline_attestation": (ingestion.snapshot_info or {}).get(
            "fast_baseline_load"
        ),
        "durable_state_counts": {
            "workload_states": ForwardWorkloadState.objects.count(),
            "current_workload_states": ForwardWorkloadState.objects.filter(
                is_current=True
            ).count(),
            "contributor_baselines": ForwardContributorBaseline.objects.count(),
            "device_identities": ForwardDeviceIdentity.objects.count(),
        },
        "object_changes": ObjectChange.objects.count(),
        "applied_changes": AppliedChange.objects.count(),
        "change_diffs": ChangeDiff.objects.count(),
        "passed": (
            ingestion.baseline_ready
            and sync.status == "completed"
            and ingestion.branch_id is None
            and observed_counts["dcim.interface"] == requested_counts["dcim.interface"]
            and observed_counts["dcim.inventoryitem"]
            == requested_counts["dcim.inventoryitem"]
            and observed_counts["dcim.macaddress"]
            == requested_counts["dcim.macaddress"]
            and observed_counts["dcim.cable"] == requested_counts["dcim.cable"]
            and observed_counts["ipam.ipaddress"] == requested_counts["ipam.ipaddress"]
            and observed_counts["ipam.prefix"] == requested_counts["ipam.prefix"]
            and observed_counts["netbox_dlm.vulnerability"]
            == requested_counts["netbox_dlm.vulnerability"]
        ),
    }
    if not verification["passed"]:
        raise AssertionError(verification)
    result = {
        "schema_version": 1,
        "engine": args.engine,
        "remedy": args.remedy,
        "scale": args.scale,
        "fixture": {
            "basis": "deterministic anonymized customer-shaped baseline",
            "actual_model_counts": ACTUAL_COUNTS,
            "requested_model_counts": requested_counts,
            "reference_counts": reference_counts,
            "source_rows": source_rows,
            "logical_change_denominator": expected_logical_changes,
        },
        "measurement": {
            "wall_seconds": wall_seconds,
            "rows_per_second": expected_logical_changes / wall_seconds,
            "statements": counter.execute_calls,
            "statements_per_change": counter.execute_calls / expected_logical_changes,
            "peak_rss_mib": rss.peak / 1024 / 1024,
            "baseline_rss_mib": rss.baseline / 1024 / 1024,
            "incremental_peak_rss_mib": (rss.peak - rss.baseline) / 1024 / 1024,
            "python_user_seconds": after_cpu[0] - before_cpu[0],
            "python_system_seconds": after_cpu[1] - before_cpu[1],
            "wal_bytes": wal_bytes(before_wal, after_wal),
            "sql_by_verb": dict(sorted(counter.by_verb.items())),
            "rate_samples": logger.rate_samples,
            "phases": phases,
        },
        "verification": verification,
    }
    Path(args.output).write_text(json.dumps(result, indent=2, sort_keys=True) + "\n")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--engine", choices=("fast", "current"), required=True)
    parser.add_argument("--scale", type=float, required=True)
    parser.add_argument("--output", required=True)
    parser.add_argument(
        "--remedy",
        choices=(
            "none",
            "synchronous_commit_off",
            "drop_secondary_indexes",
            "defer_constraints",
            "autovacuum_off",
        ),
        default="none",
    )
    parser.add_argument("--confirm-disposable-database", required=True)
    args = parser.parse_args()
    if not 0 < args.scale <= 1:
        raise SystemExit("--scale must be in (0, 1].")
    measure(args)


if __name__ == "__main__":
    main()
