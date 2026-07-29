#!/usr/bin/env python
"""Disposable two-engine benchmark for the MAC branch-merge path."""
import argparse
import gc
import importlib.metadata
import json
import logging
import os
import resource
import statistics
import sys
import threading
import time
from contextlib import ExitStack
from pathlib import Path
from unittest.mock import patch

sys.path.insert(0, "/opt/netbox/netbox")
os.environ.setdefault("DJANGO_SETTINGS_MODULE", "netbox.settings")

import django  # noqa: E402

django.setup()

from core.models import ObjectChange  # noqa: E402
from dcim.models import Device  # noqa: E402
from dcim.models import DeviceRole  # noqa: E402
from dcim.models import DeviceType  # noqa: E402
from dcim.models import Interface  # noqa: E402
from dcim.models import MACAddress  # noqa: E402
from dcim.models import Manufacturer  # noqa: E402
from dcim.models import Site  # noqa: E402
from django.conf import settings  # noqa: E402
from django.contrib.auth import get_user_model  # noqa: E402
from django.contrib.contenttypes.models import ContentType  # noqa: E402
from django.db import connections  # noqa: E402
from netbox.context import current_request  # noqa: E402
from netbox.context_managers import event_tracking  # noqa: E402
from netbox_branching.models import AppliedChange  # noqa: E402
from netbox_branching.models import Branch  # noqa: E402
from netbox_branching.models import ChangeDiff  # noqa: E402
from netbox_branching.utilities import activate_branch  # noqa: E402

from forward_netbox.models import ForwardIngestion  # noqa: E402
from forward_netbox.models import ForwardSource  # noqa: E402
from forward_netbox.models import ForwardSync  # noqa: E402
from forward_netbox.utilities.apply_engine import select_apply_engine  # noqa: E402
from forward_netbox.utilities.branching import build_branch_request  # noqa: E402
from forward_netbox.utilities.logging import SyncLogging  # noqa: E402
from forward_netbox.utilities.merge import merge_branch  # noqa: E402
from forward_netbox.utilities.merge_set_based import (  # noqa: E402
    apply_set_based_mac_range,
    set_based_merge_decision,
)
from forward_netbox.utilities.sync import ForwardSyncRunner  # noqa: E402


MODEL_STRING = "dcim.macaddress"
CONFIRMATION = "fnb-setmerge-wpa-disposable"
MIX = {"create": 0.55, "update": 0.25, "noop": 0.10, "delete": 0.10}


def mac_for(value):
    value += 1
    return "02:%02X:%02X:%02X:%02X:%02X" % (
        (value >> 32) & 0xFF,
        (value >> 24) & 0xFF,
        (value >> 16) & 0xFF,
        (value >> 8) & 0xFF,
        value & 0xFF,
    )


def operation_counts(total):
    counts = {name: int(total * ratio) for name, ratio in MIX.items()}
    counts["create"] += total - sum(counts.values())
    return counts


class QueryCounter:
    def __init__(self):
        self.execute_calls = 0
        self.by_verb = {}

    def __call__(self, execute, sql, params, many, context):
        self.execute_calls += 1
        verb = str(sql).lstrip().split(None, 1)[0].upper() if sql else "UNKNOWN"
        self.by_verb[verb] = self.by_verb.get(verb, 0) + 1
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
        while not self.stop_event.wait(0.005):
            self.peak = max(self.peak, self._current())

    def __enter__(self):
        self.thread.start()
        return self

    def __exit__(self, exc_type, exc, traceback):
        self.stop_event.set()
        self.thread.join()
        self.peak = max(self.peak, self._current())


def cpu_snapshot():
    usage = resource.getrusage(resource.RUSAGE_SELF)
    return usage.ru_utime, usage.ru_stime


def ensure_disposable_database(confirmation):
    database = settings.DATABASES["default"]
    if confirmation != CONFIRMATION or database.get("HOST") != "postgres":
        raise SystemExit(
            "Refusing to run: pass the disposable confirmation and use the "
            "isolated Docker PostgreSQL host named 'postgres'."
        )


def create_fixture(*, engine, round_number, total, namespace_offset):
    suffix = f"smb-{total}-r{round_number}-{engine}-{time.time_ns() % 1_000_000}"
    user = get_user_model().objects.create_user(username=f"{suffix}-user")
    manufacturer = Manufacturer.objects.create(
        name=f"{suffix} manufacturer", slug=f"{suffix}-manufacturer"
    )
    device_type = DeviceType.objects.create(
        manufacturer=manufacturer,
        model=f"{suffix} device type",
        slug=f"{suffix}-device-type",
    )
    role = DeviceRole.objects.create(name=f"{suffix} role", slug=f"{suffix}-role")
    site = Site.objects.create(name=f"{suffix} site", slug=f"{suffix}-site")
    devices = [
        Device(
            name=f"{suffix}-device-{index:03d}",
            device_type=device_type,
            role=role,
            site=site,
            status="active",
        )
        for index in range(25)
    ]
    Device.objects.bulk_create(devices)
    devices = list(Device.objects.filter(name__startswith=f"{suffix}-device-"))
    interfaces = []
    for device in devices:
        for index in range(4):
            interfaces.append(
                Interface(
                    device=device,
                    name=f"Ethernet{index}",
                    type="1000base-t",
                )
            )
    Interface.objects.bulk_create(interfaces)
    interfaces = list(
        Interface.objects.filter(device__in=devices)
        .select_related("device")
        .order_by("device__name", "name")
    )

    counts = operation_counts(total)
    base = namespace_offset + (total * 1000) + (round_number * 100_000_000)
    if engine == "set_based":
        base += 50_000_000
    addresses = [mac_for(base + index) for index in range(total)]
    indexes = {}
    offset = 0
    for operation in MIX:
        indexes[operation] = range(offset, offset + counts[operation])
        offset += counts[operation]
    interface_type = ContentType.objects.get_for_model(Interface)
    baseline_indexes = [
        index
        for operation in ("update", "noop", "delete")
        for index in indexes[operation]
    ]
    MACAddress.objects.bulk_create(
        [
            MACAddress(
                mac_address=addresses[index],
                assigned_object_type=interface_type,
                assigned_object_id=interfaces[index % len(interfaces)].pk,
            )
            for index in baseline_indexes
        ],
        batch_size=1000,
    )
    source = ForwardSource.objects.create(
        name=f"{suffix}-source",
        type="saas",
        url="https://fwd.app",
        status="ready",
        parameters={"network_id": "synthetic"},
    )
    sync = ForwardSync.objects.create(
        name=f"{suffix}-sync",
        source=source,
        user=user,
        auto_merge=False,
        parameters={
            "snapshot_id": "latestProcessed",
            "enable_bulk_orm": True,
            "enable_set_based_merge": engine == "set_based",
            "set_based_merge_kill_switches": [],
            MODEL_STRING: True,
        },
    )
    branch = Branch(name=f"MAC merge benchmark {suffix}")
    branch.save(provision=False)
    provision_started = time.perf_counter()
    branch.provision(user=user)
    provision_seconds = time.perf_counter() - provision_started
    branch.refresh_from_db()
    ingestion = ForwardIngestion.objects.create(
        sync=sync,
        snapshot_selector="latestProcessed",
        snapshot_id=f"synthetic-{suffix}",
        branch=branch,
    )
    return {
        "suffix": suffix,
        "user": user,
        "sync": sync,
        "branch": branch,
        "ingestion": ingestion,
        "interfaces": interfaces,
        "addresses": addresses,
        "indexes": indexes,
        "counts": counts,
        "provision_seconds": provision_seconds,
    }


def stage_fixture(fixture):
    interfaces = fixture["interfaces"]
    addresses = fixture["addresses"]
    indexes = fixture["indexes"]
    rows = []
    desired_interface_by_address = {}
    for operation in ("create", "update", "noop"):
        for index in indexes[operation]:
            desired = interfaces[(index + 1) % len(interfaces)]
            rows.append(
                {
                    "device": desired.device.name,
                    "interface": desired.name,
                    "mac": addresses[index],
                }
            )
            desired_interface_by_address[addresses[index]] = desired.pk
    deletes = [{"mac": addresses[index]} for index in indexes["delete"]]
    request = build_branch_request(fixture["user"])
    runner = ForwardSyncRunner(
        sync=fixture["sync"],
        ingestion=fixture["ingestion"],
        client=None,
        logger_=SyncLogging(),
    )
    with activate_branch(fixture["branch"]), event_tracking(request):
        token = current_request.set(request)
        try:
            apply_engine = select_apply_engine(
                sync=fixture["sync"], model_string=MODEL_STRING
            )
            apply_engine.apply_upserts(runner, MODEL_STRING, rows)
            apply_engine.apply_deletes(runner, MODEL_STRING, deletes)
        finally:
            current_request.reset(token)

    noop_addresses = [addresses[index] for index in indexes["noop"]]
    noop_targets = list(MACAddress.objects.filter(mac_address__in=noop_addresses))
    for target in noop_targets:
        target.assigned_object_id = desired_interface_by_address[
            str(target.mac_address)
        ]
    MACAddress.objects.bulk_update(
        noop_targets,
        fields=["assigned_object_id"],
        batch_size=1000,
    )
    staged = (
        fixture["branch"]
        .get_unmerged_changes()
        .values("changed_object_type_id", "changed_object_id")
        .distinct()
        .count()
    )
    if staged != sum(fixture["counts"].values()):
        raise AssertionError({"staged": staged, "counts": fixture["counts"]})


def measure_fixture(fixture, engine):
    decision = set_based_merge_decision(sync=fixture["sync"], branch=fixture["branch"])
    if engine == "set_based" and not decision.enabled:
        raise AssertionError(
            {"set_based_rejected": decision.reason_code, "context": decision.context}
        )
    gc.collect()
    counter = QueryCounter()
    connections["default"].ensure_connection()
    before_cpu = cpu_snapshot()
    fast_results = []

    def observe_set_based_range(**kwargs):
        result = apply_set_based_mac_range(**kwargs)
        fast_results.append(result)
        return result

    with ExitStack() as stack:
        stack.enter_context(connections["default"].execute_wrapper(counter))
        if engine == "set_based":
            stack.enter_context(
                patch(
                    "forward_netbox.utilities.merge_set_based."
                    "apply_set_based_mac_range",
                    side_effect=observe_set_based_range,
                )
            )
        with RSSMonitor() as rss:
            started = time.perf_counter()
            merge_branch(fixture["ingestion"], user=fixture["user"])
            wall_seconds = time.perf_counter() - started
    after_cpu = cpu_snapshot()
    copy_ops = 1 if engine == "set_based" else 0
    observed = counter.execute_calls + copy_ops
    return {
        "wall_seconds": wall_seconds,
        "python_user_seconds": after_cpu[0] - before_cpu[0],
        "python_system_seconds": after_cpu[1] - before_cpu[1],
        "python_total_seconds": sum(after_cpu) - sum(before_cpu),
        "peak_rss_mib": rss.peak / 1024 / 1024,
        "baseline_rss_mib": rss.baseline / 1024 / 1024,
        "incremental_peak_rss_mib": (rss.peak - rss.baseline) / 1024 / 1024,
        "database": {
            "django_execute_calls": counter.execute_calls,
            "copy_ops": copy_ops,
            "round_trips_observed": observed,
            "by_verb": dict(sorted(counter.by_verb.items())),
        },
        "set_based_ranges": {
            "count": len(fast_results),
            "applied": sum(len(result.applied) for result in fast_results),
            "fallback": sum(len(result.fallback) for result in fast_results),
            "operation_counts": {
                operation: sum(
                    result.operation_counts.get(operation, 0) for result in fast_results
                )
                for operation in ("I", "U", "N", "D")
            },
        },
    }


def verify_fixture(fixture, engine, metric):
    ingestion = fixture["ingestion"]
    ingestion.refresh_from_db()
    branch = fixture["branch"]
    branch.refresh_from_db()
    mac_type = ContentType.objects.get_for_model(MACAddress)
    target_count = MACAddress.objects.filter(
        mac_address__in=fixture["addresses"]
    ).count()
    expected_target_count = (
        sum(fixture["counts"].values()) - fixture["counts"]["delete"]
    )
    audits = ObjectChange.objects.filter(
        request_id=ingestion.change_request_id,
        changed_object_type=mac_type,
    )
    action_counts = {
        action: audits.filter(action=action).count()
        for action in ("create", "update", "delete")
    }
    applied_count = AppliedChange.objects.filter(
        branch=branch,
        change__request_id=ingestion.change_request_id,
        change__changed_object_type=mac_type,
    ).count()
    diff_count = ChangeDiff.objects.filter(branch=branch, object_type=mac_type).count()
    fast_ranges = metric["set_based_ranges"]
    expected_fast_operations = {
        "I": fixture["counts"]["create"],
        "U": fixture["counts"]["update"],
        "N": fixture["counts"]["noop"],
        "D": fixture["counts"]["delete"],
    }
    fast_path_passed = (
        fast_ranges["count"] == 0
        if engine == "current"
        else (
            fast_ranges["count"] >= 1
            and fast_ranges["applied"] == sum(fixture["counts"].values())
            and fast_ranges["fallback"] == 0
            and fast_ranges["operation_counts"] == expected_fast_operations
        )
    )
    passed = (
        target_count == expected_target_count
        and action_counts["create"] == fixture["counts"]["create"]
        and action_counts["update"] == fixture["counts"]["update"]
        and action_counts["delete"] == 0
        and applied_count == fixture["counts"]["create"] + fixture["counts"]["update"]
        and diff_count == sum(fixture["counts"].values())
        and ingestion.applied_change_count == sum(fixture["counts"].values())
        and ingestion.failed_change_count == 0
        and branch.status == "merged"
        and fast_path_passed
    )
    verification = {
        "passed": passed,
        "target_count": target_count,
        "expected_target_count": expected_target_count,
        "object_change_actions": action_counts,
        "applied_change_count": applied_count,
        "source_change_diff_count": diff_count,
        "ingestion_applied": ingestion.applied_change_count,
        "ingestion_failed": ingestion.failed_change_count,
        "branch_status": branch.status,
        "set_based_path_passed": fast_path_passed,
        "set_based_ranges": fast_ranges,
    }
    if not passed:
        raise AssertionError(verification)
    return verification


def measure(args):
    ensure_disposable_database(args.confirm_disposable_database)
    logging.disable(logging.WARNING)
    fixture = create_fixture(
        engine=args.engine,
        round_number=args.round,
        total=args.rows,
        namespace_offset=args.namespace_offset,
    )
    stage_started = time.perf_counter()
    stage_fixture(fixture)
    stage_seconds = time.perf_counter() - stage_started
    metric = measure_fixture(fixture, args.engine)
    verification = verify_fixture(fixture, args.engine, metric)
    total = sum(fixture["counts"].values())
    result = {
        "schema_version": 1,
        "engine": args.engine,
        "round": args.round,
        "volume": total,
        "runtime": {
            "netbox": settings.RELEASE.version,
            "branching": importlib.metadata.version("netboxlabs-netbox-branching"),
            "python": sys.version.split()[0],
        },
        "fixture": {
            "basis": "deterministic synthetic MAC merge mix",
            "operation_ratios": MIX,
            "operation_counts": fixture["counts"],
        },
        "excluded_setup_seconds": {
            "branch_provision": fixture["provision_seconds"],
            "branch_stage_and_noop_main_edit": stage_seconds,
        },
        "measurement": {
            **metric,
            "changes_per_second": total / metric["wall_seconds"],
            "milliseconds_per_change": metric["wall_seconds"] * 1000 / total,
            "statements_per_change": metric["database"]["round_trips_observed"] / total,
        },
        "verification": verification,
    }
    Path(args.output).write_text(json.dumps(result, indent=2, sort_keys=True) + "\n")


def distribution(values):
    mean = statistics.mean(values)
    deviation = statistics.stdev(values) if len(values) > 1 else 0.0
    return {
        "rounds": len(values),
        "mean": mean,
        "sample_stdev": deviation,
        "variance": statistics.variance(values) if len(values) > 1 else 0.0,
        "coefficient_of_variation": deviation / mean if mean else None,
        "min": min(values),
        "max": max(values),
    }


def aggregate(args):
    inputs = [json.loads(Path(path).read_text()) for path in args.inputs]
    grouped = {}
    for result in inputs:
        if not result["verification"]["passed"]:
            raise SystemExit(f"unverified result: {result}")
        grouped.setdefault((result["volume"], result["engine"]), []).append(result)
    volumes = sorted({volume for volume, _engine in grouped})
    summary = {"schema_version": 1, "volumes": {}}
    for volume in volumes:
        engines = {}
        for engine in ("current", "set_based"):
            results = grouped.get((volume, engine), [])
            if len(results) < 3:
                raise SystemExit(
                    f"volume {volume} engine {engine} has only {len(results)} rounds"
                )
            engines[engine] = {
                name: distribution([result["measurement"][name] for result in results])
                for name in (
                    "wall_seconds",
                    "milliseconds_per_change",
                    "statements_per_change",
                    "peak_rss_mib",
                    "incremental_peak_rss_mib",
                )
            }
        summary["volumes"][str(volume)] = {
            "engines": engines,
            "comparison": {
                "wall_clock_speedup": engines["current"]["wall_seconds"]["mean"]
                / engines["set_based"]["wall_seconds"]["mean"],
                "statement_reduction_factor": engines["current"][
                    "statements_per_change"
                ]["mean"]
                / engines["set_based"]["statements_per_change"]["mean"],
                "peak_rss_ratio_set_over_current": engines["set_based"]["peak_rss_mib"][
                    "mean"
                ]
                / engines["current"]["peak_rss_mib"]["mean"],
            },
        }
    Path(args.output).write_text(json.dumps(summary, indent=2, sort_keys=True) + "\n")


def main():
    parser = argparse.ArgumentParser()
    subparsers = parser.add_subparsers(dest="command", required=True)
    measure_parser = subparsers.add_parser("measure")
    measure_parser.add_argument(
        "--engine", choices=("current", "set_based"), required=True
    )
    measure_parser.add_argument("--round", type=int, required=True)
    measure_parser.add_argument("--rows", type=int, required=True)
    measure_parser.add_argument("--namespace-offset", type=int, default=0)
    measure_parser.add_argument("--output", required=True)
    measure_parser.add_argument("--confirm-disposable-database", required=True)
    aggregate_parser = subparsers.add_parser("aggregate")
    aggregate_parser.add_argument("--inputs", nargs="+", required=True)
    aggregate_parser.add_argument("--output", required=True)
    args = parser.parse_args()
    if args.command == "measure":
        measure(args)
    else:
        aggregate(args)


if __name__ == "__main__":
    main()
