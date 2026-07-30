#!/usr/bin/env python
"""Disposable realistic-mix benchmark for the production MAC apply engines.

This script never calls Forward. It is intentionally guarded for the local
Docker database and creates only deterministic synthetic data. Branch
provisioning, fixture seeding, and verification are outside the timed region.
"""
import argparse
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
from netbox_branching.models import Branch  # noqa: E402
from netbox_branching.models import ChangeDiff  # noqa: E402
from netbox_branching.utilities import activate_branch  # noqa: E402

from forward_netbox.models import ForwardSource  # noqa: E402
from forward_netbox.models import ForwardSync  # noqa: E402
from forward_netbox.utilities.apply_engine import select_apply_engine  # noqa: E402
from forward_netbox.utilities.branching import build_branch_request  # noqa: E402
from forward_netbox.utilities.logging import SyncLogging  # noqa: E402
from forward_netbox.utilities.sync import ForwardSyncRunner  # noqa: E402


MIX = {"noop": 0.90, "update": 0.07, "create": 0.02, "delete": 0.01}
OPERATIONS = tuple(MIX)
MODEL_STRING = "dcim.macaddress"
CONFIRMATION = "fnb-copysql-wpa-disposable"


def mac_for(index):
    value = index + 1
    return "02:%02X:%02X:%02X:%02X:%02X" % (
        (value >> 32) & 0xFF,
        (value >> 24) & 0xFF,
        (value >> 16) & 0xFF,
        (value >> 8) & 0xFF,
        value & 0xFF,
    )


def operation_counts(total):
    raw = [total * MIX[name] for name in OPERATIONS]
    counts = [int(value) for value in raw]
    remainder = total - sum(counts)
    order = sorted(
        range(len(raw)),
        key=lambda index: raw[index] - counts[index],
        reverse=True,
    )
    for index in order[:remainder]:
        counts[index] += 1
    return dict(zip(OPERATIONS, counts, strict=True))


class QueryCounter:
    def __init__(self):
        self.django_execute_calls = 0
        self.by_verb = {}
        self.copy_ops = 0

    def __call__(self, execute, sql, params, many, context):
        self.django_execute_calls += 1
        verb = str(sql).lstrip().split(None, 1)[0].upper() if sql else "UNKNOWN"
        self.by_verb[verb] = self.by_verb.get(verb, 0) + 1
        return execute(sql, params, many, context)

    def as_dict(self):
        observed = self.django_execute_calls + self.copy_ops
        return {
            "django_execute_calls": self.django_execute_calls,
            "copy_ops": self.copy_ops,
            "round_trips_observed": observed,
            "by_verb": dict(sorted(self.by_verb.items())),
        }


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
            "Refusing to run: pass the exact disposable confirmation and use "
            "the isolated Docker PostgreSQL host named 'postgres'."
        )


def create_fixture(*, engine_name, round_number, total):
    suffix = f"macbench-r{round_number}-{engine_name}-{time.time_ns() % 1_000_000_000}"
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
    devices = list(
        Device.objects.filter(name__startswith=f"{suffix}-device-").order_by("name")
    )
    interfaces = []
    for device in devices:
        for index in range(4):
            interfaces.append(
                Interface(
                    device=device,
                    name=f"{suffix}-Ethernet{index}",
                    type="1000base-t",
                )
            )
    Interface.objects.bulk_create(interfaces)
    interfaces = list(
        Interface.objects.filter(name__startswith=f"{suffix}-Ethernet")
        .select_related("device")
        .order_by("device__name", "name")
    )

    counts = operation_counts(total)
    numeric_base = round_number * 1_000_000
    addresses = [mac_for(numeric_base + index) for index in range(total)]
    operation_indexes = {}
    offset = 0
    for operation in OPERATIONS:
        operation_indexes[operation] = range(offset, offset + counts[operation])
        offset += counts[operation]
    baseline_indexes = (
        list(operation_indexes["noop"])
        + list(operation_indexes["update"])
        + list(operation_indexes["delete"])
    )
    baseline_count = len(baseline_indexes)
    interface_type = ContentType.objects.get_for_model(Interface)
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

    rows = {name: [] for name in OPERATIONS}
    for operation in OPERATIONS:
        for index in operation_indexes[operation]:
            original_interface = interfaces[index % len(interfaces)]
            target_interface = original_interface
            if operation == "update":
                target_interface = interfaces[(index + 1) % len(interfaces)]
            rows[operation].append(
                {
                    "device": target_interface.device.name,
                    "interface": target_interface.name,
                    "mac": addresses[index],
                }
            )
    source = ForwardSource.objects.create(
        name=f"{suffix}-source",
        type="saas",
        url="https://fixture.invalid",
        status="ready",
        parameters={"network_id": "synthetic"},
    )
    sync = ForwardSync.objects.create(
        name=f"{suffix}-sync",
        source=source,
        user=user,
        parameters={
            "snapshot_id": "latestProcessed",
            "enable_bulk_orm": True,
            "enable_copy_sql": engine_name == "copy_sql",
            "copy_sql_kill_switches": [],
            "dcim.device": True,
            MODEL_STRING: True,
        },
    )
    branch = Branch(name=f"MAC benchmark r{round_number} {engine_name} {suffix[-9:]}")
    branch.save(provision=False)
    started = time.perf_counter()
    branch.provision(user=user)
    provision_seconds = time.perf_counter() - started
    branch.refresh_from_db()
    return {
        "suffix": suffix,
        "user": user,
        "sync": sync,
        "branch": branch,
        "rows": rows,
        "counts": counts,
        "addresses": addresses,
        "baseline_count": baseline_count,
        "provision_seconds": provision_seconds,
    }


def apply_fixture(fixture, engine_name):
    branch = fixture["branch"]
    request = build_branch_request(fixture["user"])
    fixture["request_id"] = request.id
    runner = ForwardSyncRunner(
        sync=fixture["sync"], ingestion=None, client=None, logger_=SyncLogging()
    )
    upserts = (
        fixture["rows"]["noop"] + fixture["rows"]["update"] + fixture["rows"]["create"]
    )
    deletes = fixture["rows"]["delete"]
    counter = QueryCounter()
    aliases = ("default", branch.connection_name)
    connections[branch.connection_name].ensure_connection()
    before_cpu = cpu_snapshot()
    with ExitStack() as stack:
        for alias in aliases:
            stack.enter_context(connections[alias].execute_wrapper(counter))
        if engine_name == "copy_sql":
            counter.copy_ops = 1
            stack.enter_context(
                patch(
                    "forward_netbox.utilities.apply_engine._bulk_orm_apply_simple_models",
                    side_effect=AssertionError(
                        "COPY/SQL benchmark unexpectedly fell back"
                    ),
                )
            )
            stack.enter_context(
                patch.object(
                    runner,
                    "_delete_model_rows",
                    side_effect=AssertionError(
                        "COPY/SQL delete unexpectedly fell back"
                    ),
                )
            )
        with RSSMonitor() as rss:
            started = time.perf_counter()
            with activate_branch(branch), event_tracking(request):
                token = current_request.set(request)
                try:
                    engine = select_apply_engine(
                        sync=fixture["sync"], model_string=MODEL_STRING
                    )
                    if engine.name != engine_name:
                        raise AssertionError(
                            f"selected {engine.name!r}, expected {engine_name!r}"
                        )
                    engine.apply_plan_item(runner, MODEL_STRING, upserts, deletes)
                finally:
                    current_request.reset(token)
            wall_seconds = time.perf_counter() - started
    after_cpu = cpu_snapshot()
    return {
        "engine": engine.name,
        "wall_seconds": wall_seconds,
        "python_user_seconds": after_cpu[0] - before_cpu[0],
        "python_system_seconds": after_cpu[1] - before_cpu[1],
        "python_total_seconds": sum(after_cpu) - sum(before_cpu),
        "peak_rss_mib": rss.peak / 1024 / 1024,
        "baseline_rss_mib": rss.baseline / 1024 / 1024,
        "incremental_peak_rss_mib": (rss.peak - rss.baseline) / 1024 / 1024,
        "database": counter.as_dict(),
    }


def verify_fixture(fixture):
    branch = fixture["branch"]
    using = branch.connection_name
    addresses = fixture["addresses"]
    expected_targets = len(addresses) - fixture["counts"]["delete"]
    target_count = (
        MACAddress.objects.using(using).filter(mac_address__in=addresses).count()
    )
    content_type = ContentType.objects.get_for_model(MACAddress)
    changes = ObjectChange.objects.using(using).filter(
        request_id=fixture["request_id"],
        changed_object_type=content_type,
    )
    action_counts = {
        action: changes.filter(action=action).count()
        for action in ("create", "update", "delete")
    }
    expected_actions = {
        "create": fixture["counts"]["create"],
        "update": fixture["counts"]["update"],
        "delete": fixture["counts"]["delete"],
    }
    diff_count = (
        ChangeDiff.objects.using(using)
        .filter(branch=branch, object_type=content_type)
        .count()
    )
    expected_diffs = sum(expected_actions.values())
    verification = {
        "target_count": target_count,
        "expected_target_count": expected_targets,
        "object_change_actions": action_counts,
        "expected_object_change_actions": expected_actions,
        "change_diff_count": diff_count,
        "expected_change_diff_count": expected_diffs,
    }
    verification["passed"] = (
        target_count == expected_targets
        and action_counts == expected_actions
        and diff_count == expected_diffs
    )
    if not verification["passed"]:
        raise AssertionError(verification)
    return verification


def cleanup_main_addresses(addresses):
    with connections["default"].cursor() as cursor:
        cursor.execute(
            "DELETE FROM dcim_macaddress WHERE mac_address::text = ANY(%s)",
            [[address.lower() for address in addresses]],
        )


def measure(args):
    ensure_disposable_database(args.confirm_disposable_database)
    logging.disable(logging.WARNING)
    fixture = create_fixture(
        engine_name=args.engine,
        round_number=args.round,
        total=args.rows,
    )
    metric = apply_fixture(fixture, args.engine)
    verification = verify_fixture(fixture)
    result = {
        "schema_version": 1,
        "engine": args.engine,
        "round": args.round,
        "runtime": {
            "netbox": settings.RELEASE.version,
            "branching": importlib.metadata.version("netboxlabs-netbox-branching"),
            "python": sys.version.split()[0],
        },
        "fixture": {
            "basis": "realistic-mix campaign steady cell; deterministic synthetic MACs",
            "operation_ratios": MIX,
            "operation_counts": fixture["counts"],
            "rows": args.rows,
        },
        "branch_provision_seconds_excluded": fixture["provision_seconds"],
        "measurement": {
            **metric,
            "rows_per_second": args.rows / metric["wall_seconds"],
            "statements_per_row": metric["database"]["round_trips_observed"]
            / args.rows,
        },
        "verification": verification,
    }
    Path(args.output).write_text(json.dumps(result, indent=2, sort_keys=True) + "\n")
    cleanup_main_addresses(fixture["addresses"])


def distribution(values):
    mean = statistics.mean(values)
    deviation = statistics.stdev(values) if len(values) > 1 else 0.0
    return {
        "rounds": len(values),
        "mean": mean,
        "sample_stdev": deviation,
        "coefficient_of_variation": deviation / mean if mean else None,
        "min": min(values),
        "max": max(values),
    }


def aggregate(args):
    inputs = [json.loads(Path(path).read_text()) for path in args.inputs]
    grouped = {"bulk_orm": [], "copy_sql": []}
    for result in inputs:
        if not result["verification"]["passed"]:
            raise SystemExit(f"unverified input: {result}")
        grouped[result["engine"]].append(result)
    if any(len(results) < 3 for results in grouped.values()):
        raise SystemExit(
            "aggregation requires at least three verified rounds per engine"
        )
    summary = {"schema_version": 1, "engines": {}}
    for engine, results in grouped.items():
        summary["engines"][engine] = {
            "wall_seconds": distribution(
                [result["measurement"]["wall_seconds"] for result in results]
            ),
            "statements_per_row": distribution(
                [result["measurement"]["statements_per_row"] for result in results]
            ),
            "peak_rss_mib": distribution(
                [result["measurement"]["peak_rss_mib"] for result in results]
            ),
            "incremental_peak_rss_mib": distribution(
                [
                    result["measurement"]["incremental_peak_rss_mib"]
                    for result in results
                ]
            ),
        }
    current = summary["engines"]["bulk_orm"]
    copy_sql = summary["engines"]["copy_sql"]
    summary["comparison"] = {
        "wall_clock_speedup": current["wall_seconds"]["mean"]
        / copy_sql["wall_seconds"]["mean"],
        "statement_reduction_factor": current["statements_per_row"]["mean"]
        / copy_sql["statements_per_row"]["mean"],
        "peak_rss_ratio_copy_over_current": copy_sql["peak_rss_mib"]["mean"]
        / current["peak_rss_mib"]["mean"],
    }
    Path(args.output).write_text(json.dumps(summary, indent=2, sort_keys=True) + "\n")


def main():
    parser = argparse.ArgumentParser()
    subparsers = parser.add_subparsers(dest="command", required=True)
    measure_parser = subparsers.add_parser("measure")
    measure_parser.add_argument(
        "--engine", choices=("bulk_orm", "copy_sql"), required=True
    )
    measure_parser.add_argument("--round", type=int, required=True)
    measure_parser.add_argument("--rows", type=int, default=3000)
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
