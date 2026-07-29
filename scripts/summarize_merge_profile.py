#!/usr/bin/env python3
"""Summarize fsync'd merge-profile rounds and cgroup samples."""

import argparse
import csv
import json
import math
import statistics
from collections import defaultdict
from pathlib import Path


def _median(values):
    return statistics.median(values) if values else 0.0


def _variance(values):
    return statistics.variance(values) if len(values) > 1 else 0.0


def _metric(values):
    return {
        "median": _median(values),
        "variance": _variance(values),
        "minimum": min(values) if values else 0.0,
        "maximum": max(values) if values else 0.0,
        "samples": len(values),
    }


def _resource_samples(path):
    result = defaultdict(list)
    with path.open(encoding="utf-8") as handle:
        for row in csv.DictReader(handle, delimiter="\t"):
            result[row["role"]].append(
                {
                    "epoch": float(row["epoch_seconds"]),
                    "cpu": int(row["cpu_usage_usec"]),
                    "memory": int(row["memory_bytes"]),
                }
            )
    return result


def _round_resource(samples, started, finished):
    if not samples:
        return {"cpu_seconds": 0.0, "peak_memory_bytes": 0, "sample_count": 0}
    in_range = [
        sample
        for sample in samples
        if started - 0.25 <= sample["epoch"] <= finished + 0.25
    ]
    if len(in_range) < 2:
        nearest_start = min(samples, key=lambda item: abs(item["epoch"] - started))
        nearest_end = min(samples, key=lambda item: abs(item["epoch"] - finished))
        in_range = sorted((nearest_start, nearest_end), key=lambda item: item["epoch"])
    return {
        "cpu_seconds": max(0, in_range[-1]["cpu"] - in_range[0]["cpu"]) / 1_000_000,
        "peak_memory_bytes": max(item["memory"] for item in in_range),
        "sample_count": len(in_range),
    }


def _phase_key(bucket):
    return (bucket["owner"], bucket["phase"])


def _container_cpu(item, role):
    return item["container_resources"].get(role, {}).get("cpu_seconds", 0.0)


def summarize(rounds, resource_samples):
    for item in rounds:
        item["container_resources"] = {
            role: _round_resource(
                samples,
                item["started_epoch"],
                item["finished_epoch"],
            )
            for role, samples in resource_samples.items()
        }

    by_volume = defaultdict(list)
    for item in rounds:
        by_volume[int(item["volume"])].append(item)

    summary = {"round_count": len(rounds), "volumes": {}, "scaling": {}}
    for volume, items in sorted(by_volume.items()):
        phase_values = defaultdict(lambda: defaultdict(list))
        owner_values = defaultdict(lambda: defaultdict(list))
        model_values = defaultdict(lambda: defaultdict(list))
        cpu_split_values = defaultdict(list)
        for item in items:
            per_phase = defaultdict(
                lambda: {"wall": 0.0, "statements": 0, "db_wall": 0.0}
            )
            per_owner = defaultdict(lambda: {"wall": 0.0, "statements": 0})
            per_model = defaultdict(lambda: {"wall": 0.0, "statements": 0})
            for bucket in item["buckets"]:
                phase = per_phase[_phase_key(bucket)]
                phase["wall"] += bucket["wall_seconds"]
                phase["statements"] += bucket["statements"]
                phase["db_wall"] += bucket["db_wall_seconds"]
                per_owner[bucket["owner"]]["wall"] += bucket["wall_seconds"]
                per_owner[bucket["owner"]]["statements"] += bucket["statements"]
                if bucket["model"]:
                    per_model[bucket["model"]]["wall"] += bucket["wall_seconds"]
                    per_model[bucket["model"]]["statements"] += bucket["statements"]
            for phase_key, values in per_phase.items():
                phase = phase_values[phase_key]
                phase["wall_seconds_per_change"].append(values["wall"] / volume)
                phase["statements_per_change"].append(values["statements"] / volume)
                phase["db_wall_seconds_per_change"].append(values["db_wall"] / volume)
            for owner, values in per_owner.items():
                owner_values[owner]["wall_fraction"].append(
                    values["wall"] / item["wall_seconds"]
                )
                owner_values[owner]["statements_per_change"].append(
                    values["statements"] / volume
                )
            for model, values in per_model.items():
                row_count = max(1, int(item["model_counts"].get(model, 0)))
                model_values[model]["wall_seconds_per_model_change"].append(
                    values["wall"] / row_count
                )
                model_values[model]["statements_per_model_change"].append(
                    values["statements"] / row_count
                )
            python_cpu = _container_cpu(item, "python")
            postgres_cpu = _container_cpu(item, "postgres")
            measured_cpu = python_cpu + postgres_cpu
            if measured_cpu:
                cpu_split_values["python_fraction"].append(python_cpu / measured_cpu)
                cpu_split_values["postgres_fraction"].append(
                    postgres_cpu / measured_cpu
                )

        summary["volumes"][str(volume)] = {
            "samples": len(items),
            "wall_seconds": _metric([item["wall_seconds"] for item in items]),
            "changes_per_second": _metric(
                [item["changes_per_second"] for item in items]
            ),
            "statements_per_change": _metric(
                [item["statements_per_change"] for item in items]
            ),
            "db_round_trips_per_change": _metric(
                [
                    item.get(
                        "db_round_trips_per_change",
                        item["statements_per_change"],
                    )
                    for item in items
                ]
            ),
            "db_execute_wall_fraction": _metric(
                [item["db_execute_wall_fraction"] for item in items]
            ),
            "python_cpu_seconds": _metric(
                [item["python_cpu_seconds"] for item in items]
            ),
            "python_cpu_utilization": _metric(
                [item["python_cpu_utilization"] for item in items]
            ),
            "python_peak_rss_mib": _metric([item["peak_rss_mib"] for item in items]),
            "python_cgroup_cpu_seconds": _metric(
                [_container_cpu(item, "python") for item in items]
            ),
            "postgres_cgroup_cpu_seconds": _metric(
                [_container_cpu(item, "postgres") for item in items]
            ),
            "measured_cpu_split": {
                metric: _metric(values)
                for metric, values in sorted(cpu_split_values.items())
            },
            "postgres_peak_memory_mib": _metric(
                [
                    item["container_resources"]
                    .get("postgres", {})
                    .get("peak_memory_bytes", 0)
                    / 1024
                    / 1024
                    for item in items
                ]
            ),
            "postgres_wal_bytes_per_change": _metric(
                [(item["postgres"].get("wal_bytes") or 0) / volume for item in items]
            ),
            "owners": {
                owner: {metric: _metric(values) for metric, values in metrics.items()}
                for owner, metrics in sorted(owner_values.items())
            },
            "phases": {
                f"{owner}:{phase}": {
                    metric: _metric(values) for metric, values in metrics.items()
                }
                for (owner, phase), metrics in sorted(phase_values.items())
            },
            "models": {
                model: {metric: _metric(values) for metric, values in metrics.items()}
                for model, metrics in sorted(model_values.items())
            },
        }

    volumes = sorted(by_volume)
    if len(volumes) >= 2:
        low, high = volumes[0], volumes[-1]
        low_wall = summary["volumes"][str(low)]["wall_seconds"]["median"]
        high_wall = summary["volumes"][str(high)]["wall_seconds"]["median"]
        summary["scaling"] = {
            "low_volume": low,
            "high_volume": high,
            "wall_ratio": high_wall / low_wall if low_wall else 0.0,
            "volume_ratio": high / low,
            "power_exponent": (
                math.log(high_wall / low_wall) / math.log(high / low)
                if low_wall and high_wall
                else 0.0
            ),
        }
    return summary


def markdown(summary):
    lines = [
        "# Merge Profile Summary",
        "",
        "Variance is sample variance across completed rounds.",
        "",
        "| Volume | n | Median wall (s) | Wall variance | Median changes/s | Rate variance | Median round-trips/change | DB wait fraction | Peak Python RSS (MiB) |",
        "| ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |",
    ]
    for volume, item in summary["volumes"].items():
        lines.append(
            f"| {volume} | {item['samples']} | "
            f"{item['wall_seconds']['median']:.6f} | "
            f"{item['wall_seconds']['variance']:.6f} | "
            f"{item['changes_per_second']['median']:.3f} | "
            f"{item['changes_per_second']['variance']:.6f} | "
            f"{item['db_round_trips_per_change']['median']:.4f} | "
            f"{item['db_execute_wall_fraction']['median']:.4f} | "
            f"{item['python_peak_rss_mib']['median']:.2f} |"
        )
    lines.extend(
        [
            "",
            "## Python versus PostgreSQL",
            "",
            "CPU fractions use cgroup CPU deltas for the profiler and PostgreSQL containers during each merge window.",
            "",
            "| Volume | Python CPU (s) | PostgreSQL CPU (s) | Python fraction | PostgreSQL fraction | PostgreSQL peak memory (MiB) | WAL bytes/change |",
            "| ---: | ---: | ---: | ---: | ---: | ---: | ---: |",
        ]
    )
    for volume, item in summary["volumes"].items():
        split = item["measured_cpu_split"]
        lines.append(
            f"| {volume} | {item['python_cgroup_cpu_seconds']['median']:.6f} | "
            f"{item['postgres_cgroup_cpu_seconds']['median']:.6f} | "
            f"{split.get('python_fraction', {}).get('median', 0.0):.4f} | "
            f"{split.get('postgres_fraction', {}).get('median', 0.0):.4f} | "
            f"{item['postgres_peak_memory_mib']['median']:.2f} | "
            f"{item['postgres_wal_bytes_per_change']['median']:.2f} |"
        )

    lines.extend(
        [
            "",
            "## Owner split",
            "",
            "Wall fractions are exclusive: time in nested scopes is charged only to the innermost active owner.",
            "",
            "| Volume | Owner | Median wall fraction | Fraction variance | Median statements/change |",
            "| ---: | --- | ---: | ---: | ---: |",
        ]
    )
    for volume, item in summary["volumes"].items():
        for owner, metrics in item["owners"].items():
            lines.append(
                f"| {volume} | `{owner}` | "
                f"{metrics['wall_fraction']['median']:.4f} | "
                f"{metrics['wall_fraction']['variance']:.8f} | "
                f"{metrics['statements_per_change']['median']:.4f} |"
            )

    lines.extend(
        [
            "",
            "## Phase decomposition",
            "",
            "| Volume | Owner and phase | Median ms/change | Variance (s/change)^2 | Median statements/change | Median DB ms/change |",
            "| ---: | --- | ---: | ---: | ---: | ---: |",
        ]
    )
    for volume, item in summary["volumes"].items():
        for phase, metrics in item["phases"].items():
            lines.append(
                f"| {volume} | `{phase}` | "
                f"{metrics['wall_seconds_per_change']['median'] * 1000:.6f} | "
                f"{metrics['wall_seconds_per_change']['variance']:.10f} | "
                f"{metrics['statements_per_change']['median']:.4f} | "
                f"{metrics['db_wall_seconds_per_change']['median'] * 1000:.6f} |"
            )

    lines.extend(
        [
            "",
            "## Per-model cost curve",
            "",
            "| Volume | Model | Median ms/model change | Variance (s/change)^2 | Median statements/model change |",
            "| ---: | --- | ---: | ---: | ---: |",
        ]
    )
    for volume, item in summary["volumes"].items():
        for model, metrics in item["models"].items():
            lines.append(
                f"| {volume} | `{model}` | "
                f"{metrics['wall_seconds_per_model_change']['median'] * 1000:.6f} | "
                f"{metrics['wall_seconds_per_model_change']['variance']:.10f} | "
                f"{metrics['statements_per_model_change']['median']:.4f} |"
            )

    lines.extend(
        [
            "",
            "## Scaling",
            "",
            "```json",
            json.dumps(summary["scaling"], indent=2),
            "```",
        ]
    )
    return "\n".join(lines) + "\n"


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--rounds", type=Path, required=True)
    parser.add_argument("--resources", type=Path, required=True)
    parser.add_argument("--json-output", type=Path, required=True)
    parser.add_argument("--markdown-output", type=Path, required=True)
    args = parser.parse_args()
    rounds = [
        json.loads(line)
        for line in args.rounds.read_text(encoding="utf-8").splitlines()
        if line.strip()
    ]
    result = summarize(rounds, _resource_samples(args.resources))
    args.json_output.write_text(
        json.dumps(result, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    args.markdown_output.write_text(markdown(result), encoding="utf-8")


if __name__ == "__main__":
    main()
