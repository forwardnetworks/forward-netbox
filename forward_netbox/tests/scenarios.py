from __future__ import annotations

from forward_netbox.utilities.branch_budget import BranchWorkload


NETWORK_ID = "test-network"
SNAPSHOT_OLD = "snapshot-old"
SNAPSHOT_BEFORE = "snapshot-before"
SNAPSHOT_AFTER = "snapshot-after"


def source_parameters(**overrides):
    parameters = {
        "username": "user@example.com",
        "password": "secret",
        "verify": True,
        "timeout": 1200,
        "network_id": NETWORK_ID,
    }
    parameters.update(overrides)
    return parameters


def snapshot(snapshot_id=SNAPSHOT_AFTER, *, state="PROCESSED"):
    return {
        "id": snapshot_id,
        "state": state,
        "created_at": "",
        "processed_at": "2026-03-31T12:15:00Z",
        "processedAt": "2026-03-31T12:15:00Z",
    }


def site_rows(count):
    return [
        {
            "name": f"site-{index}",
            "slug": f"site-{index}",
        }
        for index in range(1, count + 1)
    ]


def invalid_site_rows():
    return [{"name": "site-without-slug"}]


def interface_rows(*, device_count, interfaces_per_device):
    return [
        {
            "device": f"device-{device_index}",
            "name": f"Ethernet1/{interface_index}",
        }
        for device_index in range(1, device_count + 1)
        for interface_index in range(1, interfaces_per_device + 1)
    ]


def diff_rows():
    return [
        {
            "type": "ADDED",
            "before": None,
            "after": {"name": "site-added", "slug": "site-added"},
        },
        {
            "type": "DELETED",
            "before": {"name": "site-deleted", "slug": "site-deleted"},
            "after": None,
        },
        {
            "type": "MODIFIED",
            "before": {"name": "site-old", "slug": "site-modified"},
            "after": {"name": "site-new", "slug": "site-modified"},
        },
    ]


def branch_workload(model_string, rows, *, coalesce_fields):
    return BranchWorkload(
        model_string=model_string,
        label=f"{model_string} synthetic workload",
        upsert_rows=list(rows),
        coalesce_fields=coalesce_fields,
    )
