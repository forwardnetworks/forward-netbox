# Collection health of the devices a sync scopes, per snapshot.
#
# A Forward snapshot can process green while some of the devices it covers failed
# collection (authentication failed, connection timed out, parser exception). Every
# bundled query filters on a completed collection, so those devices vanish from the
# rows and the owned objects (modules, FHRP groups, routing) read as shrinkage. The
# whole-snapshot collapse guard only sees a cliff; a fifth of the scope failing on
# one day is below it. This module counts the sync's own scope by outcome so a
# blocked run can say so.
#
# Counts and failure slugs only. No device names.
import logging
import re
import time

from rq.timeouts import JobTimeoutException

from .forward_api import build_device_tag_scope_where
from .forward_api import run_nqe_query

logger = logging.getLogger("forward_netbox.snapshot_collection_health")

SNAPSHOT_COLLECTION_REASON_PREFIX = "Snapshot collection problem:"

# Devices that completed must have fallen, and devices that did not complete must
# have risen, by at least this many before the finding is raised, so one flaky
# device is not called a snapshot problem and a retired site is not mistaken for one.
MIN_COMPLETED_DROP = 5

# A slow Forward must not turn a blocked run into a failed one.
PROBE_TIMEOUT_SECONDS = 60

_COMPLETED = "completed"

# `DeviceSnapshotResult.collectionFailed(DeviceCollectionError.X)` and
# `DeviceSnapshotResult.processingFailed(DeviceProcessingError.X)` -> X.
_FAILURE_TOKEN_RE = re.compile(r"Device(?:Collection|Processing)Error\.([A-Za-z0-9_]+)")


def scope_query(include_tags, exclude_tags, include_match):
    """The NQE that returns one row per in-scope device with its collection result.

    Same boolean-plus-`toString` shape the scope reconciliation reads, so the
    failure token parses the way it does there."""
    return "\n".join(
        [
            "foreach device in network.devices",
            "where device.platform.vendor != Vendor.FORWARD_CUSTOM",
            *build_device_tag_scope_where(include_tags, exclude_tags, include_match),
            "select {",
            "  completed: device.snapshotInfo.result "
            "== DeviceSnapshotResult.completed,",
            "  reason: toString(device.snapshotInfo.result)",
            "}",
        ]
    )


def _result_slug(row):
    """`completed`, or the Forward failure token, e.g. AUTHENTICATION_FAILED."""
    row = row or {}
    if row.get("completed") is True:
        return _COMPLETED
    match = _FAILURE_TOKEN_RE.search(str(row.get("reason") or ""))
    return match.group(1) if match else "UNKNOWN"


def summarize_results(rows):
    """Count rows of `{result}` into total, completed and failures by slug."""
    by_failure = {}
    completed = 0
    total = 0
    for row in rows or []:
        total += 1
        slug = _result_slug(row)
        if slug == _COMPLETED:
            completed += 1
        else:
            by_failure[slug] = by_failure.get(slug, 0) + 1
    return {
        "total": total,
        "completed": completed,
        "not_completed": total - completed,
        "by_failure": dict(sorted(by_failure.items(), key=lambda item: -item[1])),
    }


def scope_collection_counts(
    client, *, network_id, snapshot_id, include_tags, exclude_tags, include_match
):
    """Collection outcome counts for the sync's scope in one snapshot.

    Returns the summary, or `{"error": "<class>"}` when the probe could not run.
    A failed probe is never zero: the caller must treat `error` as unknown."""
    if not network_id or not snapshot_id:
        return {"error": "missing-network-or-snapshot"}
    try:
        rows = run_nqe_query(
            client,
            query=scope_query(include_tags, exclude_tags, include_match),
            network_id=network_id,
            snapshot_id=snapshot_id,
            fetch_all=True,
            deadline=time.monotonic() + PROBE_TIMEOUT_SECONDS,
        )
    except JobTimeoutException:
        raise
    except Exception as exc:  # noqa: BLE001 - best effort; never fails validation
        logger.warning("scope collection probe failed: %s", type(exc).__name__)
        return {"error": type(exc).__name__}
    return summarize_results(rows)


def scope_collection_finding(
    current, baseline, *, min_completed_drop=MIN_COMPLETED_DROP
):
    """A finding when fewer in-scope devices completed than in the baseline, or None.

    Silence means nothing to say: either side unknown (probe error), no fall, or
    a fall too small to call a snapshot problem."""
    if not current or not baseline or "error" in current or "error" in baseline:
        return None
    dropped = baseline["completed"] - current["completed"]
    if dropped < min_completed_drop:
        return None
    newly_failed = current["not_completed"] - baseline["not_completed"]
    if newly_failed < min_completed_drop:
        return None
    return {
        "dropped_devices": dropped,
        "newly_failed_devices": newly_failed,
        "current_completed": current["completed"],
        "current_total": current["total"],
        "baseline_completed": baseline["completed"],
        "baseline_total": baseline["total"],
        "current_failures": dict(current["by_failure"]),
        "baseline_failures": dict(baseline["by_failure"]),
    }


def _failure_phrase(failures, limit=3):
    parts = [f"{count} {slug}" for slug, count in list(failures.items())[:limit]]
    rest = len(failures) - limit
    if rest > 0:
        parts.append(f"{rest} other type(s)")
    return ", ".join(parts)


def scope_collection_reason(finding):
    """The operator-facing sentence for a snapshot collection problem."""
    return (
        f"{SNAPSHOT_COLLECTION_REASON_PREFIX} this snapshot completed collection "
        f"for {finding['current_completed']} of the {finding['current_total']} "
        "devices in this sync's scope, against "
        f"{finding['baseline_completed']} of {finding['baseline_total']} in the "
        f"last baseline snapshot ({finding['newly_failed_devices']} more not "
        "collected). Not "
        f"collected now: {_failure_phrase(finding['current_failures'])}. Devices "
        "Forward did not collect return no rows, so the modules, FHRP groups, "
        "routing and other objects they own read as shrinkage and the row-count "
        "drops below are most likely a consequence of this, not of the queries."
    )
