from contextlib import contextmanager

from ..choices import ForwardSyncStatusChoices


class StalePostSyncSnapshotError(RuntimeError):
    """Raised before an overlay can mutate state for an obsolete ingestion."""


def latest_completed_ingestion(sync):
    return sync.latest_baseline_ingestion()


def latest_completed_snapshot_id(sync):
    baseline = latest_completed_ingestion(sync)
    return str(getattr(baseline, "snapshot_id", "") or "").strip()


@contextmanager
def current_post_sync_snapshot(sync, snapshot_id, *, ingestion_id=None):
    """Prove an overlay targets the latest merged ingestion generation."""
    snapshot_id = str(snapshot_id or "").strip()

    from .ownership import ownership_write_lock

    with ownership_write_lock():
        sync.source.__class__.objects.select_for_update().get(pk=sync.source_id)
        locked_sync = sync.__class__.objects.select_for_update().get(pk=sync.pk)
        baseline = latest_completed_ingestion(locked_sync)
        current_snapshot_id = str(getattr(baseline, "snapshot_id", "") or "").strip()
        current_ingestion_id = getattr(baseline, "pk", None)
        expected_ingestion_id = int(ingestion_id) if ingestion_id is not None else None
        if (
            locked_sync.status != ForwardSyncStatusChoices.COMPLETED
            or baseline is None
            or (snapshot_id and current_snapshot_id != snapshot_id)
            or (
                expected_ingestion_id is not None
                and current_ingestion_id != expected_ingestion_id
            )
        ):
            raise StalePostSyncSnapshotError(
                "Post-sync overlay generation is no longer the latest completed "
                "ingestion; skipping mutation and scheduling catch-up."
            )
        yield {
            "generation": current_ingestion_id,
            "snapshot_id": current_snapshot_id,
        }
