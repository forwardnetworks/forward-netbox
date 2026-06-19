# Device-analysis refresh: surface GA Forward per-device operational signals
# (reachability proxy, connectivity-degree "blast radius", CVE exposure) into
# NetBox as read-only ForwardDeviceAnalysis rows for the device-detail panel.
#
# Full path-based reachability/blast radius come from Forward's path/Predict
# APIs (not device NQE); see forward_netbox/queries/forward_device_analysis.nqe.
from pathlib import Path

ANALYSIS_QUERY_NAME = "Forward Device Analysis"
ANALYSIS_QUERY_PATH = (
    Path(__file__).resolve().parents[1] / "queries" / "forward_device_analysis.nqe"
)


def _analysis_query_text():
    return ANALYSIS_QUERY_PATH.read_text(encoding="utf-8")


def fetch_device_analysis_rows(sync):
    """Run the device-analysis NQE for the sync and return (rows, snapshot_id).

    Runs the bundled .nqe text directly — this is a NetBox-side read-only overlay,
    not a Branching sync model, so it does not need a query-registry binding.
    """
    client = sync.source.get_client()
    network_id = sync.get_network_id()
    snapshot_id = sync.resolve_snapshot_id(client)
    rows = client.run_nqe_query(
        query=_analysis_query_text(),
        network_id=network_id,
        snapshot_id=snapshot_id,
        fetch_all=True,
    )
    return rows, snapshot_id


def _coerce_int(value):
    try:
        return max(int(value), 0)
    except (TypeError, ValueError):
        return 0


def refresh_device_analysis(sync) -> dict:
    """Upsert ForwardDeviceAnalysis rows from the device-analysis NQE.

    Only updates devices that exist in NetBox (analysis is a NetBox-side overlay),
    and removes stale rows for devices no longer returned. Returns counts.
    """
    from dcim.models import Device
    from django.db import transaction

    from forward_netbox.models import ForwardDeviceAnalysis

    rows, snapshot_id = fetch_device_analysis_rows(sync)
    device_by_name = {
        device.name: device
        for device in Device.objects.all()
        if (device.name or "").strip()
    }

    seen_device_ids = set()
    upserted = 0
    with transaction.atomic():
        for row in rows:
            name = str(row.get("name") or "").strip()
            device = device_by_name.get(name)
            if device is None:
                continue
            seen_device_ids.add(device.pk)
            ForwardDeviceAnalysis.objects.update_or_create(
                sync=sync,
                device=device,
                defaults={
                    "reachable": bool(row.get("reachable")),
                    "blast_radius": _coerce_int(row.get("blast_radius")),
                    "cve_count": _coerce_int(row.get("cve_count")),
                    "up_interfaces": _coerce_int(row.get("up_interfaces")),
                    "detail": str(row.get("detail") or "")[:255],
                    "snapshot_id": str(snapshot_id or ""),
                },
            )
            upserted += 1
        stale = ForwardDeviceAnalysis.objects.filter(sync=sync).exclude(
            device_id__in=seen_device_ids
        )
        removed = stale.count()
        stale.delete()

    return {"analyzed": upserted, "removed": removed, "rows": len(rows)}
