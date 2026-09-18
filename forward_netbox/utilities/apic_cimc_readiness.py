# APIC CIMC inventory readiness: whether the synced snapshot's APIC devices
# carry the controller-detail output and the `moquery -c eqptCh -a all` custom
# command that the `Forward ACI APIC CIMC Inventory` map parses. Read-only;
# one bounded NQE execution through the sync's client.
EQPTCH_COMMAND_TEXT = "moquery -c eqptCh -a all"

READINESS_QUERY = """
foreach device in network.devices
where matches(toLowerCase(replace(toString(device.platform.os), "OS.", "")), "*apic*")
let has_controller_detail = isPresent(
  min(
    foreach c in device.outputs.commands
    where c.commandType == CommandType.CISCO_APIC_CONTROLLER_DETAIL
    select c.commandType
  )
)
let has_eqptch = isPresent(
  min(
    foreach c in device.outputs.commands
    where c.commandType == CommandType.CUSTOM
      && c.commandText == "moquery -c eqptCh -a all"
    select c.commandText
  )
)
let completed = device.snapshotInfo.result == DeviceSnapshotResult.completed
select {
  has_controller_detail: has_controller_detail,
  has_eqptch: has_eqptch,
  completed: completed
}
"""


def audit_apic_cimc_readiness(sync, client=None):
    from ..exceptions import ForwardSyncError
    from .forward_api import run_nqe_query

    network_id = sync.get_network_id()
    if not network_id:
        raise ForwardSyncError("Sync source has no network configured.")
    client = client or sync.source.get_client()
    snapshot_id = sync.resolve_snapshot_id(client)
    rows = run_nqe_query(
        client,
        query=READINESS_QUERY,
        network_id=network_id,
        snapshot_id=snapshot_id,
        fetch_all=True,
    )
    apic_count = len(rows)
    with_controller_detail = sum(1 for r in rows if r.get("has_controller_detail"))
    with_eqptch = sum(1 for r in rows if r.get("has_eqptch"))
    completed_with_eqptch = sum(
        1 for r in rows if r.get("has_eqptch") and r.get("completed")
    )
    ready = completed_with_eqptch > 0
    return {
        "sync_id": sync.pk,
        "sync_name": sync.name,
        "snapshot_selector": sync.get_snapshot_id(),
        "apic_device_count": apic_count,
        "with_controller_detail": with_controller_detail,
        "with_eqptch_command": with_eqptch,
        "completed_with_eqptch": completed_with_eqptch,
        "cimc_inventory_ready": ready,
        "remediation": (
            ""
            if ready
            else (
                "No completed APIC device carries "
                f"`{EQPTCH_COMMAND_TEXT}`. Add it as a recurring custom command "
                "on the APICs in Forward so it is collected in a completed "
                "snapshot, then enable the `Forward ACI APIC CIMC Inventory` "
                "map. APIC and ACI device sync are unaffected."
            )
        ),
    }
