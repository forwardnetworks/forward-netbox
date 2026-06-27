# Read-only audit of the Mgmt_<iface> primary-IP feature: for each Forward
# Mgmt_-tagged device, classify whether it resolves and, if not, WHY. Reuses the
# resolver's own helpers (get_device_mgmt_tags + _branch_interface_ips +
# resolve_primary_ip_assignments + resolve_mgmt_interface_name) so a verdict
# matches what a real sync would compute. Reads current NetBox state; never writes.
#
# Buckets the unresolved devices so an operator can tell the difference between a
# data/scope problem and an apply/assignment gap:
#   device_not_in_netbox       - tagged in Forward, not present in NetBox
#   interface_not_matched      - device present, but the Mgmt target interface name
#                                is not on the device in NetBox
#   interface_present_no_ip    - target interface present, but no IP is assigned to
#                                it in NetBox (the apply/assignment gap)
from .interface_naming import resolve_mgmt_interface_name
from .primary_ip import _branch_interface_ips
from .primary_ip import resolve_primary_ip_assignments


def audit_primary_ip_resolution(sync, client, *, snapshot_id=None, sample_limit=10):
    from .sync_facade import device_tag_scope

    network_id = sync.get_network_id()
    snapshot_id = snapshot_id or sync.resolve_snapshot_id(client)
    include_tags, exclude_tags, include_match = device_tag_scope(sync)
    device_mgmt_tags = client.get_device_mgmt_tags(
        network_id,
        snapshot_id,
        include_tags=include_tags,
        exclude_tags=exclude_tags,
        include_match=include_match,
    )

    names = list(device_mgmt_tags.keys())
    devices, device_interface_ips, _ = _branch_interface_ips(names)
    assignments = resolve_primary_ip_assignments(device_mgmt_tags, device_interface_ips)

    not_in_netbox = []
    interface_not_matched = []
    interface_present_no_ip = []
    for name in names:
        if name in assignments:
            continue
        if name not in devices:
            not_in_netbox.append(name)
            continue
        interface_names = list((device_interface_ips.get(name) or {}).keys())
        matched = None
        for tag in device_mgmt_tags[name]:
            matched = resolve_mgmt_interface_name(tag, interface_names)
            if matched:
                break
        if not matched:
            interface_not_matched.append((name, device_mgmt_tags[name]))
        else:
            interface_present_no_ip.append((name, matched, device_mgmt_tags[name]))

    return {
        "snapshot_id": str(snapshot_id),
        "mgmt_tagged_devices": len(names),
        "resolvable": len(assignments),
        "unresolved": len(names) - len(assignments),
        "unresolved_device_not_in_netbox": len(not_in_netbox),
        "unresolved_interface_not_matched": len(interface_not_matched),
        "unresolved_interface_present_no_ip": len(interface_present_no_ip),
        "example_device_not_in_netbox": not_in_netbox[:sample_limit],
        "example_interface_not_matched": interface_not_matched[:sample_limit],
        "example_interface_present_no_ip": interface_present_no_ip[:sample_limit],
    }
