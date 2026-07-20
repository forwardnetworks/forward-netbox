# Pure resolution logic for the Mgmt_<iface> primary-IP feature.
#
# Given each device's Mgmt_<iface> tag(s) (from Forward) and the IPs synced onto
# each interface (from the branch), decide which address becomes the device's
# primary_ip4 / primary_ip6. Kept ORM-free so it is exhaustively unit-testable;
# the executor wires the inputs from Forward NQE + the branch ORM and applies the
# result.
from ipaddress import ip_address
from ipaddress import ip_interface

from rq.timeouts import JobTimeoutException

from .interface_naming import parse_mgmt_tag
from .interface_naming import resolve_mgmt_interface_name

PRIMARY_IP_FROM_MGMT_TAG_PARAMETER = "set_primary_ip_from_mgmt_tag"


def _host_ip(value):
    """Return the bare host IP (no mask) for an address string, or None."""
    if not value:
        return None
    text = str(value).strip()
    if not text:
        return None
    try:
        return ip_interface(text).ip
    except ValueError:
        try:
            return ip_address(text)
        except ValueError:
            return None


def _pick_lowest(ips, version):
    candidates = []
    for raw in ips:
        host = _host_ip(raw)
        if host is not None and host.version == version:
            candidates.append((host, raw))
    if not candidates:
        return None
    # Deterministic: lowest numeric address wins when an interface has several.
    candidates.sort(key=lambda item: item[0])
    return candidates[0][1]


def resolve_primary_ip_assignments(device_mgmt_tags, device_interface_ips):
    """Resolve per-device primary v4/v6 from Mgmt_ tags + interface IPs.

    Args:
        device_mgmt_tags: {device_name: [tag, ...]} — every tag on the device;
            non-``Mgmt_`` tags are ignored.
        device_interface_ips: {device_name: {interface_name: [ip_str, ...]}}.

    Returns:
        {device_name: {"interface": name, "v4": ip_str|None, "v6": ip_str|None}}
        with one entry per device that has a resolvable Mgmt_ tag pointing at a
        known interface. Devices with no Mgmt_ tag, an unmatched interface, or no
        IPs on the matched interface are omitted (callers log skips).
    """
    assignments = {}
    for device_name, tags in (device_mgmt_tags or {}).items():
        mgmt_tags = [tag for tag in (tags or []) if parse_mgmt_tag(tag)]
        if not mgmt_tags:
            continue
        interface_ips = (device_interface_ips or {}).get(device_name) or {}
        interface_names = list(interface_ips.keys())
        # First Mgmt_ tag that resolves to a real interface with IPs wins.
        for tag in mgmt_tags:
            matched_name = resolve_mgmt_interface_name(tag, interface_names)
            if matched_name is None:
                continue
            ips = interface_ips.get(matched_name) or []
            v4 = _pick_lowest(ips, 4)
            v6 = _pick_lowest(ips, 6)
            if v4 is None and v6 is None:
                continue
            assignments[device_name] = {
                "interface": matched_name,
                "v4": v4,
                "v6": v6,
            }
            break
    return assignments


def primary_ip_from_mgmt_tag_enabled(sync):
    return bool((sync.parameters or {}).get(PRIMARY_IP_FROM_MGMT_TAG_PARAMETER))


def _branch_interface_ips(device_names):
    """Return ({device_name: {iface_name: [addr_str]}}, ip_lookup) from the branch.

    ``ip_lookup`` maps ``(device_name, iface_name, addr_str)`` -> IPAddress so the
    caller can resolve the chosen address back to the concrete object.
    """
    from core.models import ObjectType
    from dcim.models import Device
    from dcim.models import Interface
    from ipam.models import IPAddress

    interface_ct = ObjectType.objects.get_for_model(Interface)
    devices = {d.name: d for d in Device.objects.filter(name__in=list(device_names))}
    device_interface_ips = {}
    ip_lookup = {}
    for name, device in devices.items():
        interfaces = {i.pk: i for i in Interface.objects.filter(device=device)}
        per_interface = {i.name: [] for i in interfaces.values()}
        ips = IPAddress.objects.filter(
            assigned_object_type=interface_ct,
            assigned_object_id__in=list(interfaces.keys()),
        )
        for ip in ips:
            interface = interfaces.get(ip.assigned_object_id)
            if interface is None:
                continue
            addr = str(ip.address)
            per_interface.setdefault(interface.name, []).append(addr)
            ip_lookup[(name, interface.name, addr)] = ip
        device_interface_ips[name] = per_interface
    return devices, device_interface_ips, ip_lookup


def apply_primary_ip_from_mgmt_tags(executor, branch, *, snapshot_id):
    """Set device primary_ip4/6 from Forward ``Mgmt_<iface>`` tags, in the branch.

    Runs after all workloads are staged (so interfaces + IPs exist in the branch)
    and before merge, inside ``active_branch`` so the device updates merge with the
    rest of the sync. Defensive: any failure is logged and swallowed so it never
    breaks the ingest. Returns the number of devices updated.
    """
    from netbox.context import current_request
    from netbox_branching.contextvars import active_branch

    from .apply_engine_bulk import emit_branch_object_changes
    from .branching import build_branch_request
    from .sync_facade import device_tag_scope

    sync = executor.sync
    logger = executor.logger
    try:
        network_id = sync.get_network_id()
        if not network_id:
            logger.log_info("primary_ip-from-tag: no network on the source; skipping.")
            return 0
        include_tags, exclude_tags, include_match = device_tag_scope(sync)
        device_mgmt_tags = executor.client.get_device_mgmt_tags(
            network_id,
            snapshot_id,
            include_tags=include_tags,
            exclude_tags=exclude_tags,
            include_match=include_match,
        )
        if not device_mgmt_tags:
            logger.log_info("primary_ip-from-tag: no Mgmt_ device tags found.")
            return 0
    except JobTimeoutException:
        raise
    except Exception as error:  # never break the ingest on the tag fetch
        logger.log_warning(f"primary_ip-from-tag: tag fetch failed: {error}")
        return 0

    current_branch = active_branch.get()
    request_token = None
    if current_request.get() is None:
        request_token = current_request.set(build_branch_request(executor.user))
    try:
        active_branch.set(branch)
        try:
            devices, device_interface_ips, ip_lookup = _branch_interface_ips(
                device_mgmt_tags.keys()
            )
            assignments = resolve_primary_ip_assignments(
                device_mgmt_tags, device_interface_ips
            )
            updated = []
            unresolved = 0
            for name, assignment in assignments.items():
                device = devices.get(name)
                if device is None:
                    continue
                interface = assignment["interface"]
                device.snapshot()
                changed = False
                for version, attr in (("v4", "primary_ip4"), ("v6", "primary_ip6")):
                    addr = assignment[version]
                    if not addr:
                        continue
                    ip = ip_lookup.get((name, interface, addr))
                    if ip is not None and getattr(device, f"{attr}_id") != ip.pk:
                        setattr(device, attr, ip)
                        changed = True
                if changed:
                    device.save(update_fields=["primary_ip4", "primary_ip6"])
                    updated.append(device)
            # Devices whose Mgmt_ tag pointed at no resolvable interface/IP.
            unresolved = len(device_mgmt_tags) - len(assignments)
            if updated:
                emit_branch_object_changes([], updated)
            logger.log_info(
                f"primary_ip-from-tag: set primary IP on {len(updated)} device(s)"
                f"; {unresolved} tag(s) unresolved."
            )
            return len(updated)
        finally:
            active_branch.set(None)
    finally:
        active_branch.set(current_branch)
        if request_token is not None:
            current_request.reset(request_token)
