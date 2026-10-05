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

from .diagnostics import failure_classifier
from .forward_api import get_device_management_ips
from .forward_api import get_device_mgmt_tags
from .interface_naming import parse_mgmt_tag
from .interface_naming import resolve_mgmt_interface_name

PRIMARY_IP_FROM_MGMT_TAG_PARAMETER = "set_primary_ip_from_mgmt_tag"
PRIMARY_IP_FROM_MANAGEMENT_IP_PARAMETER = "set_primary_ip_from_forward_management_ip"


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


def resolve_management_ip_assignments(
    device_management_ips, device_interface_ips, *, skip=(), reasons=None
):
    """Resolve primary v4/v6 from Forward's recorded management address.

    The fallback for a device that carries no ``Mgmt_`` tag. Deliberately
    narrow: Forward must record exactly one management address for the device,
    and that exact address must already sit on exactly one of the device's
    interfaces in NetBox. Several recorded addresses, an address that is on no
    interface, or one on several interfaces is ambiguous and is skipped rather
    than guessed. ``skip`` names devices whose ``Mgmt_`` tag already resolved:
    an explicit tag that resolved always wins. A tag that did NOT resolve leaves
    the device with no primary IP at all, so it is not skipped - the management
    address is strictly better than nothing there.

    ``reasons``, when given, receives ``{device: slug}`` for every device this
    could not resolve: ``multiple-addresses``, ``no-interface`` or
    ``several-interfaces``.

    Returns the same shape as `resolve_primary_ip_assignments`.
    """
    assignments = {}
    skipped = set(skip or ())
    for device_name, raw_ips in (device_management_ips or {}).items():
        if device_name in skipped:
            continue
        hosts = {
            host
            for host in (_host_ip(raw) for raw in raw_ips or ())
            if host is not None
        }
        if len(hosts) != 1:
            if reasons is not None and hosts:
                reasons[device_name] = "multiple-addresses"
            continue
        host = next(iter(hosts))
        matches = [
            (interface_name, address)
            for interface_name, addresses in (
                (device_interface_ips or {}).get(device_name) or {}
            ).items()
            for address in addresses
            if _host_ip(address) == host
        ]
        if len(matches) != 1:
            if reasons is not None:
                reasons[device_name] = (
                    "no-interface" if not matches else "several-interfaces"
                )
            continue
        interface_name, address = matches[0]
        assignments[device_name] = {
            "interface": interface_name,
            "v4": address if host.version == 4 else None,
            "v6": address if host.version == 6 else None,
        }
    return assignments


def primary_ip_from_mgmt_tag_enabled(sync):
    return bool((sync.parameters or {}).get(PRIMARY_IP_FROM_MGMT_TAG_PARAMETER))


def primary_ip_from_management_ip_enabled(sync):
    return bool((sync.parameters or {}).get(PRIMARY_IP_FROM_MANAGEMENT_IP_PARAMETER))


def primary_ip_step_enabled(sync):
    """Either source turns the post-staging primary-IP step on."""
    return primary_ip_from_mgmt_tag_enabled(
        sync
    ) or primary_ip_from_management_ip_enabled(sync)


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
    by_name = {}
    for device in Device.objects.filter(name__in=list(device_names)):
        by_name.setdefault(device.name, []).append(device)

    def interface_ips(device):
        interfaces = {i.pk: i for i in Interface.objects.filter(device=device)}
        per_interface = {i.name: [] for i in interfaces.values()}
        lookup = {}
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
            lookup[(device.name, interface.name, addr)] = ip
        return per_interface, lookup

    devices = {}
    device_interface_ips = {}
    ip_lookup = {}
    for name, group in by_name.items():
        resolved = [(device, *interface_ips(device)) for device in group]
        if len(resolved) > 1:
            # Two devices share this name (a site relabel leaves a pair). The
            # name-keyed map used to keep whichever came last, which could be
            # the copy with no addresses, so a tagged device never got its
            # primary IP. Take the one copy that has interface addresses; with
            # none or several, the name is ambiguous and stays unresolved.
            with_addresses = [row for row in resolved if any(row[1].values())]
            if len(with_addresses) != 1:
                continue
            resolved = with_addresses
        device, per_interface, lookup = resolved[0]
        devices[name] = device
        device_interface_ips[name] = per_interface
        ip_lookup.update(lookup)
    return devices, device_interface_ips, ip_lookup


def _existing_primary_ip_owners():
    """Map ``(attr, ip_pk) -> device_pk`` for primary IPs already claimed.

    Read from the branch rather than inferred from this run: an address may
    already be another device's primary from an earlier sync or an operator
    edit, and that owner need not appear in this run's assignment set.
    """
    from dcim.models import Device

    owners = {}
    rows = Device.objects.exclude(primary_ip4__isnull=True, primary_ip6__isnull=True)
    for pk, ip4, ip6 in rows.values_list("pk", "primary_ip4_id", "primary_ip6_id"):
        if ip4:
            owners[("primary_ip4", ip4)] = pk
        if ip6:
            owners[("primary_ip6", ip6)] = pk
    return owners


def _devices_sharing_management_address(unresolved, device_management_ips):
    """Devices whose one management address is already held by another device.

    A virtual system or an HA peer reports its parent's management address, so
    several devices name the same one. NetBox allows one primary-IP owner per
    address, so only one of them can ever have it - that is not a failure to
    resolve, and it should not read like one.
    """
    from ipam.models import IPAddress

    shared = set()
    for name in unresolved:
        hosts = {
            host
            for host in (_host_ip(raw) for raw in device_management_ips.get(name) or ())
            if host is not None
        }
        if len(hosts) != 1:
            continue
        host = str(next(iter(hosts)))
        for ip in IPAddress.objects.filter(address__net_host=host)[:5]:
            owner = getattr(ip.assigned_object, "device", None)
            if owner is not None and owner.name != name:
                shared.add(name)
                break
    return shared


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
        device_mgmt_tags = {}
        if primary_ip_from_mgmt_tag_enabled(sync):
            device_mgmt_tags = get_device_mgmt_tags(
                executor.client,
                network_id,
                snapshot_id,
                include_tags=include_tags,
                exclude_tags=exclude_tags,
                include_match=include_match,
            )
        device_management_ips = {}
        if primary_ip_from_management_ip_enabled(sync):
            device_management_ips = get_device_management_ips(
                executor.client,
                network_id,
                snapshot_id,
                include_tags=include_tags,
                exclude_tags=exclude_tags,
                include_match=include_match,
            )
        if not device_mgmt_tags and not device_management_ips:
            logger.log_info(
                "primary_ip-from-tag: no Mgmt_ device tags or management "
                "addresses found."
            )
            return 0
    except JobTimeoutException:
        raise
    except Exception as error:  # never break the ingest on the tag fetch
        logger.log_warning(
            "primary_ip-from-tag: tag fetch failed " f"({failure_classifier(error)})."
        )
        return 0

    current_branch = active_branch.get()
    request_token = None
    if current_request.get() is None:
        request_token = current_request.set(build_branch_request(executor.user))
    try:
        active_branch.set(branch)
        try:
            devices, device_interface_ips, ip_lookup = _branch_interface_ips(
                set(device_mgmt_tags) | set(device_management_ips)
            )
            tag_assignments = resolve_primary_ip_assignments(
                device_mgmt_tags, device_interface_ips
            )
            # A tag that resolved is the tag's to decide. A tag that did not
            # resolve leaves the device with no primary IP, so the
            # management-address fallback covers it too: 590 tagged devices on
            # one estate were left bare because an unresolved tag blocked it.
            fallback_reasons = {}
            fallback_assignments = resolve_management_ip_assignments(
                device_management_ips,
                device_interface_ips,
                skip=set(tag_assignments),
                reasons=fallback_reasons,
            )
            assignments = {**fallback_assignments, **tag_assignments}
            updated = []
            unresolved = 0
            # NetBox enforces UNIQUE(primary_ip4_id) and UNIQUE(primary_ip6_id):
            # an address is the primary of exactly one device. Two devices
            # resolving the same Mgmt_ address — a shared management address, or
            # one address discovered on interfaces of two devices — both reached
            # `device.save()` and failed the whole ingestion with
            # `dcim_device_primary_ip4_id_key`, after every workload had already
            # been staged. Claim each address once and report the loser.
            claimed_by = _existing_primary_ip_owners()
            conflicts = {}
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
                    if ip is None or getattr(device, f"{attr}_id") == ip.pk:
                        continue
                    holder = claimed_by.get((attr, ip.pk))
                    if holder is not None and holder != device.pk:
                        conflicts[attr] = conflicts.get(attr, 0) + 1
                        continue
                    setattr(device, attr, ip)
                    claimed_by[(attr, ip.pk)] = device.pk
                    changed = True
                if changed:
                    device.save(update_fields=["primary_ip4", "primary_ip6"])
                    updated.append(device)
            if conflicts:
                # Counts and field names only — device names are customer data.
                detail = ", ".join(
                    f"{count} on {attr}" for attr, count in sorted(conflicts.items())
                )
                logger.log_warning(
                    f"primary_ip-from-tag: {sum(conflicts.values())} device(s) "
                    "resolved a management address that is already another "
                    f"device's primary IP ({detail}). NetBox allows one owner per "
                    "address, so those assignments were skipped and the rest "
                    "applied.",
                    obj=getattr(executor, "current_ingestion", None) or sync,
                )
            # Devices whose Mgmt_ tag pointed at no resolvable interface/IP.
            unresolved = len(device_mgmt_tags) - len(tag_assignments)
            if updated:
                emit_branch_object_changes([], updated)
            from_fallback = sum(
                1
                for device in updated
                if device.name in fallback_assignments
                and device.name not in tag_assignments
            )
            logger.log_info(
                f"primary_ip-from-tag: set primary IP on {len(updated)} device(s)"
                f" ({from_fallback} from Forward's management address)"
                f"; {unresolved} tag(s) unresolved."
            )
            self_unresolved = {
                name: reason
                for name, reason in fallback_reasons.items()
                if name not in assignments
            }
            if self_unresolved:
                shared = _devices_sharing_management_address(
                    self_unresolved, device_management_ips
                )
                counts = {}
                for name, reason in self_unresolved.items():
                    key = "shared" if name in shared else reason
                    counts[key] = counts.get(key, 0) + 1
                logger.log_info(
                    "primary_ip-from-tag: "
                    f"{len(self_unresolved)} device(s) left without a primary IP "
                    "from the management address: "
                    f"{counts.get('shared', 0)} share it with another device "
                    "(NetBox allows one primary-IP owner per address), "
                    f"{counts.get('no-interface', 0)} have it on no synced "
                    f"interface, {counts.get('multiple-addresses', 0)} have "
                    "several management addresses, "
                    f"{counts.get('several-interfaces', 0)} have it on several "
                    "interfaces."
                )
            return len(updated)
        finally:
            active_branch.set(None)
    finally:
        active_branch.set(current_branch)
        if request_token is not None:
            current_request.reset(request_token)
