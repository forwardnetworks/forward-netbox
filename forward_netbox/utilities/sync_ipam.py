from ipaddress import ip_interface

from django.core.exceptions import ObjectDoesNotExist

from ..exceptions import ForwardDependencySkipError
from ..exceptions import ForwardSearchError


def delete_ipam_vlan(runner, row):
    from ipam.models import VLAN

    site = None
    if row.get("site_slug"):
        from dcim.models import Site

        site = runner._get_unique_or_raise(Site, {"slug": row["site_slug"]})
    if site is None and row.get("site"):
        from dcim.models import Site

        site = runner._get_unique_or_raise(Site, {"name": row["site"]})
    if site is None or row.get("vid") in (None, ""):
        return False
    return runner._delete_by_coalesce(
        VLAN,
        [{"site": site, "vid": int(row["vid"])}],
    )


def delete_ipam_vrf(runner, row):
    from ipam.models import VRF

    lookups = []
    if row.get("rd"):
        lookups.append({"rd": row["rd"]})
    if row.get("name"):
        lookups.append({"name": row["name"]})
    return runner._delete_by_coalesce(VRF, lookups)


def delete_ipam_prefix(runner, row):
    from ipam.models import Prefix
    from ipam.models import VRF

    vrf = None
    if row.get("vrf"):
        vrf = runner._get_unique_or_raise(VRF, {"name": row["vrf"]})
        if vrf is None:
            return False
    lookups = []
    if row.get("prefix") and vrf is not None:
        lookups.append({"prefix": row["prefix"], "vrf": vrf})
    elif row.get("prefix"):
        lookups.append({"prefix": row["prefix"], "vrf__isnull": True})
    return runner._delete_by_coalesce(Prefix, lookups)


def delete_ipam_ipaddress(runner, row):
    from ipam.models import IPAddress
    from ipam.models import VRF

    vrf = None
    if row.get("vrf"):
        vrf = runner._get_unique_or_raise(VRF, {"name": row["vrf"]})
    lookups = []
    if row.get("address") and vrf is not None:
        lookups.append({"address": row["address"], "vrf": vrf})
    if row.get("address"):
        lookups.append({"address": row["address"]})
    return runner._delete_by_coalesce(IPAddress, lookups)


def delete_ipam_fhrpgroup(runner, row):
    from dcim.models import Interface
    from ipam.models import FHRPGroupAssignment
    from ipam.models import IPAddress

    group = _lookup_fhrp_group(runner, row)
    if group is None:
        return False
    device = runner._lookup_device_by_name(row.get("device"))
    interface = (
        runner._lookup_interface(device, row.get("interface")) if device else None
    )
    deleted = False
    if interface is not None:
        assignment = FHRPGroupAssignment.objects.filter(
            interface_type=runner._content_type_for(Interface),
            interface_id=interface.pk,
            group=group,
        ).first()
        if assignment is not None:
            assignment.delete()
            deleted = True

    if not FHRPGroupAssignment.objects.filter(group=group).exists():
        vrf = _fhrp_vrf(runner, row)
        ip_address = runner._get_unique_or_raise(
            IPAddress,
            {
                "address": row.get("address"),
                "vrf": vrf,
            },
        )
        if ip_address is not None:
            ip_address.delete()
            deleted = True
        group.delete()
        deleted = True
    return deleted


def apply_ipam_vlan(runner, row):
    site = (
        runner._ensure_site({"name": row["site"], "slug": row["site_slug"]})
        if row.get("site")
        else None
    )
    runner._ensure_vlan(
        vid=int(row["vid"]),
        name=row["name"],
        status=row["status"],
        site=site,
    )


def apply_ipam_vrf(runner, row):
    runner._ensure_vrf(row)


def apply_ipam_prefix(runner, row):
    from ipam.models import Prefix

    vrf = (
        runner._ensure_vrf(
            {
                "name": row["vrf"],
                "rd": None,
                "description": "",
                "enforce_unique": False,
            },
            update_existing=False,
        )
        if row.get("vrf")
        else None
    )
    coalesce_lookups = (
        [{"prefix": row["prefix"], "vrf": vrf}]
        if vrf is not None
        else [{"prefix": row["prefix"], "vrf__isnull": True}]
    )
    values = {
        "prefix": row["prefix"],
        "vrf": vrf,
        "status": row["status"],
    }
    _, created, changed = runner._coalesce_upsert(
        "ipam.prefix",
        Prefix,
        coalesce_lookups=coalesce_lookups,
        create_values=values,
        update_values=values,
        return_change=True,
    )
    return True if created or changed else "unchanged"


def _fhrp_vrf(runner, row):
    return (
        runner._ensure_vrf(
            {
                "name": row["vrf"],
                "rd": None,
                "description": "",
                "enforce_unique": False,
            },
            update_existing=False,
        )
        if row.get("vrf")
        else None
    )


def _fhrp_group_name(row):
    protocol = str(row.get("protocol") or "fhrp").strip().lower()
    group_id = str(row.get("group_id") or "").strip()
    address = str(row.get("address") or "").split("/", 1)[0]
    vrf = str(row.get("vrf") or "").strip()
    parts = [protocol, group_id]
    if vrf:
        parts.append(vrf)
    if address:
        parts.append(address)
    return "-".join(part for part in parts if part)[:100]


def _fhrp_vip_role(protocol):
    protocol = str(protocol or "").strip().lower()
    if protocol in {"vrrp2", "vrrp3"}:
        return "vrrp"
    return protocol or "vip"


def _lookup_fhrp_group(runner, row):
    from ipam.models import FHRPGroup

    return runner._get_unique_or_raise(
        FHRPGroup,
        {
            "protocol": row.get("protocol") or "hsrp",
            "group_id": int(row["group_id"]),
            "name": _fhrp_group_name(row),
        },
    )


def _find_fhrp_group_by_vip(runner, host_ip, vrf, protocol, group_id):
    """Return the FHRPGroup that currently owns the VIP at host_ip/vrf, or None.

    Used as a pre-lookup in apply_ipam_fhrpgroup so that name changes (e.g.
    VRF now present in NQE where it was absent before) don't produce a spurious
    create+delete cycle: we find the existing group via its stable VIP assignment
    instead of its mutable name field.
    """
    from ipam.models import FHRPGroup
    from ipam.models import IPAddress

    existing_ip = runner._get_unique_or_raise(
        IPAddress,
        {"address__net_host": host_ip, "vrf": vrf},
    )
    if existing_ip is None:
        return None
    ct = runner._content_type_for(FHRPGroup)
    if existing_ip.assigned_object_type_id != ct.pk:
        return None
    return FHRPGroup.objects.filter(
        pk=existing_ip.assigned_object_id,
        protocol=protocol,
        group_id=group_id,
    ).first()


def _ensure_fhrp_vip(runner, row, *, group, vrf, protocol):
    from ipam.models import FHRPGroup
    from ipam.models import IPAddress

    desired_assigned_object_type = runner._content_type_for(FHRPGroup)
    desired_assigned_object_id = group.pk
    desired_role = _fhrp_vip_role(protocol)
    host_ip = str(ip_interface(row["address"]).ip)
    existing = runner._get_unique_or_raise(
        IPAddress,
        {"address__net_host": host_ip, "vrf": vrf},
    )
    if existing is None:
        ip_address = IPAddress(
            address=row["address"],
            vrf=vrf,
            status=row["status"],
            role=desired_role,
            assigned_object_type=desired_assigned_object_type,
            assigned_object_id=desired_assigned_object_id,
        )
        ip_address.full_clean()
        ip_address.save()
        return True

    current_type_id = existing.assigned_object_type_id
    current_object_id = existing.assigned_object_id
    is_unassigned = current_type_id is None and current_object_id is None
    is_same_fhrp_group = (
        current_type_id == desired_assigned_object_type.pk
        and current_object_id == desired_assigned_object_id
    )
    if not is_unassigned and not is_same_fhrp_group:
        runner._record_aggregated_skip_warning(
            model_string="ipam.fhrpgroup",
            reason="vip-conflict",
            warning_message=(
                f"Skipping FHRP VIP `{row['address']}` for group "
                f"`{row['group_id']}` because an existing IP address is "
                "assigned to another object."
            ),
        )
        return False

    update_fields = []
    if str(existing.address) != str(row["address"]):
        existing.address = row["address"]
        update_fields.append("address")
    if existing.status != row["status"]:
        existing.status = row["status"]
        update_fields.append("status")
    if existing.role != desired_role:
        existing.role = desired_role
        update_fields.append("role")
    if is_unassigned:
        existing.assigned_object_type = desired_assigned_object_type
        existing.assigned_object_id = desired_assigned_object_id
        update_fields.extend(["assigned_object_type", "assigned_object_id"])
    if update_fields:
        existing.full_clean()
        existing.save(update_fields=update_fields)
    return True


def apply_ipam_fhrpgroup(runner, row):
    from dcim.models import Interface
    from ipam.models import FHRPGroup
    from ipam.models import FHRPGroupAssignment

    try:
        device = runner._get_device_by_name(row["device"])
    except ObjectDoesNotExist as exc:
        key = (row["device"],)
        if runner._dependency_failed("dcim.device", key):
            raise ForwardDependencySkipError(
                f"Skipping FHRP group because dependency `dcim.device` failed for {key}.",
                model_string="ipam.fhrpgroup",
                context={
                    "device": row["device"],
                    "interface": row.get("interface"),
                },
                data=row,
            ) from exc
        raise ForwardSearchError(
            f"Unable to find device `{row['device']}` for FHRP group.",
            model_string="ipam.fhrpgroup",
            context={"device": row["device"], "interface": row.get("interface")},
            data=row,
        ) from exc
    interface = runner._lookup_interface(device, row["interface"])
    if interface is None:
        key = (device.name, row["interface"])
        if runner._dependency_failed("dcim.interface", key):
            raise ForwardDependencySkipError(
                f"Skipping FHRP group because dependency `dcim.interface` failed for {key}.",
                model_string="ipam.fhrpgroup",
                context={"device": device.name, "interface": row["interface"]},
                data=row,
            )
        runner._record_aggregated_skip_warning(
            model_string="ipam.fhrpgroup",
            reason="missing-interface",
            warning_message=(
                f"Skipping FHRP group `{row['group_id']}` on `{device.name}` "
                f"`{row['interface']}` because the target interface was not imported."
            ),
        )
        return False

    vrf = _fhrp_vrf(runner, row)
    protocol = row.get("protocol") or "hsrp"
    group_name = _fhrp_group_name(row)

    # VIP-first lookup: find the group that already owns this VIP.  This
    # handles name-change scenarios (e.g. VRF data newly present in NQE)
    # without creating a spurious duplicate that immediately hits VIP conflict.
    group_created = False
    host_ip = str(ip_interface(row["address"]).ip) if row.get("address") else None
    group = (
        _find_fhrp_group_by_vip(runner, host_ip, vrf, protocol, int(row["group_id"]))
        if host_ip
        else None
    )
    if group is not None:
        # Migrate name to canonical form if it changed (e.g. VRF appeared).
        if group.name != group_name:
            group.name = group_name
            group.save(update_fields=["name"])
    else:
        group, group_created = runner._coalesce_update_or_create(
            FHRPGroup,
            coalesce_lookups=[
                {
                    "protocol": protocol,
                    "group_id": int(row["group_id"]),
                    "name": group_name,
                }
            ],
            create_values={
                "protocol": protocol,
                "group_id": int(row["group_id"]),
                "name": group_name,
                "description": "Forward FHRP group",
                "comments": "",
            },
            update_values={
                "description": "Forward FHRP group",
                "comments": "",
            },
        )

    vip_applied = _ensure_fhrp_vip(
        runner,
        row,
        group=group,
        vrf=vrf,
        protocol=protocol,
    )
    if not vip_applied:
        if (
            group_created
            and not FHRPGroupAssignment.objects.filter(group=group).exists()
        ):
            group.delete()
        return False

    runner._coalesce_update_or_create(
        FHRPGroupAssignment,
        coalesce_lookups=[
            {
                "interface_type": runner._content_type_for(Interface),
                "interface_id": interface.pk,
                "group": group,
            }
        ],
        create_values={
            "interface_type": runner._content_type_for(Interface),
            "interface_id": interface.pk,
            "group": group,
            "priority": int(row.get("priority") or 100),
        },
        update_values={"priority": int(row.get("priority") or 100)},
    )


def apply_ipam_ipaddress(runner, row):
    from dcim.models import Interface
    from ipam.models import IPAddress

    try:
        device = runner._get_device_by_name(row["device"])
    except ObjectDoesNotExist as exc:
        key = (row["device"],)
        if runner._dependency_failed("dcim.device", key):
            raise ForwardDependencySkipError(
                f"Skipping IP assignment because dependency `dcim.device` failed for {key}.",
                model_string="ipam.ipaddress",
                context={
                    "device": row["device"],
                    "interface": row.get("interface"),
                },
                data=row,
            ) from exc
        raise ForwardSearchError(
            f"Unable to find device `{row['device']}` for IP assignment.",
            model_string="ipam.ipaddress",
            context={"device": row["device"], "interface": row.get("interface")},
            data=row,
        ) from exc
    interface = runner._lookup_interface(device, row["interface"])
    if interface is None:
        key = (device.name, row["interface"])
        if runner._dependency_failed("dcim.interface", key):
            raise ForwardDependencySkipError(
                f"Skipping IP assignment because dependency `dcim.interface` failed for {key}.",
                model_string="ipam.ipaddress",
                context={"device": device.name, "interface": row["interface"]},
                data=row,
            )
        runner._record_aggregated_skip_warning(
            model_string="ipam.ipaddress",
            reason="missing-interface",
            warning_message=(
                f"Skipping IP address `{row['address']}` on `{device.name}` "
                f"`{row['interface']}` because the target interface was not imported."
            ),
        )
        return False
    skip_reason = runner._ipaddress_assignment_skip_reason(row["address"])
    if skip_reason:
        reason_label = {
            "network-id": "subnet network IDs",
            "broadcast-address": "broadcast addresses",
        }[skip_reason]
        runner._record_aggregated_skip_warning(
            model_string="ipam.ipaddress",
            reason=skip_reason,
            warning_message=(
                f"Skipping IP address `{row['address']}` on `{device.name}` "
                f"`{row['interface']}` because NetBox cannot assign {reason_label} "
                "to interfaces."
            ),
        )
        return False
    vrf = (
        runner._ensure_vrf(
            {
                "name": row["vrf"],
                "rd": None,
                "description": "",
                "enforce_unique": False,
            },
            update_existing=False,
        )
        if row.get("vrf")
        else None
    )
    if vrf is None:
        host_ip = row.get("host_ip") or str(ip_interface(row["address"]).ip)
        desired_type = runner._content_type_for(Interface)
        # A reused /30 link range can leave several global (VRF-less) IPs with the
        # same host. _get_unique_or_raise would fail the row; instead resolve
        # deterministically — prefer the copy already on this interface, else the
        # lowest pk — and warn so the duplicate is visible without breaking sync.
        global_matches = list(
            IPAddress.objects.filter(
                address__net_host=host_ip, vrf__isnull=True
            ).order_by("pk")
        )
        if len(global_matches) > 1:
            runner._record_aggregated_skip_warning(
                model_string="ipam.ipaddress",
                reason="duplicate-global-ip",
                warning_message=(
                    f"Multiple global IP addresses exist for `{host_ip}`; assigning "
                    f"`{row['address']}` to `{device.name}` `{row['interface']}` to "
                    "one of them. Deduplicate the global table to remove the others."
                ),
            )
        existing = next(
            (
                match
                for match in global_matches
                if match.assigned_object_type_id == desired_type.pk
                and match.assigned_object_id == interface.pk
            ),
            global_matches[0] if global_matches else None,
        )
        if existing is None:
            existing = IPAddress(
                address=row["address"],
                vrf=None,
                status=row["status"],
                assigned_object_type=desired_type,
                assigned_object_id=interface.pk,
            )
            existing.full_clean()
            existing.save()
            return True
        desired_assigned_object_type = desired_type
        update_fields = []
        if str(existing.address) != str(row["address"]):
            existing.address = row["address"]
            update_fields.append("address")
        if existing.vrf_id is not None:
            existing.vrf = None
            update_fields.append("vrf")
        if existing.status != row["status"]:
            existing.status = row["status"]
            update_fields.append("status")
        if existing.assigned_object_type_id != desired_assigned_object_type.pk:
            existing.assigned_object_type = desired_assigned_object_type
            update_fields.append("assigned_object_type")
        if existing.assigned_object_id != interface.pk:
            existing.assigned_object_id = interface.pk
            update_fields.append("assigned_object_id")
        if update_fields:
            existing.save(update_fields=update_fields)
        return True
    runner._upsert_values_from_defaults(
        "ipam.ipaddress",
        IPAddress,
        values={
            "address": row["address"],
            "vrf": vrf,
            "status": row["status"],
            "assigned_object_type": runner._content_type_for(Interface),
            "assigned_object_id": interface.pk,
        },
        coalesce_sets=runner._coalesce_sets_for(
            "ipam.ipaddress",
            [("address", "vrf")],
        ),
    )
