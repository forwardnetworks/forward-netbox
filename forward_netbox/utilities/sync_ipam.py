from ipaddress import ip_interface

from django.core.exceptions import ObjectDoesNotExist

from ..exceptions import ForwardDependencySkipError
from ..exceptions import ForwardSearchError


def delete_ipam_vlan(runner, row):
    from dcim.models import Site
    from ipam.models import VLAN

    site = None
    if row.get("site_slug"):
        site = Site.objects.filter(slug=row["site_slug"]).order_by("pk").first()
    if site is None and row.get("site"):
        site = Site.objects.filter(name=row["site"]).order_by("pk").first()
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
        vrf = VRF.objects.filter(name=row["vrf"]).order_by("pk").first()
    lookups = []
    if row.get("prefix") and vrf is not None:
        lookups.append({"prefix": row["prefix"], "vrf": vrf})
    if row.get("prefix"):
        lookups.append({"prefix": row["prefix"]})
    return runner._delete_by_coalesce(Prefix, lookups)


def delete_ipam_ipaddress(runner, row):
    from ipam.models import IPAddress
    from ipam.models import VRF

    vrf = None
    if row.get("vrf"):
        vrf = VRF.objects.filter(name=row["vrf"]).order_by("pk").first()
    lookups = []
    if row.get("address") and vrf is not None:
        lookups.append({"address": row["address"], "vrf": vrf})
    if row.get("address"):
        lookups.append({"address": row["address"]})
    return runner._delete_by_coalesce(IPAddress, lookups)


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
            }
        )
        if row.get("vrf")
        else None
    )
    runner._upsert_values_from_defaults(
        "ipam.prefix",
        Prefix,
        values={
            "prefix": row["prefix"],
            "vrf": vrf,
            "status": row["status"],
        },
        coalesce_sets=runner._coalesce_sets_for(
            "ipam.prefix",
            [("prefix", "vrf")],
        ),
    )


def apply_ipam_ipaddress(runner, row):
    from dcim.models import Device
    from dcim.models import Interface
    from ipam.models import IPAddress

    try:
        device = Device.objects.get(name=row["device"])
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
            }
        )
        if row.get("vrf")
        else None
    )
    if vrf is None:
        host_ip = row.get("host_ip") or str(ip_interface(row["address"]).ip)
        lookup = {
            "address__net_host": host_ip,
            "vrf__isnull": True,
        }
        existing = runner._get_unique_or_raise(IPAddress, lookup)
        if existing is None:
            existing = IPAddress(
                address=row["address"],
                vrf=None,
                status=row["status"],
                assigned_object_type=runner._content_type_for(Interface),
                assigned_object_id=interface.pk,
            )
            existing.full_clean()
            existing.save()
            return True
        existing.address = row["address"]
        existing.vrf = None
        existing.status = row["status"]
        existing.assigned_object_type = runner._content_type_for(Interface)
        existing.assigned_object_id = interface.pk
        existing.save(
            update_fields=[
                "address",
                "vrf",
                "status",
                "assigned_object_type",
                "assigned_object_id",
            ]
        )
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
