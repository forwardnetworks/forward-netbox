"""Pinned, set-based model loaders used only by the first-baseline path.

Each loader either proves that every input row fits its narrow contract and
returns ``True`` after a bulk write, or returns ``False`` without writing so
the normal adapter can preserve behavior for an unfamiliar shape.
"""

from collections import defaultdict
from ipaddress import ip_address
from ipaddress import ip_interface
from ipaddress import ip_network
from itertools import chain

from core.models import ObjectType
from django.db import transaction

_BATCH_SIZE = 5_000
FAST_BASELINE_SET_BASED_MODELS = frozenset(
    {
        "dcim.interface",
        "dcim.inventoryitem",
        "dcim.macaddress",
        "dcim.cable",
        "ipam.ipaddress",
        "ipam.prefix",
        "netbox_dlm.vulnerability",
    }
)
FAST_BASELINE_ADAPTER_CONTRACT_MODELS = frozenset(
    {
        "dcim.module",
        "extras.taggeditem",
        "ipam.fhrpgroup",
        "ipam.vlan",
        "netbox_routing.bgpaddressfamily",
        "netbox_routing.bgppeer",
        "netbox_routing.bgppeeraddressfamily",
        "netbox_routing.ospfarea",
        "netbox_routing.ospfinstance",
        "netbox_routing.ospfinterface",
    }
)
MODULE_NATIVE_INVENTORY_PART_TYPES = frozenset(
    {"FABRIC MODULE", "LINE CARD", "ROUTING ENGINE", "SUPERVISOR"}
)


def _bulk_create(model, objects):
    if objects:
        model.objects.bulk_create(objects, batch_size=_BATCH_SIZE)


def _record_applied(runner, model_string, amount):
    runner.logger.increment_statistics(
        model_string,
        outcome="applied",
        amount=amount,
    )


def _record_side_changes(runner, amount):
    ingestion = runner.ingestion
    ingestion._fast_baseline_side_changes = int(
        getattr(ingestion, "_fast_baseline_side_changes", 0)
    ) + int(amount)


def _interfaces_by_key(keys):
    from dcim.models import Interface

    device_names = {device for device, _ in keys}
    interface_names = {name for _, name in keys}
    return {
        (interface.device.name, interface.name): interface
        for interface in Interface.objects.select_related("device").filter(
            device__name__in=device_names,
            name__in=interface_names,
        )
        if (interface.device.name, interface.name) in keys
    }


def _assignable_address(value):
    try:
        address = ip_interface(str(value))
    except ValueError:
        return False
    if address.network.prefixlen == address.max_prefixlen:
        return True
    return address.ip not in {
        address.network.network_address,
        address.network.broadcast_address,
    }


def bulk_load_interfaces(runner, rows):
    """Use the normal parity-tested batched Interface engine direct-to-main."""
    required = {"device", "name", "type", "enabled"}
    if any(not required.issubset(row) for row in rows):
        return False
    identities = [(str(row["device"]), str(row["name"])) for row in rows]
    if len(identities) != len(set(identities)):
        return False

    from .apply_engine_bulk import bulk_orm_apply_interface

    return bulk_orm_apply_interface(runner, rows)


def bulk_load_mac_addresses(runner, rows):
    required = {"device", "interface", "mac"}
    if any(not required.issubset(row) for row in rows):
        return False
    macs = [str(row["mac"]) for row in rows]
    if len(macs) != len(set(macs)):
        return False
    keys = {(str(row["device"]), str(row["interface"])) for row in rows}
    interfaces = _interfaces_by_key(keys)
    if set(interfaces) != keys:
        return False

    from dcim.models import Interface, MACAddress

    interface_type = ObjectType.objects.get_for_model(Interface)
    objects = [
        MACAddress(
            mac_address=row["mac"],
            assigned_object_type=interface_type,
            assigned_object_id=interfaces[
                (str(row["device"]), str(row["interface"]))
            ].pk,
        )
        for row in rows
    ]
    _bulk_create(MACAddress, objects)
    _record_applied(runner, "dcim.macaddress", len(rows))
    return True


def bulk_load_ip_addresses(runner, rows):
    """Use the normal parity-tested batched IP engine on direct-to-main.

    Real workloads legitimately contain network/broadcast rows (normal-path
    skips) and repeated host+VRF identities on different interfaces (ordered
    coalesce/reassignment).  The earlier raw bulk_create contract could not
    preserve either behavior.  The batched engine is the production apply
    implementation for this model and retains those exact semantics while the
    fast-baseline gate proves every referenced device/interface is present in
    the same admitted workload.
    """
    required = {"device", "interface", "address", "status"}
    if any(not required.issubset(row) for row in rows):
        return False
    try:
        for row in rows:
            ip_interface(str(row["address"]))
    except ValueError:
        return False

    from .apply_engine_bulk import bulk_orm_apply_ipaddress

    return bulk_orm_apply_ipaddress(runner, rows)


def bulk_load_prefixes(runner, rows):
    """Use the normal batched Prefix engine, including implicit VRF creation."""
    required = {"prefix", "status"}
    if any(not required.issubset(row) for row in rows):
        return False
    identities = [(str(row["prefix"]), str(row.get("vrf") or "")) for row in rows]
    if len(identities) != len(set(identities)):
        return False

    from .apply_engine_bulk import bulk_orm_apply_simple_models

    return bulk_orm_apply_simple_models(runner, "ipam.prefix", rows)


def _search_values(model, objects):
    """Build the exact non-empty CachedValue rows NetBox save signals build."""
    from extras.models import CachedValue
    from netbox.search import get_indexer

    try:
        fields = get_indexer(model).fields
    except KeyError:
        return []
    object_type = ObjectType.objects.get_for_model(model)
    values = []
    for obj in objects:
        for field_name, weight in fields:
            value = getattr(obj, field_name, None)
            if value in (None, ""):
                continue
            values.append(
                CachedValue(
                    object_type=object_type,
                    object_id=obj.pk,
                    field=field_name,
                    type=type(value).__name__,
                    value=str(value),
                    weight=weight,
                )
            )
    return values


def _write_search_values(*model_objects):
    from extras.models import CachedValue

    values = list(
        chain.from_iterable(
            _search_values(model, objects) for model, objects in model_objects
        )
    )
    _bulk_create(CachedValue, values)


def _inventory_rows_contract(rows, *, module_enabled):
    if module_enabled:
        rows = [row for row in rows if not _module_native_inventory_row(row)]
    required = {
        "device",
        "name",
        "part_id",
        "serial",
        "status",
        "discovered",
    }
    if any(not required.issubset(row) for row in rows):
        return False
    if any(row.get("parent") or row.get("component_id") for row in rows):
        return False
    identities = [
        (
            str(row["device"]),
            str(row["name"]),
        )
        for row in rows
    ]
    if len(identities) != len(set(identities)):
        return False
    asset_tags = [str(row["asset_tag"]) for row in rows if row.get("asset_tag")]
    if len(asset_tags) != len(set(asset_tags)):
        return False

    roles_by_slug = {}
    manufacturers_by_name = {}
    manufacturer_names_by_slug = {}
    for row in rows:
        if row.get("role"):
            role = (
                str(row["role"]),
                str(row.get("role_slug") or ""),
                str(row.get("role_color") or ""),
            )
            if not role[1] or not role[2]:
                return False
            if roles_by_slug.setdefault(role[1], role) != role:
                return False
        if row.get("manufacturer"):
            name = str(row["manufacturer"])
            slug = str(row.get("manufacturer_slug") or "")
            if not slug:
                return False
            if manufacturers_by_name.setdefault(name, slug) != slug:
                return False
            if manufacturer_names_by_slug.setdefault(slug, name) != name:
                return False
    return True


def _inventory_contract(runner, rows):
    return _inventory_rows_contract(
        rows,
        module_enabled=runner.sync.is_model_enabled("dcim.module"),
    )


def bulk_load_inventory_items(runner, rows):
    """Load flat, component-free InventoryItems and their role/search state."""
    if not _inventory_contract(runner, rows):
        return False
    omitted_module_rows = []
    if runner.sync.is_model_enabled("dcim.module"):
        omitted_module_rows = [
            row for row in rows if runner._is_module_native_inventory_row(row)
        ]
        rows = [row for row in rows if not runner._is_module_native_inventory_row(row)]

    from dcim.models import Device, InventoryItem, InventoryItemRole, Manufacturer

    device_by_name = {
        obj.name: obj
        for obj in Device.objects.only(
            "id", "name", "site_id", "location_id", "rack_id"
        ).filter(name__in={str(row["device"]) for row in rows})
    }
    if len(device_by_name) != len({str(row["device"]) for row in rows}):
        return False

    manufacturer_specs = {
        str(row["manufacturer"]): str(row["manufacturer_slug"])
        for row in rows
        if row.get("manufacturer")
    }
    for name, slug in sorted(manufacturer_specs.items()):
        runner._ensure_manufacturer({"name": name, "slug": slug})
    manufacturer_names = set(manufacturer_specs)
    manufacturers = {
        obj.name: obj
        for obj in Manufacturer.objects.filter(name__in=manufacturer_names)
    }
    if set(manufacturers) != manufacturer_names:
        return False

    role_specs = {}
    for row in rows:
        if not row.get("role"):
            continue
        spec = (
            str(row["role"]),
            str(row.get("role_slug") or ""),
            str(row.get("role_color") or ""),
        )
        if not spec[1] or not spec[2]:
            return False
        prior = role_specs.setdefault(spec[1], spec)
        if prior != spec:
            return False

    with transaction.atomic():
        existing_roles = {
            role.slug: role
            for role in InventoryItemRole.objects.filter(slug__in=role_specs)
        }
        roles = [
            InventoryItemRole(name=name, slug=slug, color=color)
            for name, slug, color in role_specs.values()
            if slug not in existing_roles
        ]
        _bulk_create(InventoryItemRole, roles)
        role_by_slug = {**existing_roles, **{role.slug: role for role in roles}}

        objects = []
        # No tree bookkeeping is assembled here any more. NetBox 4.7 dropped
        # `lft`/`rght`/`tree_id`/`level` from InventoryItem when ltree replaced
        # django-mptt, and the `path` column that replaced them is maintained by
        # a database trigger, which fires for these inserts like any other.
        for row in rows:
            device = device_by_name[str(row["device"])]
            objects.append(
                InventoryItem(
                    device=device,
                    name=str(row["name"]),
                    label=str(row.get("label") or ""),
                    description=str(row.get("description") or ""),
                    status=row["status"],
                    role=role_by_slug.get(str(row.get("role_slug") or "")),
                    manufacturer=manufacturers.get(str(row.get("manufacturer") or "")),
                    part_id=str(row.get("part_id") or ""),
                    serial=str(row.get("serial") or ""),
                    asset_tag=row.get("asset_tag") or None,
                    discovered=bool(row["discovered"]),
                    _site_id=device.site_id,
                    _location_id=device.location_id,
                    _rack_id=device.rack_id,
                )
            )
        _bulk_create(InventoryItem, objects)
        # The current branch merge carries InventoryItem search entries to main
        # but not the role objects' signal-derived search entries.
        _write_search_values((InventoryItem, objects))

    _record_applied(runner, "dcim.inventoryitem", len(rows))
    if omitted_module_rows:
        runner.logger.increment_statistics(
            "dcim.inventoryitem",
            outcome="skipped",
            amount=len(omitted_module_rows),
        )
    _record_side_changes(runner, len(roles))
    return True


def _cable_contract(rows):
    required = {"device", "interface", "remote_device", "remote_interface", "status"}
    if any(not required.issubset(row) for row in rows):
        return False
    endpoint_pairs = []
    all_endpoints = []
    for row in rows:
        a = (str(row["device"]), str(row["interface"]))
        b = (str(row["remote_device"]), str(row["remote_interface"]))
        if a == b:
            return False
        endpoint_pairs.append(tuple(sorted((a, b))))
        all_endpoints.extend((a, b))
    return len(endpoint_pairs) == len(set(endpoint_pairs)) and len(
        all_endpoints
    ) == len(set(all_endpoints))


def bulk_load_cables(runner, rows):
    """Load simple Interface-to-Interface cables and derived path/search state."""
    if not _cable_contract(rows):
        return False

    from dcim.models import Cable, CableTermination, Interface

    names_by_device = defaultdict(set)
    for row in rows:
        names_by_device[str(row["device"])].add(str(row["interface"]))
        names_by_device[str(row["remote_device"])].add(str(row["remote_interface"]))
    expected = set(
        chain.from_iterable(
            ((device, name) for name in names)
            for device, names in names_by_device.items()
        )
    )
    interface_by_key = _interfaces_by_key(expected)
    if set(interface_by_key) != expected:
        return False
    # CableTermination is locked and proven empty by the outer eligibility
    # transaction, so no endpoint can already be cabled here. Avoid the
    # GenericRelation-backed ``cable_id`` property: reading it would issue one
    # query per endpoint and recreate the very deceleration this path removes.
    if any(
        str(interface.type or "").lower() == "lag" or interface._path_id is not None
        for interface in interface_by_key.values()
    ):
        return False

    interface_type = ObjectType.objects.get_for_model(Interface)
    with transaction.atomic():
        cables = [Cable(status=row["status"]) for row in rows]
        _bulk_create(Cable, cables)

        terminations = []
        for cable, row in zip(cables, rows, strict=True):
            a = interface_by_key[(str(row["device"]), str(row["interface"]))]
            b = interface_by_key[
                (str(row["remote_device"]), str(row["remote_interface"]))
            ]
            for end, interface in (("A", a), ("B", b)):
                terminations.append(
                    CableTermination(
                        cable=cable,
                        cable_end=end,
                        termination_type=interface_type,
                        termination_id=interface.pk,
                        _device_id=interface.device_id,
                        _rack_id=interface.device.rack_id,
                        _location_id=interface.device.location_id,
                        _site_id=interface.device.site_id,
                    )
                )
        _bulk_create(CableTermination, terminations)

        # The pinned current branch-merge path materializes Cable and
        # CableTermination rows but does not retain signal-derived CablePath or
        # endpoint CachedValue rows. Reproducing those direct-save side effects
        # here would make the baseline state *different* from the current path.

    _record_applied(runner, "dcim.cable", len(rows))
    _record_side_changes(runner, len(terminations))
    return True


def _dlm_contract(rows):
    required = {"name", "platform_slug", "version", "cve_id"}
    if any(
        not required.issubset(row) or not all(str(row[key]).strip() for key in required)
        for row in rows
    ):
        return False
    identities = [
        (
            str(row["name"]).strip(),
            str(row["platform_slug"]).strip(),
            str(row["version"]).strip(),
            str(row["cve_id"]).strip(),
        )
        for row in rows
    ]
    if len(identities) != len(set(identities)):
        return False
    device_versions = {}
    for device, platform, version, _ in identities:
        prior = device_versions.setdefault(device, (platform, version))
        if prior != (platform, version):
            return False
    return True


def fast_baseline_workload_contract(sync, workloads):
    """Prove every specialized row shape before the first target mutation."""
    rows_by_model = defaultdict(list)
    for workload in workloads:
        rows_by_model[str(workload.model_string)].extend(workload.upsert_rows)

    adapter_valid, adapter_context = _adapter_workload_contract(rows_by_model)
    if not adapter_valid:
        return False, "unsupported_row_contract", adapter_context

    for model_string in sorted(FAST_BASELINE_SET_BASED_MODELS):
        rows = rows_by_model.get(model_string, [])
        if not rows:
            continue
        valid = True
        if model_string == "dcim.interface":
            required = {"device", "name", "type", "enabled"}
            identities = [
                (str(row.get("device")), str(row.get("name"))) for row in rows
            ]
            device_names = {
                str(row.get("name") or "")
                for row in rows_by_model.get("dcim.device", [])
            }
            valid = (
                all(required.issubset(row) for row in rows)
                and len(identities) == len(set(identities))
                and all(device in device_names for device, _ in identities)
            )
        elif model_string == "dcim.inventoryitem":
            admitted_rows = (
                [row for row in rows if not _module_native_inventory_row(row)]
                if sync.is_model_enabled("dcim.module")
                else rows
            )
            device_names = {
                str(row.get("name") or "")
                for row in rows_by_model.get("dcim.device", [])
            }
            valid = _inventory_rows_contract(
                rows,
                module_enabled=sync.is_model_enabled("dcim.module"),
            ) and all(
                str(row.get("device") or "") in device_names for row in admitted_rows
            )
        elif model_string == "dcim.macaddress":
            required = {"device", "interface", "mac"}
            identities = [str(row.get("mac")) for row in rows]
            valid = all(required.issubset(row) for row in rows) and len(
                identities
            ) == len(set(identities))
        elif model_string == "dcim.cable":
            valid = _cable_contract(rows)
        elif model_string == "ipam.ipaddress":
            required = {"device", "interface", "address", "status"}
            interface_keys = {
                (str(row.get("device")), str(row.get("name")))
                for row in rows_by_model.get("dcim.interface", [])
            }
            try:
                for row in rows:
                    ip_interface(str(row["address"]))
            except (KeyError, ValueError):
                valid = False
            else:
                valid = all(required.issubset(row) for row in rows) and all(
                    (str(row["device"]), str(row["interface"])) in interface_keys
                    for row in rows
                )
        elif model_string == "ipam.prefix":
            required = {"prefix", "status"}
            try:
                identities = [
                    (
                        str(ip_network(str(row["prefix"]), strict=False)),
                        str(row.get("vrf") or ""),
                    )
                    for row in rows
                ]
            except (KeyError, ValueError):
                valid = False
            else:
                valid = all(required.issubset(row) for row in rows) and len(
                    identities
                ) == len(set(identities))
        else:
            valid = _dlm_contract(rows)
        if not valid:
            return False, "unsupported_row_contract", {"model": model_string}
    return True, "supported_row_contracts", {}


def _module_native_inventory_row(row):
    return (
        row.get("module_component") is True
        or row.get("part_type") in MODULE_NATIVE_INVENTORY_PART_TYPES
    )


def _required_rows(model_string, rows):
    from .sync_contracts import MODEL_SYNC_CONTRACTS

    required = set(MODEL_SYNC_CONTRACTS[model_string].required_fields)
    blank_allowed = {"enabled", "group_id", "part_number", "process_id"}
    return all(
        required.issubset(row)
        and all(
            key in blank_allowed or row.get(key) not in (None, "") for key in required
        )
        for row in rows
    )


def _routing_af(value):
    value = str(value or "").strip()
    if "." in value:
        value = value.rsplit(".", 1)[-1]
    value = value.lower().replace("_", "-")
    return {
        "ipv4-any": "ipv4-unicast",
        "ipv6-any": "ipv6-unicast",
        "l3vpn-ipv4-any": "vpnv4-unicast",
        "l3vpn-ipv4-unicast": "vpnv4-unicast",
        "l3vpn-ipv6-any": "vpnv6-unicast",
        "l3vpn-ipv6-unicast": "vpnv6-unicast",
        "l3vpn-ipv6-multicast": "vpnv6-multicast",
        "nsap-unicast": "nsap",
    }.get(value, value)


def _adapter_workload_contract(rows_by_model):
    """Prove relationship adapters can resolve every first-load dependency."""
    from .sync_routing_impl import routing_interface_lookup_candidates

    enabled = FAST_BASELINE_ADAPTER_CONTRACT_MODELS.intersection(rows_by_model)
    for model_string in sorted(enabled):
        if not _required_rows(model_string, rows_by_model[model_string]):
            return False, {"model": model_string, "reason": "required_fields"}

    devices = {
        str(row.get("name") or "").strip()
        for row in rows_by_model.get("dcim.device", [])
        if str(row.get("name") or "").strip()
    }
    interfaces = {
        (
            str(row.get("device") or "").strip(),
            str(row.get("name") or "").strip(),
        )
        for row in rows_by_model.get("dcim.interface", [])
        if str(row.get("device") or "").strip() and str(row.get("name") or "").strip()
    }

    def parents(model_string, rows, *, interface=False):
        for row in rows:
            device = str(row.get("device") or "").strip()
            if device and device not in devices:
                return False, {"model": model_string, "reason": "missing_device"}
            if interface:
                name = str(
                    row.get("interface") or row.get("local_interface") or ""
                ).strip()
                candidates = {name}
                if model_string == "netbox_routing.ospfinterface":
                    candidates.update(routing_interface_lookup_candidates(name))
                if not any(
                    (device, candidate) in interfaces for candidate in candidates
                ):
                    return False, {
                        "model": model_string,
                        "reason": "missing_interface",
                    }
        return True, {}

    module_rows = rows_by_model.get("dcim.module", [])
    ok, context = parents("dcim.module", module_rows)
    if not ok:
        return ok, context

    tag_rows = rows_by_model.get("extras.taggeditem", [])
    ok, context = parents("extras.taggeditem", tag_rows)
    if not ok:
        return ok, context

    vlan_rows = rows_by_model.get("ipam.vlan", [])
    try:
        vlan_ids = [
            (str(row.get("site") or ""), int(row.get("vid"))) for row in vlan_rows
        ]
    except (TypeError, ValueError):
        return False, {"model": "ipam.vlan", "reason": "invalid_vid"}
    if any(not 1 <= vid <= 4094 for _, vid in vlan_ids):
        return False, {"model": "ipam.vlan", "reason": "invalid_vid"}
    for row in vlan_rows:
        if row.get("site") and not row.get("site_slug"):
            return False, {"model": "ipam.vlan", "reason": "missing_site_slug"}

    fhrp_rows = rows_by_model.get("ipam.fhrpgroup", [])
    ok, context = parents("ipam.fhrpgroup", fhrp_rows, interface=True)
    if not ok:
        return ok, context
    try:
        for row in fhrp_rows:
            int(row.get("group_id"))
            ip_interface(str(row.get("address")))
    except (TypeError, ValueError):
        return False, {"model": "ipam.fhrpgroup", "reason": "invalid_identity"}

    bgp_models = (
        "netbox_routing.bgppeer",
        "netbox_routing.bgpaddressfamily",
        "netbox_routing.bgppeeraddressfamily",
    )
    for model_string in bgp_models:
        rows = rows_by_model.get(model_string, [])
        ok, context = parents(model_string, rows)
        if not ok:
            return ok, context
        try:
            for row in rows:
                int(row.get("local_asn"))
                if "peer_asn" in row:
                    int(row.get("peer_asn"))
                if "neighbor_address" in row:
                    ip_address(str(row.get("neighbor_address")))
                if "afi_safi" in row and not _routing_af(row.get("afi_safi")):
                    raise ValueError
        except (TypeError, ValueError):
            return False, {"model": model_string, "reason": "invalid_routing_identity"}

    af_rows = rows_by_model.get("netbox_routing.bgpaddressfamily", [])
    peer_af_rows = rows_by_model.get("netbox_routing.bgppeeraddressfamily", [])
    # Unconditional before, which crashed the whole fast baseline with
    # ModuleNotFoundError on NetBox 4.7 - netbox-routing caps at a max_version
    # in the 4.6 series and cannot be installed there, so this import always
    # failed and it failed LOUDLY in a contract function rather than declining.
    #
    # Nothing to validate is not the same as failing to validate: with no
    # routing rows there is no address family to check and the contract has
    # nothing to say. Rows for a model whose plugin is absent is a different
    # thing entirely, and fails closed below.
    if af_rows or peer_af_rows:
        try:
            from netbox_routing.models import BGPAddressFamily
        except ImportError:
            return False, {
                "model": "netbox_routing.bgpaddressfamily",
                "reason": "routing_plugin_not_installed",
            }
        allowed_address_families = {
            str(choice[0])
            for choice in BGPAddressFamily._meta.get_field("address_family").choices
        }
    else:
        allowed_address_families = set()
    if allowed_address_families and any(
        _routing_af(row.get("afi_safi")) not in allowed_address_families
        for row in [*af_rows, *peer_af_rows]
    ):
        return False, {
            "model": "netbox_routing.bgpaddressfamily",
            "reason": "unsupported_address_family",
        }
    instance_rows = rows_by_model.get("netbox_routing.ospfinstance", [])
    ok, context = parents("netbox_routing.ospfinstance", instance_rows)
    if not ok:
        return ok, context
    ospf_interface_rows = rows_by_model.get("netbox_routing.ospfinterface", [])
    ok, context = parents(
        "netbox_routing.ospfinterface", ospf_interface_rows, interface=True
    )
    if not ok:
        return ok, context
    return True, {}


def bulk_load_vulnerabilities(runner, rows):
    """Load minimal vulnerability findings and all required DLM side tables."""
    if not _dlm_contract(rows):
        return False

    from dcim.models import Device, Platform

    try:
        from netbox_dlm.models import CVE
        from netbox_dlm.models import DeviceSoftware
        from netbox_dlm.models import SoftwareVersion
        from netbox_dlm.models import Vulnerability
    except ImportError:
        # Reachable only with rows for a model whose plugin is absent, which is
        # a refusal rather than a crash - the same shape as the routing guard.
        return False

    device_names = {str(row["name"]).strip() for row in rows}
    platform_slugs = {str(row["platform_slug"]).strip() for row in rows}
    devices = {obj.name: obj for obj in Device.objects.filter(name__in=device_names)}
    platforms = {
        obj.slug: obj for obj in Platform.objects.filter(slug__in=platform_slugs)
    }
    if set(devices) != device_names or set(platforms) != platform_slugs:
        return False

    version_keys = list(
        dict.fromkeys(
            (str(row["platform_slug"]).strip(), str(row["version"]).strip())
            for row in rows
        )
    )
    cve_ids = list(dict.fromkeys(str(row["cve_id"]).strip() for row in rows))
    device_version_keys = dict.fromkeys(
        (
            str(row["name"]).strip(),
            str(row["platform_slug"]).strip(),
            str(row["version"]).strip(),
        )
        for row in rows
    )
    existing_versions = {
        (obj.platform.slug, obj.version): obj
        for obj in SoftwareVersion.objects.select_related("platform").filter(
            platform__slug__in=platform_slugs,
            version__in={version for _, version in version_keys},
        )
    }
    existing_device_software = {
        obj.device.name: obj
        for obj in DeviceSoftware.objects.select_related(
            "device", "software_version__platform"
        ).filter(device__name__in=device_names)
    }
    for device, platform, version in device_version_keys:
        existing = existing_device_software.get(device)
        if existing is not None and (
            existing.software_version.platform.slug,
            existing.software_version.version,
        ) != (platform, version):
            return False
    existing_cves = {obj.cve_id: obj for obj in CVE.objects.filter(cve_id__in=cve_ids)}

    with transaction.atomic():
        versions = [
            SoftwareVersion(platform=platforms[platform], version=version)
            for platform, version in version_keys
            if (platform, version) not in existing_versions
        ]
        _bulk_create(SoftwareVersion, versions)
        version_by_key = {
            **existing_versions,
            **{
                (version.platform.slug, version.version): version
                for version in versions
            },
        }

        device_software = [
            DeviceSoftware(
                device=devices[device],
                software_version=version_by_key[(platform, version)],
            )
            for device, platform, version in device_version_keys
            if device not in existing_device_software
        ]
        _bulk_create(DeviceSoftware, device_software)

        cves = [CVE(cve_id=cve_id) for cve_id in cve_ids if cve_id not in existing_cves]
        _bulk_create(CVE, cves)
        cve_by_id = {**existing_cves, **{cve.cve_id: cve for cve in cves}}
        vulnerabilities = [
            Vulnerability(
                device=devices[str(row["name"]).strip()],
                software_version=version_by_key[
                    (
                        str(row["platform_slug"]).strip(),
                        str(row["version"]).strip(),
                    )
                ],
                cve=cve_by_id[str(row["cve_id"]).strip()],
            )
            for row in rows
        ]
        _bulk_create(Vulnerability, vulnerabilities)

        through = CVE.affected_software.through
        relation_pairs = list(
            dict.fromkeys(
                (
                    cve_by_id[str(row["cve_id"]).strip()].pk,
                    version_by_key[
                        (
                            str(row["platform_slug"]).strip(),
                            str(row["version"]).strip(),
                        )
                    ].pk,
                )
                for row in rows
            )
        )
        cve_field = next(
            field for field in through._meta.fields if field.related_model is CVE
        )
        version_field = next(
            field
            for field in through._meta.fields
            if field.related_model is SoftwareVersion
        )
        relations = [
            through(
                **{
                    cve_field.attname: cve_id,
                    version_field.attname: version_id,
                }
            )
            for cve_id, version_id in relation_pairs
        ]
        if relations:
            through.objects.bulk_create(
                relations,
                batch_size=_BATCH_SIZE,
                ignore_conflicts=True,
            )

    _record_applied(runner, "netbox_dlm.vulnerability", len(rows))
    _record_side_changes(
        runner,
        len(versions) + len(device_software) + len(cves),
    )
    return True


def fast_baseline_apply_upserts(runner, model_string, rows):
    """Dispatch a pinned direct-load spec, returning False for normal fallback."""
    if model_string == "dcim.interface":
        return bulk_load_interfaces(runner, rows)
    if model_string == "dcim.inventoryitem":
        return bulk_load_inventory_items(runner, rows)
    if model_string == "dcim.macaddress":
        return bulk_load_mac_addresses(runner, rows)
    if model_string == "dcim.cable":
        return bulk_load_cables(runner, rows)
    if model_string == "ipam.ipaddress":
        return bulk_load_ip_addresses(runner, rows)
    if model_string == "ipam.prefix":
        return bulk_load_prefixes(runner, rows)
    if model_string == "netbox_dlm.vulnerability":
        return bulk_load_vulnerabilities(runner, rows)
    return False
