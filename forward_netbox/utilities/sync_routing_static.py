# netbox-routing static routes.
#
# Forward's structured model has no configured-static-route field, so the
# bundled map reads `ip route` / `ipv6 route` lines out of `device.files.config`
# and emits one row per configured line carrying the line's arguments verbatim.
# This module parses them and writes netbox-routing `StaticRoute` objects.
#
# The destination shape drives the design:
#
# - **A StaticRoute is shared by every device that configures it.** It has a
#   `devices` many-to-many and is unique on (vrf, prefix, next hop), so one
#   default route configured on 400 switches is one object with 400 devices,
#   not 400 objects. Each row therefore adds its device to the route, and its
#   removal takes only that device off.
# - **One object cannot carry per-device attributes.** Distance, tag, name and
#   permanent are single values, so the device whose name sorts first is the
#   route's owner and the only one whose row writes them. The result does not
#   depend on the order rows arrive in.
# - **The unique key is nullable.** PostgreSQL treats NULLs as distinct, so a
#   route in the global table (no VRF) is not protected from duplicates by the
#   database; the lookup below is what keeps it to one.
#
# Removal is deliberately narrow: a route is deleted only when its last device
# leaves AND it carries this module's comment marker, so a route an operator
# created by hand is never deleted by the sync.
from ipaddress import ip_address
from ipaddress import ip_network

from django.core.exceptions import ObjectDoesNotExist

from .sync_reporting import STATIC_ROUTE_UNREADABLE_REASON
from .sync_routing_impl import lookup_device_for_routing
from .sync_routing_impl import preview_leaf_outcome
from .sync_routing_impl import routing_vrf
from .sync_routing_impl import VRF_ABSENT

STATIC_ROUTE_MODEL = "netbox_routing.staticroute"
ROUTING_STATIC_ROLLUP_REASONS = frozenset({STATIC_ROUTE_UNREADABLE_REASON})
STATIC_ROUTE_MARKER = "Observed by Forward from device configuration."
_NAME_MAX_LENGTH = 50
_DISTANCE_MAX = 255


class UnreadableStaticRoute(ValueError):
    """A configured line this adapter will not guess at."""

    def __init__(self, reason):
        super().__init__(reason)
        self.reason = reason


def _is_ip(token):
    try:
        ip_address(token)
    except ValueError:
        return False
    return True


def parse_static_route_args(family, args):
    """The structured route for one configured line's arguments.

    ``args`` is everything after ``ip route [vrf NAME] `` (or the ``ipv6``
    form). Returns ``{prefix, next_hop, interface, distance, tag, name,
    permanent}`` or raises `UnreadableStaticRoute` with a reason. Platforms
    differ in token order, so the destination and the next hop are read first
    and the optional keywords after, in whatever order they come.
    """
    tokens = str(args or "").split()
    if not tokens:
        raise UnreadableStaticRoute("empty")
    index = 0
    try:
        if "/" in tokens[0]:
            network = ip_network(tokens[0], strict=False)
            index = 1
        elif family == "ip" and len(tokens) > 1 and _is_ip(tokens[1]):
            # IOS writes the destination as address and netmask.
            network = ip_network(f"{tokens[0]}/{tokens[1]}", strict=False)
            index = 2
        else:
            raise UnreadableStaticRoute("destination")
    except ValueError as exc:
        raise UnreadableStaticRoute("destination") from exc
    if index >= len(tokens):
        raise UnreadableStaticRoute("no_next_hop")

    next_hop = None
    interface = None
    target = tokens[index]
    index += 1
    if _is_ip(target):
        next_hop = target
    else:
        interface = target
        if index < len(tokens) and _is_ip(tokens[index]):
            next_hop = tokens[index]
            index += 1
    if next_hop is not None and ip_address(next_hop).version != network.version:
        raise UnreadableStaticRoute("family_mismatch")

    distance = None
    tag = None
    name = None
    permanent = False
    rest = tokens[index:]
    position = 0
    while position < len(rest):
        token = rest[position]
        lowered = token.lower()
        if lowered == "tag" and position + 1 < len(rest):
            tag = rest[position + 1] if rest[position + 1].isdigit() else None
            position += 2
        elif lowered == "name" and position + 1 < len(rest):
            name = rest[position + 1][:_NAME_MAX_LENGTH]
            position += 2
        elif lowered == "permanent":
            permanent = True
            position += 1
        elif lowered in ("track", "vrf", "distance", "metric") and position + 1 < len(
            rest
        ):
            # `track N` is liveness tracking and `vrf X` a leak target:
            # neither is something a StaticRoute holds.
            position += 2
        elif token.isdigit():
            if distance is None and int(token) <= _DISTANCE_MAX:
                distance = int(token)
            position += 1
        else:
            position += 1
    return {
        "prefix": str(network),
        "next_hop": next_hop,
        # The model's interface field is for routes without an IP next hop.
        "interface": None if next_hop else interface,
        "distance": distance,
        "tag": int(tag) if tag is not None else None,
        "name": name,
        "permanent": permanent,
    }


def static_route_comments(row):
    return STATIC_ROUTE_MARKER


def _skip_unreadable(runner, row, exc):
    warn = getattr(runner, "_record_aggregated_skip_warning", None)
    if warn is not None:
        warn(
            model_string=STATIC_ROUTE_MODEL,
            reason=STATIC_ROUTE_UNREADABLE_REASON,
            warning_message=(
                f"Static route line could not be read ({exc.reason}); it was "
                "skipped rather than guessed."
            ),
            sample=f"{row.get('device')} {row.get('family')} route ({exc.reason})",
        )


def _route_lookup(vrf, parsed):
    lookup = {"vrf": vrf, "prefix": parsed["prefix"]}
    if parsed["next_hop"] is None:
        lookup["next_hop__isnull"] = True
        lookup["interface_next_hop"] = parsed["interface"]
    else:
        lookup["next_hop"] = parsed["next_hop"]
    if vrf is None:
        lookup.pop("vrf")
        lookup["vrf__isnull"] = True
    return lookup


def _existing_route(StaticRoute, vrf, parsed):
    return (
        StaticRoute.objects.filter(**_route_lookup(vrf, parsed)).order_by("pk").first()
    )


def ensure_static_route(runner, row, *, preview=False):
    """Create or update the route and add the row's device to it.

    Returns the route, ``False`` for a line that was skipped, or ``None``
    under preview when a parent (the VRF) does not exist yet, which is a create.
    """
    StaticRoute = runner._optional_model(
        "netbox_routing", "StaticRoute", STATIC_ROUTE_MODEL
    )
    try:
        parsed = parse_static_route_args(row.get("family"), row.get("args"))
    except UnreadableStaticRoute as exc:
        _skip_unreadable(runner, row, exc)
        return False
    device = lookup_device_for_routing(runner, row, STATIC_ROUTE_MODEL, "static route")
    vrf = routing_vrf(runner, row, preview=preview)
    if vrf is VRF_ABSENT:
        return None

    existing = _existing_route(StaticRoute, vrf, parsed)
    held_by = (
        set(existing.devices.values_list("name", flat=True)) if existing else set()
    )
    held_by.add(device.name)
    is_owner = device.name == min(held_by)
    if existing is not None and not is_owner:
        # Another device sorts first and owns the attributes; this row only
        # adds its device.
        scalars = {
            "metric": existing.metric,
            "name": existing.name,
            "tag": existing.tag,
            "permanent": existing.permanent,
        }
    else:
        scalars = {
            "metric": parsed["distance"] if parsed["distance"] is not None else 1,
            "name": parsed["name"],
            "tag": parsed["tag"],
            "permanent": parsed["permanent"],
        }
    desired_devices = (
        set(existing.devices.values_list("pk", flat=True)) if existing else set()
    )
    desired_devices.add(device.pk)
    values = runner._model_field_values(
        StaticRoute,
        {
            "vrf": vrf,
            "prefix": parsed["prefix"],
            "next_hop": parsed["next_hop"],
            "interface_next_hop": parsed["interface"],
            "comments": static_route_comments(row),
            **scalars,
        },
    )
    # The address fields compare as network objects, not strings: handing the
    # upsert the text form made every stored route read as changed, so every
    # sync rewrote every static route.
    for field_name in ("prefix", "next_hop"):
        if values.get(field_name) is not None:
            values[field_name] = StaticRoute._meta.get_field(field_name).to_python(
                values[field_name]
            )
    route, _ = runner._upsert_values_from_defaults(
        STATIC_ROUTE_MODEL,
        StaticRoute,
        values=values,
        coalesce_sets=[("vrf", "prefix", "next_hop", "interface_next_hop")],
        m2m_values={"devices": sorted(desired_devices)},
    )
    return route


def apply_netbox_routing_staticroute(runner, row, *, preview=False):
    route = ensure_static_route(runner, row, preview=preview)
    if preview:
        if route is False:
            return False
        return preview_leaf_outcome(runner, route)
    return route


def delete_netbox_routing_staticroute(runner, row):
    """Take the row's device off its route; delete the route only when it is
    the sync's own and no device holds it any more."""
    StaticRoute = runner._optional_model(
        "netbox_routing", "StaticRoute", STATIC_ROUTE_MODEL
    )
    try:
        parsed = parse_static_route_args(row.get("family"), row.get("args"))
    except UnreadableStaticRoute:
        return False
    try:
        device = runner._get_device_by_name(row["device"])
    except ObjectDoesNotExist:  # the device is gone; nothing to detach
        return False
    vrf = None
    if row.get("vrf"):
        from ipam.models import VRF

        vrf = VRF.objects.filter(name=row["vrf"]).first()
        if vrf is None:
            return False
    route = _existing_route(StaticRoute, vrf, parsed)
    if route is None or not route.devices.filter(pk=device.pk).exists():
        return False
    route.devices.remove(device)
    if not route.devices.exists() and STATIC_ROUTE_MARKER in (route.comments or ""):
        route.delete()
    return True


__all__ = (
    "ROUTING_STATIC_ROLLUP_REASONS",
    "STATIC_ROUTE_MARKER",
    "STATIC_ROUTE_MODEL",
    "UnreadableStaticRoute",
    "apply_netbox_routing_staticroute",
    "delete_netbox_routing_staticroute",
    "ensure_static_route",
    "parse_static_route_args",
    "static_route_comments",
)
