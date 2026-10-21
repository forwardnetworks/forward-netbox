# The device and module types that look sync-created, and what each could be.
#
# NetBox does not record who created a type. The sync creates a type with no
# component templates (no interface, port or bay templates), while one imported from
# the NetBox device-type library has them, so "no templates" is the practical mark of
# a type the sync made. It is also the mark of a hand-made type and of a few library
# types with nothing to template, so everything here is described as "looks
# sync-created" and decides nothing by itself.
#
# Of those, the ones in use by the sync's devices are split by whether a type that
# does have templates matches them under the sync's own matching rules (the same ones
# that now stop new duplicates); the ones in use by nothing anywhere are listed as
# unused. Counts and ids only: names are shown on the page behind the viewer's
# permissions, never in a payload.
from collections import Counter

from .netbox_type_matching import TypeIndex

KINDS = {
    "device": {"noun": "device type", "users": "devices"},
    "module": {"noun": "module type", "users": "modules"},
}


def _type_model(kind):
    from dcim.models import DeviceType
    from dcim.models.modules import ModuleType

    return DeviceType if kind == "device" else ModuleType


def templated_type_ids(kind):
    """Pks of the types that have at least one component template of any kind."""
    model = _type_model(kind)
    ids = set()
    for relation in model._meta.related_objects:
        if not relation.related_model.__name__.endswith("Template"):
            continue
        attname = relation.field.attname
        ids |= set(
            relation.related_model.objects.filter(**{f"{attname}__isnull": False})
            .order_by()
            .values_list(attname, flat=True)
            .distinct()
        )
    return ids


def _used_by_sync(kind, sync):
    """``{type pk: how many of this sync's devices or modules use it}``."""
    from dcim.models import Device
    from dcim.models.modules import Module

    from ..models import ForwardDeviceIdentity

    bound = ForwardDeviceIdentity.objects.filter(sync=sync).values_list(
        "device_id", flat=True
    )
    if kind == "device":
        rows = Device.objects.filter(pk__in=bound).values_list(
            "device_type_id", flat=True
        )
    else:
        rows = (
            Module.objects.filter(device_id__in=bound)
            .order_by()
            .values_list("module_type_id", flat=True)
        )
    return dict(Counter(rows))


def _used_anywhere(kind):
    from dcim.models import Device
    from dcim.models.modules import Module

    if kind == "device":
        return set(
            Device.objects.order_by()
            .values_list("device_type_id", flat=True)
            .distinct()
        )
    return set(
        Module.objects.order_by().values_list("module_type_id", flat=True).distinct()
    )


def _entries(kind, pks):
    model = _type_model(kind)
    fields = ["pk", "manufacturer__slug", "manufacturer__name", "model", "part_number"]
    if kind == "device":
        fields.append("slug")
    out = []
    for row in model.objects.filter(pk__in=pks).values_list(*fields):
        pk, m_slug, m_name, type_model, part_number = row[:5]
        out.append(
            {
                "pk": pk,
                "manufacturer_slug": m_slug,
                "manufacturer_name": m_name,
                "model": type_model,
                "slug": row[5] if kind == "device" else "",
                "part_number": part_number or "",
            }
        )
    return out


def sync_created_types(kind, sync):
    """The template-less types of ``kind``, grouped for the operator.

    Returns ``{"equivalent": [(type pk, equivalent pk, users)], "unmatched":
    [(type pk, users)], "unused": [type pk]}``. ``equivalent`` and ``unmatched`` are
    types this sync's devices (or modules) use; ``unused`` is every template-less
    type that nothing in NetBox uses.
    """
    model = _type_model(kind)
    templated = templated_type_ids(kind)
    used_here = _used_by_sync(kind, sync)
    looks_synced = {pk: n for pk, n in used_here.items() if pk not in templated}
    index = None
    if looks_synced:
        # What the sync itself would see once the old type is gone: every type
        # except the candidates being looked at. A template-less look-alike that is
        # not a candidate counts, so a suggestion that the sync would find ambiguous
        # (and answer by creating a duplicate again) is not offered.
        everything = set(model.objects.values_list("pk", flat=True))
        index = TypeIndex(_entries(kind, templated | (everything - set(looks_synced))))
    equivalent = []
    unmatched = []
    for entry in _entries(kind, set(looks_synced)):
        match = index.resolve(
            manufacturer_slug=entry["manufacturer_slug"],
            model=entry["model"],
            slug=entry["slug"],
            part_number=entry["part_number"],
        )
        users = looks_synced[entry["pk"]]
        # Only a type that has templates is a place to move devices to.
        if match is not None and match["pk"] in templated:
            equivalent.append((entry["pk"], match["pk"], users))
        else:
            unmatched.append((entry["pk"], users))
    unused = sorted(
        set(model.objects.values_list("pk", flat=True))
        - templated
        - _used_anywhere(kind)
    )
    return {
        "equivalent": sorted(equivalent),
        "unmatched": sorted(unmatched),
        "unused": unused,
    }
