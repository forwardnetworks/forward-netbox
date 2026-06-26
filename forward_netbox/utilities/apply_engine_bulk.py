from typing import Any

# Fields the bulk engines must set on CREATE but preserve on UPDATE, matching the
# adapter's intent. (2.0: platforms are global/manufacturer-less — see
# forward_platforms.nqe — so there are no create-only platform fields anymore.)
CREATE_ONLY_UPDATE_FIELDS_BY_MODEL: dict[str, frozenset] = {}

# Chunk size for synthesizing branch ObjectChanges after a bulk write: bounds the
# serialized batch + the ObjectChange list so memory does not grow with the
# model's total row count (a single shard can be ~500k rows).
EMIT_OBJECT_CHANGE_CHUNK = 1000


def _isolate_bulk_objects(
    model, objects, operation, runner, model_string, *, fields=None
):
    """Re-apply objects one at a time after a bulk write hit a constraint error.

    A failed ``bulk_create``/``bulk_update`` rolls back the whole batch, so one
    bad row would otherwise fail the entire shard. This saves each object in its
    own savepoint — good rows apply, the offending row(s) are recorded as
    ingestion issues — restoring the per-row resilience the adapter path has.
    """
    from django.db import transaction

    for obj in objects:
        try:
            with transaction.atomic():
                if operation == "create":
                    obj.save(force_insert=True)
                else:
                    obj.save(update_fields=list(fields) if fields else None)
        except Exception as exc:  # noqa: BLE001 - isolate one row, keep the shard
            runner._record_issue(
                model_string,
                f"Bulk {operation} row failed; isolated so the shard continues: {exc}",
                {"name": getattr(obj, "name", None), "pk": getattr(obj, "pk", None)},
                exception=exc,
            )


def _branch_is_active():
    """True when staging into a netbox_branching branch (vs direct-to-main)."""
    try:
        from netbox_branching.contextvars import active_branch
    except Exception:  # pragma: no cover - branching not installed
        return False
    return active_branch.get() is not None


def emit_branch_object_changes(created, updated):
    """Synthesize the core.ObjectChange rows a bulk write would otherwise skip.

    bulk_create / bulk_update fire no post_save, so NetBox records no
    ObjectChange — and the plugin merge replays a branch purely from its
    ObjectChange rows (``branch.get_unmerged_changes()``), so without this every
    bulk-staged row is silently dropped at merge. Phase 4: after the bulk write,
    synthesize each row's ObjectChange and ``bulk_create`` them. While
    ``active_branch`` is set the rows live in the branch schema, but
    core.ObjectChange is not branchable so the router would write it to main —
    target the branch connection explicitly (the framework does the same,
    branches.py:676/743) so get_unmerged_changes() sees these rows. The rows keep
    the same pks they had in the branch (it shares main's id sequence).

    ``postchange_data`` must be exactly ``serialize_object`` output (the inverse
    the merge's ``deserialize`` consumes). Rather than per-object
    ``to_objectchange`` (which runs Django's JSON serializer + a tag query per
    row — the dominant staging cost), serialize a whole chunk in ONE
    ``serialize('json', chunk)`` and prefetch the chunk's tags in one query.
    CREATE uses the freshly-saved instance (pk back-filled by bulk_create);
    UPDATE requires the caller to have ``snapshot()``-ed before mutating so
    ``prechange_data`` is correct. No-op (returns False) when not staging into a
    branch or without a request context, so FAST_BOOTSTRAP direct-to-main is
    untouched.
    """
    try:
        from netbox_branching.contextvars import active_branch
    except Exception:  # pragma: no cover - branching not installed
        return False
    branch = active_branch.get()
    if branch is None:
        return False
    from netbox.context import current_request

    request = current_request.get()
    if request is None or getattr(request, "user", None) is None:
        # Core skips ObjectChange entirely without a request/user; do not emit a
        # userless change. Caller must fall back to the adapter for tracking.
        return False
    from core.choices import ObjectChangeActionChoices
    from core.models import ObjectChange
    from django.db.models import prefetch_related_objects
    from extras.utils import is_taggable

    try:
        from netbox_branching.models import ChangeDiff
    except Exception:  # pragma: no cover - branching not installed
        ChangeDiff = None

    user = request.user
    user_name = getattr(user, "username", "") or ""
    request_id = getattr(request, "id", None)

    def _flush(objs, action):
        # Build + write ObjectChanges in chunks so the OC list never grows with
        # the model's total row count (a single shard can be ~500k rows).
        for start in range(0, len(objs), EMIT_OBJECT_CHANGE_CHUNK):
            chunk = objs[start : start + EMIT_OBJECT_CHANGE_CHUNK]
            if is_taggable(type(chunk[0])):
                # serialize_object resolves tags via obj.tags.all() (one query per
                # row); prefetch the chunk's tags in a single query so each access
                # hits the cache.
                prefetch_related_objects(chunk, "tags")
            ocs = []
            for obj in chunk:
                # to_objectchange builds postchange_data via serialize_object —
                # the exact inverse the merge's deserialize consumes.
                oc = obj.to_objectchange(action)
                oc.user = user
                oc.user_name = user_name
                oc.request_id = request_id
                ocs.append(oc)
            ObjectChange.objects.using(branch.connection_name).bulk_create(
                ocs, batch_size=EMIT_OBJECT_CHANGE_CHUNK
            )
            # bulk_create fires no post_save, so netbox_branching.record_change_diff
            # never runs and no ChangeDiff row is written. ChangeDiff(action=CREATE)
            # is the ONLY thing check_object_accessible_in_branch consults to allow a
            # later in-branch modify of a branch-created object (signal_receivers.py).
            # Without it, on a clean main every modify of a bulk-created row aborts
            # with "deleted in the main branch" (a dirty main masked this because the
            # rows already existed in main). The merge replays ObjectChange only and
            # never reads ChangeDiff, so synthesizing these is purely additive — no
            # double-apply. Only CREATE diffs are needed: an UPDATEd object either
            # exists in main (accessible) or was created in this branch (already has a
            # CREATE diff from the create pass above).
            if (
                ChangeDiff is not None
                and action == ObjectChangeActionChoices.ACTION_CREATE
            ):
                diffs = [
                    ChangeDiff(
                        branch=branch,
                        object_type_id=oc.changed_object_type_id,
                        object_id=oc.changed_object_id,
                        # object_repr is NOT NULL and only set in ChangeDiff.save(),
                        # which bulk_create bypasses — populate it explicitly.
                        object_repr=(oc.object_repr or "")[:200],
                        action=ObjectChangeActionChoices.ACTION_CREATE,
                        original=None,
                        modified=oc.postchange_data_clean or None,
                        current=None,
                        conflicts=None,
                    )
                    for oc in ocs
                ]
                # ChangeDiff is branch metadata in main (record_change_diff writes it
                # with no using()); mirror that — do NOT target the branch connection.
                ChangeDiff.objects.bulk_create(
                    diffs, batch_size=EMIT_OBJECT_CHANGE_CHUNK
                )

    created = list(created)
    updated = list(updated)
    if created:
        _flush(created, ObjectChangeActionChoices.ACTION_CREATE)
    if updated:
        _flush(updated, ObjectChangeActionChoices.ACTION_UPDATE)
    return True


def bulk_orm_apply_simple_models(runner, model_string: str, rows: list[dict[str, Any]]):
    from django.db import transaction
    from django.db.models import Q

    if model_string == "dcim.macaddress":
        return bulk_orm_apply_macaddress(runner, rows)
    if model_string == "dcim.virtualchassis":
        return bulk_orm_apply_virtualchassis(runner, rows)
    if model_string == "ipam.ipaddress":
        return bulk_orm_apply_ipaddress(runner, rows)
    if model_string == "dcim.interface":
        return bulk_orm_apply_interface(runner, rows)
    if model_string == "dcim.device":
        return bulk_orm_apply_device(runner, rows)

    from dcim.models import DeviceType
    from dcim.models import DeviceRole
    from dcim.models import Manufacturer
    from dcim.models import Platform
    from dcim.models import Site
    from ipam.models import VLAN
    from ipam.models import Prefix
    from ipam.models import VRF

    from .sync_primitives import _model_field_value_matches

    create_only_fields = CREATE_ONLY_UPDATE_FIELDS_BY_MODEL.get(
        model_string, frozenset()
    )

    specs = {
        "dcim.site": {
            "model": Site,
            "required": ("name", "slug"),
            "fields": ("name", "slug"),
            "lookup_fields": ("slug", "name"),
            "lookup_sets": (("slug",), ("name",)),
        },
        "dcim.manufacturer": {
            "model": Manufacturer,
            "required": ("name", "slug"),
            "fields": ("name", "slug"),
            "lookup_fields": ("slug", "name"),
            "lookup_sets": (("slug",), ("name",)),
        },
        "dcim.devicerole": {
            "model": DeviceRole,
            "required": ("name", "slug"),
            "fields": ("name", "slug", "color"),
            "lookup_fields": ("slug", "name"),
            "lookup_sets": (("slug",), ("name",)),
        },
        "dcim.platform": {
            "model": Platform,
            "required": ("name", "slug"),
            "fields": ("name", "slug", "manufacturer"),
            "lookup_fields": ("slug", "name"),
            "lookup_sets": (("slug",), ("name",)),
        },
        "dcim.devicetype": {
            "model": DeviceType,
            "required": ("manufacturer", "model", "slug"),
            "fields": ("manufacturer", "model", "slug"),
            "lookup_fields": ("slug", "model"),
            "lookup_sets": (("slug",), ("manufacturer", "model")),
        },
        "ipam.vlan": {
            "model": VLAN,
            "required": ("vid", "name", "status"),
            "fields": ("site", "vid", "name", "status"),
            "lookup_fields": ("site", "vid"),
            "lookup_sets": (("site", "vid"),),
        },
        "ipam.vrf": {
            "model": VRF,
            "required": ("name",),
            "fields": ("name", "rd", "description", "enforce_unique"),
            "lookup_fields": ("rd", "name"),
            "lookup_sets": (("rd",), ("name",)),
        },
        "ipam.prefix": {
            "model": Prefix,
            "required": ("prefix", "status"),
            "fields": ("prefix", "vrf", "status"),
            "lookup_fields": ("prefix",),
            "lookup_sets": (("prefix", "vrf"),),
            # vrf is part of the prefix identity but legitimately null (global
            # table). It must encode as a sentinel in the lookup key, not bail to
            # None (which would never match an existing null-VRF prefix and create
            # a duplicate).
            "nullable_lookup_fields": ("vrf",),
        },
    }
    spec = specs.get(model_string)
    if not spec:
        return False

    model = spec["model"]
    fields = tuple(spec["fields"])
    required = tuple(spec["required"])
    lookup_fields = tuple(spec["lookup_fields"])
    lookup_sets = tuple(tuple(lookup_set) for lookup_set in spec["lookup_sets"])
    nullable_lookup_fields = tuple(spec.get("nullable_lookup_fields", ()))

    site_by_slug = {}
    site_by_name = {}
    manufacturer_by_slug = {}
    manufacturer_by_name = {}
    if model_string == "ipam.vlan":
        site_rows = [
            {"name": row.get("site"), "slug": row.get("site_slug") or row.get("site")}
            for row in rows
            if row.get("site")
        ]
        bulk_orm_apply_simple_models(runner, "dcim.site", site_rows)
        site_values = {
            value
            for row in site_rows
            for value in (row.get("slug"), row.get("name"))
            if value not in ("", None)
        }
        sites = Site.objects.filter(Q(slug__in=site_values) | Q(name__in=site_values))
        site_by_slug = {site.slug: site for site in sites if site.slug}
        site_by_name = {site.name: site for site in sites if site.name}
    if model_string == "dcim.devicetype":
        manufacturer_rows = [
            {
                "name": row.get("manufacturer"),
                "slug": row.get("manufacturer_slug") or row.get("manufacturer"),
            }
            for row in rows
            if row.get("manufacturer")
        ]
        bulk_orm_apply_simple_models(runner, "dcim.manufacturer", manufacturer_rows)
        manufacturer_values = {
            value
            for row in manufacturer_rows
            for value in (row.get("slug"), row.get("name"))
            if value not in ("", None)
        }
        manufacturers = Manufacturer.objects.filter(
            Q(slug__in=manufacturer_values) | Q(name__in=manufacturer_values)
        )
        manufacturer_by_slug = {
            manufacturer.slug: manufacturer
            for manufacturer in manufacturers
            if manufacturer.slug
        }
        manufacturer_by_name = {
            manufacturer.name: manufacturer
            for manufacturer in manufacturers
            if manufacturer.name
        }
    if model_string == "ipam.prefix":
        requested_vrf_names = {
            row.get("vrf") for row in rows if row.get("vrf") not in ("", None)
        }
        existing_vrf_names = set(
            VRF.objects.filter(name__in=requested_vrf_names).values_list(
                "name", flat=True
            )
        )
        # Create only VRFs that do not exist yet. Upserting here would rewrite an
        # existing VRF's rd/description/enforce_unique with these empty defaults,
        # clobbering values set by the ipam.vrf map.
        missing_vrf_rows = [
            {"name": name, "rd": None, "description": "", "enforce_unique": False}
            for name in sorted(requested_vrf_names - existing_vrf_names)
        ]
        if missing_vrf_rows:
            bulk_orm_apply_simple_models(runner, "ipam.vrf", missing_vrf_rows)
        vrf_by_name = {
            vrf.name: vrf
            for vrf in VRF.objects.filter(name__in=requested_vrf_names)
            if vrf.name
        }

    lookup_values = {field_name: [] for field_name in lookup_fields}
    normalized_rows: list[dict[str, Any]] = []
    for row in rows:
        if any(not row.get(field_name) for field_name in required):
            runner._record_issue(
                model_string,
                "Bulk ORM row missing required identity fields.",
                row,
                context={"required": required},
            )
            runner.logger.increment_statistics(model_string, outcome="failed")
            continue
        normalized = {field_name: row.get(field_name) for field_name in fields}
        if model_string == "dcim.devicerole" and not normalized.get("color"):
            normalized["color"] = "9e9e9e"
        if model_string == "dcim.platform":
            # 2.0: platforms are global. Force manufacturer=None on create AND
            # update so a manufacturer-scoped platform (e.g. a legacy UNKNOWN tied
            # to one vendor) is cleared and any vendor's device can attach.
            normalized["manufacturer"] = None
        if model_string == "dcim.devicetype":
            manufacturer = None
            if row.get("manufacturer"):
                manufacturer = manufacturer_by_slug.get(
                    row.get("manufacturer_slug")
                ) or manufacturer_by_name.get(row.get("manufacturer"))
            normalized["manufacturer"] = manufacturer
            if not normalized.get("slug"):
                normalized["slug"] = normalized.get("model")
        if model_string == "ipam.vlan":
            site = None
            if row.get("site"):
                site = site_by_slug.get(row.get("site_slug")) or site_by_name.get(
                    row.get("site")
                )
            normalized["site"] = site
            normalized["vid"] = int(normalized["vid"])
        if model_string == "ipam.vrf":
            normalized["rd"] = normalized.get("rd") or None
            normalized["description"] = normalized.get("description") or ""
            normalized["enforce_unique"] = bool(normalized.get("enforce_unique"))
        if model_string == "ipam.prefix":
            normalized["vrf"] = (
                vrf_by_name.get(row.get("vrf")) if row.get("vrf") else None
            )
        normalized_rows.append(normalized)
        for field_name in lookup_fields:
            value = normalized.get(field_name)
            if value not in ("", None):
                lookup_values[field_name].append(value)

    if not normalized_rows:
        return True

    if model_string in {"dcim.devicerole", "dcim.platform", "ipam.prefix"}:
        # Per-object save path. ipam.prefix relies on this so NetBox's post_save
        # hierarchy signal recomputes `_depth`/`_children` (bulk_create would skip
        # it), keeping parity with the adapter.
        return bulk_orm_apply_tree_models(
            runner=runner,
            model_string=model_string,
            model=model,
            fields=fields,
            lookup_sets=lookup_sets,
            normalized_rows=normalized_rows,
            nullable_lookup_fields=nullable_lookup_fields,
        )

    existing_qs = model.objects.none()
    if any(lookup_values.values()):
        query = Q()
        for field_name, values in lookup_values.items():
            if values:
                query |= Q(**{f"{field_name}__in": values})
        existing_qs = model.objects.filter(query)

    existing_by_lookup = {lookup_set: {} for lookup_set in lookup_sets}
    for obj in existing_qs:
        for lookup_set in lookup_sets:
            key = lookup_key_from_object(
                obj,
                lookup_set,
                model_string=model_string,
                nullable_fields=nullable_lookup_fields,
            )
            if key is not None:
                existing_by_lookup[lookup_set][key] = obj

    create_objects = []
    update_objects = []
    branch_active = _branch_is_active()
    for values in normalized_rows:
        existing = None
        for lookup_set in lookup_sets:
            key = lookup_key_from_values(
                values,
                lookup_set,
                model_string=model_string,
                nullable_fields=nullable_lookup_fields,
            )
            if key is None:
                continue
            existing = existing_by_lookup[lookup_set].get(key)
            if existing is not None:
                break
        if existing is None:
            obj = model(**values)
            # The existing-object lookup above already proved this identity is
            # absent, so validate_unique/validate_constraints (a DB query per row)
            # is redundant; a genuine constraint violation still surfaces via the
            # bulk_create IntegrityError -> per-row isolate path. Field validation
            # is kept.
            obj.full_clean(validate_unique=False, validate_constraints=False)
            create_objects.append(obj)
            for lookup_set in lookup_sets:
                key = lookup_key_from_values(
                    values,
                    lookup_set,
                    model_string=model_string,
                    nullable_fields=nullable_lookup_fields,
                )
                if key is not None:
                    existing_by_lookup[lookup_set][key] = obj
            runner.logger.increment_statistics(model_string, outcome="applied")
            runner.events_clearer.increment()
            continue
        if branch_active and getattr(existing, "pk", None) is not None:
            # Capture pre-mutation state so a synthesized UPDATE ObjectChange (or
            # the per-object isolate fallback) gets correct prechange_data.
            existing.snapshot()
        changed = False
        for field_name in fields:
            if field_name in create_only_fields:
                continue
            incoming = values.get(field_name)
            # Use the adapter's value matcher: it compares relations by id and
            # special-cases typed fields (e.g. ipam.prefix IPNetwork vs string),
            # so a re-applied row does not churn just because the stored type
            # differs from the incoming string.
            if not _model_field_value_matches(model, existing, field_name, incoming):
                setattr(existing, field_name, incoming)
                changed = True
        if changed and getattr(existing, "pk", None) is not None:
            # Existing objects already satisfy DB constraints; skip
            # validate_unique/validate_constraints (both issue DB queries).
            existing.clean_fields()
            existing.clean()
            update_objects.append(existing)
            runner.logger.increment_statistics(model_string, outcome="applied")
            runner.events_clearer.increment()
            continue
        runner.logger.increment_statistics(model_string, outcome="unchanged")

    from django.db import IntegrityError

    try:
        with transaction.atomic():
            if create_objects:
                model.objects.bulk_create(create_objects, batch_size=1000)
            if update_objects:
                model.objects.bulk_update(
                    update_objects,
                    fields=list(fields),
                    batch_size=1000,
                )
    except IntegrityError as exc:
        # A DB constraint violation rolled the whole batch back. Isolate per row
        # so a single bad row does not fail the entire shard; the offending
        # row(s) become ingestion issues for operator review.
        runner.logger.log_warning(
            f"Bulk write for {model_string} hit a constraint error ({exc}); "
            "retrying the batch row-by-row to isolate the offending row(s).",
            obj=runner.sync,
        )
        # Per-object isolate saves fire post_save, so when staging into a branch
        # they record their own ObjectChanges (CREATE, and UPDATE via the
        # snapshot taken above) — no synthesized emission needed on this path.
        _isolate_bulk_objects(model, create_objects, "create", runner, model_string)
        _isolate_bulk_objects(
            model, update_objects, "update", runner, model_string, fields=fields
        )
    else:
        # Bulk write succeeded and recorded no post_save, so synthesize the
        # branch ObjectChanges the merge replays from (no-op direct-to-main).
        if branch_active:
            try:
                emit_branch_object_changes(create_objects, update_objects)
            except Exception as exc:  # noqa: BLE001 - surface, do not crash shard
                runner._record_issue(
                    model_string,
                    f"Bulk write staged into the branch but its change log could "
                    f"not be recorded; these rows would not merge: {exc}",
                    {},
                    exception=exc,
                )
    runner.events_clearer.clear()
    return True


def _canonical_mac(value):
    """NetBox-canonical (upper, colon-expanded) form of a MAC for stable matching.

    Forward emits ``toString(macAddress)`` — often lowercase (``6c:4e:..``) or
    cisco-dot (``6c4e.f637.6380``) — while NetBox normalizes the stored value to
    the upper colon-expanded form (``6C:4E:F6:37:63:80``). Matching on the raw
    Forward string misses the existing row, and since MACAddress has no uniqueness
    constraint, every sync re-creates the same MAC (silent duplication + branch
    churn). Normalize both the existing-index key and the per-row lookup key.
    """
    from netaddr import EUI
    from netaddr import mac_unix_expanded

    try:
        eui = EUI(str(value))
        eui.dialect = mac_unix_expanded
        return str(eui).upper()
    except Exception:  # noqa: BLE001 - fall back to a stable normalized string
        return str(value).strip().upper()


def bulk_orm_apply_macaddress(runner, rows: list[dict[str, Any]]):
    from dcim.models import Device
    from dcim.models import Interface
    from dcim.models import MACAddress
    from django.db import transaction

    from ..exceptions import ForwardDependencySkipError
    from ..exceptions import ForwardSearchError
    from ..exceptions import ForwardSyncDataError

    interface_content_type = runner._content_type_for(Interface)
    device_names = {
        row.get("device") for row in rows if row.get("device") not in ("", None)
    }
    interface_names = {
        row.get("interface") for row in rows if row.get("interface") not in ("", None)
    }
    mac_values = {
        row.get("mac") or row.get("mac_address")
        for row in rows
        if (row.get("mac") or row.get("mac_address")) not in ("", None)
    }

    devices_by_name = {
        device.name: device for device in Device.objects.filter(name__in=device_names)
    }
    interfaces_by_key = {
        (interface.device.name, interface.name): interface
        for interface in Interface.objects.select_related("device").filter(
            device__name__in=device_names,
            name__in=interface_names,
        )
    }
    macs_by_address = {
        _canonical_mac(mac.mac_address): mac
        for mac in MACAddress.objects.filter(mac_address__in=mac_values)
    }

    create_objects = {}
    update_objects = {}
    branch_active = _branch_is_active()
    for row in rows:
        device_name = row.get("device")
        interface_name = row.get("interface")
        mac_address = row.get("mac") or row.get("mac_address")
        if not device_name or not interface_name or not mac_address:
            exc = ForwardSyncDataError(
                "MAC address row is missing required device, interface, or mac identity.",
                model_string="dcim.macaddress",
                context={
                    "required": ("device", "interface", "mac"),
                    "device": device_name,
                    "interface": interface_name,
                },
                data=row,
            )
            runner._mark_dependency_failed("dcim.macaddress", row)
            runner.logger.increment_statistics("dcim.macaddress", outcome="failed")
            runner._record_issue(
                "dcim.macaddress",
                str(exc),
                row,
                exception=exc,
                context=exc.context,
                defaults=exc.defaults,
            )
            continue

        device = devices_by_name.get(device_name)
        if device is None:
            key = (device_name,)
            if runner._dependency_failed("dcim.device", key):
                exc = ForwardDependencySkipError(
                    "Skipping MAC assignment because dependency "
                    f"`dcim.device` failed for {key}.",
                    model_string="dcim.macaddress",
                    context={"device": device_name, "interface": interface_name},
                    data=row,
                )
                runner.logger.increment_statistics(
                    "dcim.macaddress",
                    outcome="skipped",
                )
                runner._record_issue(
                    "dcim.macaddress",
                    str(exc),
                    row,
                    exception=exc,
                    context=exc.context,
                    defaults=exc.defaults,
                )
                continue
            exc = ForwardSearchError(
                f"Unable to find device `{device_name}` for MAC assignment.",
                model_string="dcim.macaddress",
                context={"device": device_name, "interface": interface_name},
                data=row,
            )
            runner._mark_dependency_failed("dcim.macaddress", row)
            runner.logger.increment_statistics("dcim.macaddress", outcome="failed")
            runner._record_issue(
                "dcim.macaddress",
                str(exc),
                row,
                exception=exc,
                context=exc.context,
                defaults=exc.defaults,
            )
            continue

        interface = interfaces_by_key.get(
            (device.name, interface_name)
        ) or runner._lookup_interface(device, interface_name)
        if interface is None:
            key = (device.name, interface_name)
            if runner._dependency_failed("dcim.interface", key):
                exc = ForwardDependencySkipError(
                    "Skipping MAC assignment because dependency "
                    f"`dcim.interface` failed for {key}.",
                    model_string="dcim.macaddress",
                    context={"device": device.name, "interface": interface_name},
                    data=row,
                )
                runner.logger.increment_statistics(
                    "dcim.macaddress",
                    outcome="skipped",
                )
                runner._record_issue(
                    "dcim.macaddress",
                    str(exc),
                    row,
                    exception=exc,
                    context=exc.context,
                    defaults=exc.defaults,
                )
                continue
            # A MAC whose target interface was not imported is benign — the same
            # condition the IP path treats as an aggregated skip, not a hard
            # ForwardSearchError "failed" issue. Keep it consistent so a MAC that
            # cannot attach does not paint the model red. (The deeper "why was the
            # interface not imported" is surfaced by the dcim.interface sync, not
            # here.)
            runner._record_aggregated_skip_warning(
                model_string="dcim.macaddress",
                reason="missing-interface",
                warning_message=(
                    f"Skipping MAC address `{row.get('mac')}` on `{device.name}` "
                    f"`{interface_name}` because the target interface was not "
                    "imported."
                ),
            )
            continue

        mac_key = _canonical_mac(mac_address)
        mac = macs_by_address.get(mac_key)
        if mac is None:
            mac = create_objects.get(mac_key)
        if mac is None:
            mac = MACAddress(
                mac_address=mac_address,
                assigned_object_type=interface_content_type,
                assigned_object_id=interface.pk,
            )
            # Identity proven absent above; skip the per-row validate_unique DB
            # query (constraint violations still surface via isolate).
            mac.full_clean(validate_unique=False, validate_constraints=False)
            create_objects[mac_key] = mac
            macs_by_address[mac_key] = mac
            runner.logger.increment_statistics("dcim.macaddress", outcome="applied")
            runner.events_clearer.increment()
            continue

        # Existing MAC: only write when the interface assignment actually
        # changes, otherwise every sync re-PATCHes unchanged rows.
        if (
            mac.assigned_object_type_id == interface_content_type.pk
            and mac.assigned_object_id == interface.pk
        ):
            runner.logger.increment_statistics("dcim.macaddress", outcome="unchanged")
            continue
        # `mac` may be a not-yet-saved in-memory create from an earlier row with
        # the same canonical MAC (duplicate MAC across interfaces/devices — only
        # surfaces at full-network scale); snapshot() touches tags and raises on
        # an unsaved object, so only snapshot already-persisted rows.
        if branch_active and getattr(mac, "pk", None) is not None:
            mac.snapshot()
        mac.assigned_object_type = interface_content_type
        mac.assigned_object_id = interface.pk
        mac.full_clean()
        if getattr(mac, "pk", None):
            update_objects[mac.pk] = mac
        runner.logger.increment_statistics("dcim.macaddress", outcome="applied")
        runner.events_clearer.increment()

    from django.db import IntegrityError

    mac_update_fields = ["assigned_object_type", "assigned_object_id"]
    try:
        with transaction.atomic():
            if create_objects:
                MACAddress.objects.bulk_create(
                    list(create_objects.values()),
                    batch_size=1000,
                )
            if update_objects:
                MACAddress.objects.bulk_update(
                    list(update_objects.values()),
                    fields=mac_update_fields,
                    batch_size=1000,
                )
    except IntegrityError as exc:
        runner.logger.log_warning(
            f"Bulk write for dcim.macaddress hit a constraint error ({exc}); "
            "retrying row-by-row to isolate the offending row(s).",
            obj=runner.sync,
        )
        _isolate_bulk_objects(
            MACAddress,
            list(create_objects.values()),
            "create",
            runner,
            "dcim.macaddress",
        )
        _isolate_bulk_objects(
            MACAddress,
            list(update_objects.values()),
            "update",
            runner,
            "dcim.macaddress",
            fields=mac_update_fields,
        )
    else:
        if branch_active:
            emit_branch_object_changes(
                list(create_objects.values()), list(update_objects.values())
            )

    runner.events_clearer.clear()
    return True


def _interface_field_differs(existing, field, value) -> bool:
    """Return True if ``value`` differs from ``existing``'s stored ``field``.

    Relations are compared by id (``<field>_id``) so an incoming related
    instance does not trigger a lazy DB fetch just to compare equality.
    """
    field_obj = existing._meta.get_field(field)
    if field_obj.is_relation:
        incoming_id = value.pk if value is not None else None
        return getattr(existing, f"{field}_id") != incoming_id
    return getattr(existing, field) != value


def bulk_orm_apply_interface(runner, rows: list[dict[str, Any]]):
    """Batched apply for dcim.interface (experimental, opt-in).

    Plain interfaces are resolved with adapter-parity semantics (device skip/
    fail, optional mtu/speed/description, access/tagged mode + untagged VLAN) and
    written with bulk_create/bulk_update. Rows with LAG interdependencies — LAG
    membership (``lag``) or converting an interface to type ``lag`` while it
    still has a cable — are delegated row-by-row to the adapter
    ``apply_dcim_interface`` so their parent-ordering and cable side-effects keep
    exact parity. Existing rows load from the DB, so fields absent from a row are
    written back unchanged (no clearing), matching the adapter upsert.
    """
    from dcim.models import Device
    from dcim.models import Interface
    from django.core.exceptions import ObjectDoesNotExist
    from django.db import transaction

    from ..exceptions import ForwardDependencySkipError
    from ..exceptions import ForwardSearchError
    from .interface_naming import canonical_interface_key
    from .sync_interface import _interface_untagged_vlan
    from .sync_interface import apply_dcim_interface

    update_field_names = [
        "type",
        "enabled",
        "mtu",
        "speed",
        "description",
        "mode",
        "untagged_vlan",
    ]

    device_names = {row.get("device") for row in rows if row.get("device")}
    devices_by_name = {
        device.name: device for device in Device.objects.filter(name__in=device_names)
    }

    create_objects = {}
    update_objects = {}
    # Pending bulk creates indexed by canonical (device.pk, (type, number)) so a
    # second row whose name canonicalizes to one already staged (e.g. Forward
    # emits both ``po9`` and ``Port-channel9``) collapses onto the first instead
    # of staging a duplicate the lookup can't yet see (it isn't committed).
    pending_canonical = {}
    # LAG-interdependent rows (members or lag-type-with-cable) are deferred until
    # AFTER the bulk write commits. A plain LAG parent (``po9``) is staged in
    # create_objects (uncommitted); delegating a member that references it inline
    # would have the adapter independently create the parent it can't see in the
    # pending dict, so the trailing bulk_create then hits a duplicate-key error.
    # Deferring guarantees the parent is committed (and cached) before members
    # are applied.
    adapter_rows = []

    def _delegate_to_adapter(row):
        # Hard LAG/cable cases keep exact adapter semantics (parent ensure,
        # cable removal). Mirror the runner's row-error handling.
        try:
            apply_dcim_interface(runner, row)
            runner.logger.increment_statistics("dcim.interface", outcome="applied")
        except ForwardDependencySkipError as exc:
            runner.logger.increment_statistics("dcim.interface", outcome="skipped")
            runner._record_issue(
                "dcim.interface",
                str(exc),
                row,
                exception=exc,
                context=getattr(exc, "context", None),
                defaults=getattr(exc, "defaults", None),
            )
        except (ForwardSearchError, Exception) as exc:  # noqa: BLE001
            runner._mark_dependency_failed("dcim.interface", row)
            runner.logger.increment_statistics("dcim.interface", outcome="failed")
            runner._record_issue(
                "dcim.interface",
                str(exc),
                row,
                exception=exc,
                context=getattr(exc, "context", None),
                defaults=getattr(exc, "defaults", None),
            )

    branch_active = _branch_is_active()
    for row in rows:
        device_name = row.get("device")
        try:
            device = devices_by_name.get(device_name) or runner._get_device_by_name(
                device_name
            )
        except ObjectDoesNotExist:
            key = (device_name,)
            if runner._dependency_failed("dcim.device", key):
                runner.logger.increment_statistics("dcim.interface", outcome="skipped")
                runner._record_issue(
                    "dcim.interface",
                    f"Skipping interface `{row.get('name')}` because dependency "
                    f"`dcim.device` failed for {key}.",
                    row,
                    exception=ForwardDependencySkipError(
                        "dependency dcim.device failed",
                        model_string="dcim.interface",
                    ),
                )
                continue
            runner._mark_dependency_failed("dcim.interface", row)
            runner.logger.increment_statistics("dcim.interface", outcome="failed")
            runner._record_issue(
                "dcim.interface",
                f"Unable to find device `{device_name}` for interface "
                f"`{row.get('name')}`.",
                row,
                exception=ForwardSearchError(
                    f"device {device_name} not found",
                    model_string="dcim.interface",
                ),
            )
            continue

        existing = runner._lookup_interface(device, row["name"])

        # Delegate LAG-interdependent rows to the adapter for exact parity.
        needs_adapter = bool(row.get("lag")) or (
            row.get("type") == "lag"
            and existing is not None
            and getattr(existing, "cable", None) is not None
        )
        if needs_adapter:
            adapter_rows.append(row)
            continue

        defaults = {
            "device": device,
            "name": row["name"],
            "type": row["type"],
            "enabled": row["enabled"],
        }
        if row.get("mtu") not in ("", None):
            defaults["mtu"] = row["mtu"]
        if row.get("speed") not in ("", None):
            defaults["speed"] = row["speed"]
        if row.get("description") not in (None, ""):
            defaults["description"] = row["description"]
        if row.get("mode") in {"access", "tagged"}:
            defaults["mode"] = row["mode"]
            found_vlan, vlan = _interface_untagged_vlan(runner, device, row)
            if found_vlan:
                defaults["untagged_vlan"] = vlan

        if existing is None:
            key = (device.pk, row["name"])
            canon = canonical_interface_key(row["name"])
            canon_key = (device.pk, canon) if canon is not None else None
            interface = create_objects.get(key)
            if interface is None and canon_key is not None:
                # A differently-spelled but canonically-equal name already staged
                # for this device (po9 vs Port-channel9): reuse it, don't dup.
                interface = pending_canonical.get(canon_key)
            if interface is None:
                interface = Interface(**defaults)
                # Identity proven absent above; skip the per-row validate_unique
                # DB query (constraint violations still surface via isolate).
                interface.full_clean(validate_unique=False, validate_constraints=False)
                create_objects[key] = interface
                if canon_key is not None:
                    pending_canonical[canon_key] = interface
            runner.logger.increment_statistics("dcim.interface", outcome="applied")
            runner.events_clearer.increment()
            continue

        # Existing interface: only write when a field actually changes,
        # otherwise every sync re-PATCHes unchanged interfaces.
        if branch_active:
            existing.snapshot()
        changed = False
        for field, value in defaults.items():
            if _interface_field_differs(existing, field, value):
                setattr(existing, field, value)
                changed = True
        if not changed:
            runner.logger.increment_statistics("dcim.interface", outcome="unchanged")
            continue
        existing.full_clean()
        if getattr(existing, "pk", None):
            update_objects[existing.pk] = existing
        runner.logger.increment_statistics("dcim.interface", outcome="applied")
        runner.events_clearer.increment()

    from django.db import IntegrityError

    try:
        with transaction.atomic():
            if create_objects:
                Interface.objects.bulk_create(
                    list(create_objects.values()), batch_size=1000
                )
            if update_objects:
                Interface.objects.bulk_update(
                    list(update_objects.values()),
                    fields=update_field_names,
                    batch_size=1000,
                )
    except IntegrityError as exc:
        runner.logger.log_warning(
            f"Bulk write for dcim.interface hit a constraint error ({exc}); "
            "retrying row-by-row to isolate the offending row(s).",
            obj=runner.sync,
        )
        _isolate_bulk_objects(
            Interface, list(create_objects.values()), "create", runner, "dcim.interface"
        )
        _isolate_bulk_objects(
            Interface,
            list(update_objects.values()),
            "update",
            runner,
            "dcim.interface",
            fields=update_field_names,
        )
    else:
        if branch_active:
            emit_branch_object_changes(
                list(create_objects.values()), list(update_objects.values())
            )

    # Reflect freshly-created interfaces in the runner lookup caches so the
    # deferred LAG rows (and any later same-run lookup) resolve them instead of
    # re-creating them off a stale negative-cache entry. Clear every negative for
    # a device that just gained an interface (cheap; negatives are only a hint)
    # so a canonically-equivalent spelling can no longer short-circuit to None.
    created = [iface for iface in create_objects.values() if getattr(iface, "pk", None)]
    if created:
        created_device_pks = {iface.device_id for iface in created}
        runner._missing_interface_by_device_name_cache = {
            miss_key
            for miss_key in runner._missing_interface_by_device_name_cache
            if miss_key[0] not in created_device_pks
        }
        for iface in created:
            dev_pk = iface.device_id
            runner._interface_by_device_name_cache[(dev_pk, iface.name)] = iface
            canon = canonical_interface_key(iface.name)
            if canon is not None:
                device_map = runner._interface_canonical_cache.get(dev_pk)
                if device_map is not None:
                    device_map.setdefault(canon, iface)

    # Apply deferred LAG-interdependent rows now that their parent interfaces are
    # committed and cached (parent-first ordering, no duplicate-key race).
    for row in adapter_rows:
        _delegate_to_adapter(row)

    runner.events_clearer.clear()
    return True


def _device_field_differs(existing, field, value):
    if field in {"site", "role", "device_type", "platform"}:
        return getattr(existing, f"{field}_id") != (value.pk if value else None)
    return getattr(existing, field) != value


def bulk_orm_apply_device(runner, rows: list[dict[str, Any]]):
    """Batched apply for dcim.device with adapter-parity semantics.

    The clean common case — a device whose site/role/device-type (and optional
    platform) are already staged by their own workloads immediately before
    dcim.device, with no virtual-chassis membership — is resolved by batch FK
    lookup and written with bulk_create/bulk_update + branch ObjectChange
    synthesis. Rows that need adapter sequencing (a missing parent the adapter
    would create on demand, virtual-chassis membership, or the opt-in
    ``apply_device_scope_tags`` per-device tagging) delegate row-by-row to
    ``apply_dcim_device`` so their behavior stays byte-for-byte identical.
    Existing devices are written only when a field actually changes.
    """
    from dcim.models import Device
    from dcim.models import DeviceRole
    from dcim.models import DeviceType
    from dcim.models import Platform
    from dcim.models import Site
    from django.db import IntegrityError
    from django.db import transaction
    from django.db.models import Q

    from ..exceptions import ForwardDependencySkipError
    from ..exceptions import ForwardSearchError
    from .sync_device import _scope_tags_enabled
    from .sync_device import apply_dcim_device

    update_field_names = ["site", "role", "device_type", "platform", "serial", "status"]

    def _delegate(row):
        try:
            apply_dcim_device(runner, row)
            runner.logger.increment_statistics("dcim.device", outcome="applied")
        except ForwardDependencySkipError as exc:
            runner.logger.increment_statistics("dcim.device", outcome="skipped")
            runner._record_issue(
                "dcim.device",
                str(exc),
                row,
                exception=exc,
                context=getattr(exc, "context", None),
                defaults=getattr(exc, "defaults", None),
            )
        except (ForwardSearchError, Exception) as exc:  # noqa: BLE001
            runner._mark_dependency_failed("dcim.device", row)
            runner.logger.increment_statistics("dcim.device", outcome="failed")
            runner._record_issue(
                "dcim.device",
                str(exc),
                row,
                exception=exc,
                context=getattr(exc, "context", None),
                defaults=getattr(exc, "defaults", None),
            )

    # Opt-in per-device scope tagging is an adapter side effect; keep exact parity
    # by delegating the whole batch when it is enabled.
    if _scope_tags_enabled(runner):
        for row in rows:
            _delegate(row)
        runner.events_clearer.clear()
        return True

    def _index(model, slug_values, name_values):
        objs = model.objects.filter(Q(slug__in=slug_values) | Q(name__in=name_values))
        by_slug, by_name = {}, {}
        for obj in objs:
            if obj.slug:
                by_slug[obj.slug] = obj
            if obj.name:
                by_name[obj.name] = obj
        return by_slug, by_name

    site_slug_v = {r.get("site_slug") for r in rows if r.get("site_slug")}
    site_name_v = {r.get("site") for r in rows if r.get("site")}
    sites_by_slug, sites_by_name = _index(Site, site_slug_v, site_name_v)
    role_slug_v = {r.get("role_slug") for r in rows if r.get("role_slug")}
    role_name_v = {r.get("role") for r in rows if r.get("role")}
    roles_by_slug, roles_by_name = _index(DeviceRole, role_slug_v, role_name_v)
    plat_slug_v = {r.get("platform_slug") for r in rows if r.get("platform_slug")}
    plat_name_v = {r.get("platform") for r in rows if r.get("platform")}
    plats_by_slug, plats_by_name = _index(Platform, plat_slug_v, plat_name_v)
    # DeviceType has no `name`; it is keyed by slug and `model`.
    dt_slug_v = {r.get("device_type_slug") for r in rows if r.get("device_type_slug")}
    dt_model_v = {r.get("device_type") for r in rows if r.get("device_type")}
    dts = DeviceType.objects.filter(Q(slug__in=dt_slug_v) | Q(model__in=dt_model_v))
    dt_by_slug = {dt.slug: dt for dt in dts if dt.slug}
    dt_by_model = {dt.model: dt for dt in dts if dt.model}

    existing_by_name = {
        d.name: d
        for d in Device.objects.filter(
            name__in={r["name"] for r in rows if r.get("name")}
        )
    }

    create_objects = {}
    update_objects = {}
    branch_active = _branch_is_active()

    for row in rows:
        if not row.get("name"):
            runner.logger.increment_statistics("dcim.device", outcome="failed")
            runner._record_issue(
                "dcim.device", "Bulk ORM device row missing `name`.", row
            )
            continue
        site = sites_by_slug.get(row.get("site_slug")) or sites_by_name.get(
            row.get("site")
        )
        role = roles_by_slug.get(row.get("role_slug")) or roles_by_name.get(
            row.get("role")
        )
        device_type = dt_by_slug.get(row.get("device_type_slug")) or dt_by_model.get(
            row.get("device_type")
        )
        platform = None
        wants_platform = bool(row.get("platform"))
        if wants_platform:
            platform = plats_by_slug.get(row.get("platform_slug")) or plats_by_name.get(
                row.get("platform")
            )
        # Anything needing adapter sequencing keeps exact parity via the adapter:
        # a parent not pre-staged (adapter creates it) or virtual-chassis rows.
        if (
            site is None
            or role is None
            or device_type is None
            or (wants_platform and platform is None)
            or row.get("virtual_chassis")
        ):
            _delegate(row)
            continue

        defaults = {
            "name": row["name"],
            "site": site,
            "role": role,
            "device_type": device_type,
            "platform": platform,
            "serial": row.get("serial", ""),
            "status": row["status"],
        }
        try:
            existing = existing_by_name.get(row["name"])
            if existing is None:
                device = create_objects.get(row["name"])
                if device is None:
                    device = Device(**defaults)
                    device.full_clean(validate_unique=False, validate_constraints=False)
                    create_objects[row["name"]] = device
                runner.logger.increment_statistics("dcim.device", outcome="applied")
                runner.events_clearer.increment()
                continue
            if branch_active:
                existing.snapshot()
            changed = False
            for field, value in defaults.items():
                if _device_field_differs(existing, field, value):
                    setattr(existing, field, value)
                    changed = True
            if not changed:
                runner.logger.increment_statistics("dcim.device", outcome="unchanged")
                continue
            existing.full_clean()
            if getattr(existing, "pk", None):
                update_objects[existing.pk] = existing
            runner.logger.increment_statistics("dcim.device", outcome="applied")
            runner.events_clearer.increment()
        except Exception as exc:  # noqa: BLE001 - isolate one device, keep staging
            # e.g. a device deleted in main while the branch modifies it, or any
            # per-row validation conflict: record the issue + mark the dependency
            # failed so child interfaces/IPs skip cleanly, and keep the sync going.
            runner._mark_dependency_failed("dcim.device", row)
            runner.logger.increment_statistics("dcim.device", outcome="failed")
            runner._record_issue(
                "dcim.device",
                f"Skipping device `{row.get('name')}`; isolated after apply "
                f"error: {exc}",
                row,
                exception=exc,
            )
            continue

    try:
        with transaction.atomic():
            if create_objects:
                Device.objects.bulk_create(
                    list(create_objects.values()), batch_size=1000
                )
            if update_objects:
                Device.objects.bulk_update(
                    list(update_objects.values()),
                    fields=update_field_names,
                    batch_size=1000,
                )
    except IntegrityError as exc:
        runner.logger.log_warning(
            f"Bulk write for dcim.device hit a constraint error ({exc}); retrying "
            "row-by-row to isolate the offending row(s).",
            obj=runner.sync,
        )
        _isolate_bulk_objects(
            Device, list(create_objects.values()), "create", runner, "dcim.device"
        )
        _isolate_bulk_objects(
            Device,
            list(update_objects.values()),
            "update",
            runner,
            "dcim.device",
            fields=update_field_names,
        )
    else:
        if branch_active:
            emit_branch_object_changes(
                list(create_objects.values()), list(update_objects.values())
            )

    runner.events_clearer.clear()
    return True


def bulk_orm_apply_ipaddress(runner, rows: list[dict[str, Any]]):
    """Batched apply for ipam.ipaddress with adapter-parity semantics.

    Mirrors ``apply_ipam_ipaddress`` (sync_ipam.py) row-for-row — device and
    interface resolution, dependency skip/fail, network-id/broadcast skips, VRF
    ensure, and the null-VRF net_host vs (address, vrf) coalesce — but collects
    resolved IPAddress objects and writes them with bulk_create/bulk_update at
    the end instead of saving per row. Experimental: enabled only when
    `ipam.ipaddress` is explicitly listed in the sync's `bulk_orm_models`.
    """
    import operator
    from functools import reduce
    from ipaddress import ip_interface

    from dcim.models import Device
    from dcim.models import Interface
    from ipam.models import IPAddress
    from django.core.exceptions import ObjectDoesNotExist
    from django.db import transaction
    from django.db.models import Q

    from ..exceptions import ForwardDependencySkipError
    from ..exceptions import ForwardSearchError

    interface_ct = runner._content_type_for(Interface)
    update_field_names = [
        "address",
        "vrf",
        "status",
        "assigned_object_type",
        "assigned_object_id",
    ]

    device_names = {row.get("device") for row in rows if row.get("device")}
    interface_names = {row.get("interface") for row in rows if row.get("interface")}
    devices_by_name = {
        device.name: device for device in Device.objects.filter(name__in=device_names)
    }
    interfaces_by_key = {
        (interface.device.name, interface.name): interface
        for interface in Interface.objects.select_related("device").filter(
            device__name__in=device_names, name__in=interface_names
        )
    }

    # Pre-ensure VRFs once (the adapter ensures-or-creates per row).
    vrf_by_name = {}
    for vrf_name in {row.get("vrf") for row in rows if row.get("vrf")}:
        vrf_by_name[vrf_name] = runner._ensure_vrf(
            {
                "name": vrf_name,
                "rd": None,
                "description": "",
                "enforce_unique": False,
            },
            update_existing=False,
        )

    # Pre-fetch existing IPs keyed by (host_ip, vrf_id) for net_host coalesce.
    host_ips = set()
    for row in rows:
        try:
            host_ips.add(str(ip_interface(row["address"]).ip))
        except (KeyError, ValueError):
            continue
    existing_by_key = {}
    if host_ips:
        host_filter = reduce(
            operator.or_, (Q(address__net_host=host) for host in host_ips)
        )
        # order_by("pk") + first-wins so a duplicate global IP (same host, same
        # VRF) resolves to the lowest pk deterministically.
        for ip in IPAddress.objects.filter(host_filter).order_by("pk"):
            key = (str(ip.address.ip), ip.vrf_id)
            if key not in existing_by_key:
                existing_by_key[key] = ip

    create_objects = {}
    update_objects = {}
    branch_active = _branch_is_active()

    for row in rows:
        device_name = row.get("device")
        interface_name = row.get("interface")
        try:
            device = devices_by_name.get(device_name) or runner._get_device_by_name(
                device_name
            )
        except ObjectDoesNotExist:
            key = (device_name,)
            if runner._dependency_failed("dcim.device", key):
                runner.logger.increment_statistics("ipam.ipaddress", outcome="skipped")
                runner._record_issue(
                    "ipam.ipaddress",
                    f"Skipping IP assignment because dependency `dcim.device` "
                    f"failed for {key}.",
                    row,
                    exception=ForwardDependencySkipError(
                        "dependency dcim.device failed",
                        model_string="ipam.ipaddress",
                    ),
                )
                continue
            runner._mark_dependency_failed("ipam.ipaddress", row)
            runner.logger.increment_statistics("ipam.ipaddress", outcome="failed")
            runner._record_issue(
                "ipam.ipaddress",
                f"Unable to find device `{device_name}` for IP assignment.",
                row,
                exception=ForwardSearchError(
                    f"device {device_name} not found",
                    model_string="ipam.ipaddress",
                ),
            )
            continue

        interface = interfaces_by_key.get(
            (device.name, interface_name)
        ) or runner._lookup_interface(device, interface_name)
        if interface is None:
            key = (device.name, interface_name)
            if runner._dependency_failed("dcim.interface", key):
                runner.logger.increment_statistics("ipam.ipaddress", outcome="skipped")
                runner._record_issue(
                    "ipam.ipaddress",
                    f"Skipping IP assignment because dependency `dcim.interface` "
                    f"failed for {key}.",
                    row,
                    exception=ForwardDependencySkipError(
                        "dependency dcim.interface failed",
                        model_string="ipam.ipaddress",
                    ),
                )
                continue
            runner._record_aggregated_skip_warning(
                model_string="ipam.ipaddress",
                reason="missing-interface",
                warning_message=(
                    f"Skipping IP address `{row['address']}` on `{device.name}` "
                    f"`{interface_name}` because the target interface was not "
                    "imported."
                ),
            )
            continue

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
                    f"`{interface_name}` because NetBox cannot assign "
                    f"{reason_label} to interfaces."
                ),
            )
            continue

        vrf = vrf_by_name.get(row.get("vrf")) if row.get("vrf") else None
        vrf_id = vrf.pk if vrf is not None else None
        host_ip = str(ip_interface(row["address"]).ip)
        lookup_key = (host_ip, vrf_id)

        ip = existing_by_key.get(lookup_key) or create_objects.get(lookup_key)
        if ip is None:
            ip = IPAddress(
                address=row["address"],
                vrf=vrf,
                status=row["status"],
                assigned_object_type=interface_ct,
                assigned_object_id=interface.pk,
            )
            # Identity proven absent above; skip the per-row validate_unique DB
            # query (constraint violations still surface via isolate). IPAddress
            # has no unique global constraint here, so behaviour is unchanged.
            ip.full_clean(validate_unique=False, validate_constraints=False)
            create_objects[lookup_key] = ip
            existing_by_key[lookup_key] = ip
            runner.logger.increment_statistics("ipam.ipaddress", outcome="applied")
            runner.events_clearer.increment()
            continue

        # `ip` may be a not-yet-saved in-memory create from an earlier row that
        # shares this (host_ip, vrf) key (duplicate IP across devices — only
        # surfaces at full-network scale). snapshot() touches tags, which raises
        # on an unsaved object; only snapshot already-persisted rows.
        if branch_active and ip.pk is not None:
            ip.snapshot()
        changed = False
        if str(ip.address) != str(row["address"]):
            ip.address = row["address"]
            changed = True
        if ip.vrf_id != vrf_id:
            ip.vrf = vrf
            changed = True
        if ip.status != row["status"]:
            ip.status = row["status"]
            changed = True
        if ip.assigned_object_type_id != interface_ct.pk:
            ip.assigned_object_type = interface_ct
            changed = True
        if ip.assigned_object_id != interface.pk:
            ip.assigned_object_id = interface.pk
            changed = True
        if not changed:
            runner.logger.increment_statistics("ipam.ipaddress", outcome="unchanged")
            continue
        if getattr(ip, "pk", None):
            # Field-level validation only on update. IPAddress.clean() runs the
            # global-duplicate check, which would raise when a pre-existing
            # duplicate global IP exists for the same host; the adapter updates via
            # save(update_fields=...) with no clean() at all, so skipping clean()
            # here keeps the two engines consistent and avoids failing the row.
            ip.clean_fields()
            update_objects[ip.pk] = ip
        runner.logger.increment_statistics("ipam.ipaddress", outcome="applied")
        runner.events_clearer.increment()

    from django.db import IntegrityError

    try:
        with transaction.atomic():
            if create_objects:
                IPAddress.objects.bulk_create(
                    list(create_objects.values()), batch_size=1000
                )
            if update_objects:
                IPAddress.objects.bulk_update(
                    list(update_objects.values()),
                    fields=update_field_names,
                    batch_size=1000,
                )
    except IntegrityError as exc:
        runner.logger.log_warning(
            f"Bulk write for ipam.ipaddress hit a constraint error ({exc}); "
            "retrying row-by-row to isolate the offending row(s).",
            obj=runner.sync,
        )
        _isolate_bulk_objects(
            IPAddress, list(create_objects.values()), "create", runner, "ipam.ipaddress"
        )
        _isolate_bulk_objects(
            IPAddress,
            list(update_objects.values()),
            "update",
            runner,
            "ipam.ipaddress",
            fields=update_field_names,
        )
    else:
        if branch_active:
            emit_branch_object_changes(
                list(create_objects.values()), list(update_objects.values())
            )

    runner.events_clearer.clear()
    return True


def bulk_orm_apply_virtualchassis(runner, rows: list[dict[str, Any]]):
    # VirtualChassis staging is a two-phase write (the chassis plus member-device
    # reassignments) and is low-volume, so it is not worth synthesizing branch
    # ObjectChanges for. When staging into a branch, defer to the adapter
    # (per-object saves fire post_save -> tracked branch changes) by returning
    # False so the engine falls back to runner._apply_model_rows.
    if _branch_is_active():
        return False
    from dcim.models import Device
    from dcim.models import VirtualChassis
    from django.core.exceptions import ValidationError
    from django.db import transaction

    from ..exceptions import ForwardDependencySkipError
    from ..exceptions import ForwardSearchError
    from ..exceptions import ForwardSyncDataError

    usable_rows = []
    vc_names = set()
    device_names = set()
    for row in rows:
        vc_name = row.get("vc_name") or row.get("name")
        if row.get("device") and not row.get("vc_position"):
            runner._record_aggregated_skip_warning(
                model_string="dcim.virtualchassis",
                reason="virtual-chassis-without-position",
                warning_message=(
                    "Skipping incomplete virtual chassis assignment for device "
                    f"`{row['device']}` because the row has virtual chassis "
                    "membership but no `vc_position`."
                ),
            )
            runner.logger.increment_statistics(
                "dcim.virtualchassis",
                outcome="skipped",
            )
            continue
        if not vc_name:
            exc = ValidationError("Virtual chassis row is missing `vc_name`.")
            runner.logger.increment_statistics(
                "dcim.virtualchassis",
                outcome="failed",
            )
            runner._record_issue(
                "dcim.virtualchassis",
                str(exc),
                row,
                exception=exc,
                context={"required": ("vc_name", "name")},
            )
            continue
        usable_rows.append((row, vc_name))
        vc_names.add(vc_name)
        if row.get("device"):
            device_names.add(row["device"])

    if not usable_rows:
        runner.events_clearer.clear()
        return True

    existing_vcs = {
        vc.name: vc for vc in VirtualChassis.objects.filter(name__in=vc_names)
    }
    existing_devices = {
        device.name: device for device in Device.objects.filter(name__in=device_names)
    }
    create_vcs = []
    update_vcs = []
    vcs_by_name = dict(existing_vcs)

    for row, vc_name in usable_rows:
        domain = row.get("vc_domain", row.get("domain", ""))
        vc = vcs_by_name.get(vc_name)
        if vc is None:
            vc = VirtualChassis(name=vc_name, domain=domain)
            vc.full_clean()
            create_vcs.append(vc)
            vcs_by_name[vc_name] = vc
            continue
        if vc.domain != domain:
            vc.domain = domain
            vc.full_clean()
            update_vcs.append(vc)

    with transaction.atomic():
        if create_vcs:
            VirtualChassis.objects.bulk_create(create_vcs, batch_size=1000)
        if update_vcs:
            VirtualChassis.objects.bulk_update(
                update_vcs,
                fields=["domain"],
                batch_size=1000,
            )

    vcs_by_name = {
        vc.name: vc for vc in VirtualChassis.objects.filter(name__in=vc_names)
    }
    occupied_positions = {
        (device.virtual_chassis_id, device.vc_position): device
        for device in Device.objects.filter(
            virtual_chassis_id__in=[
                vc.pk for vc in vcs_by_name.values() if getattr(vc, "pk", None)
            ],
            vc_position__isnull=False,
        )
    }
    devices_to_update = []

    for row, vc_name in usable_rows:
        vc = vcs_by_name[vc_name]
        if not row.get("device"):
            runner.logger.increment_statistics(
                "dcim.virtualchassis",
                outcome="applied",
            )
            runner.events_clearer.increment()
            continue

        device_name = row["device"]
        device = existing_devices.get(device_name)
        if device is None:
            key = (device_name,)
            if runner._dependency_failed("dcim.device", key):
                exc = ForwardDependencySkipError(
                    "Skipping virtual chassis assignment because dependency "
                    f"`dcim.device` failed for {key}.",
                    model_string="dcim.virtualchassis",
                    context={"device": device_name},
                    data=row,
                )
                runner.logger.increment_statistics(
                    "dcim.virtualchassis",
                    outcome="skipped",
                )
                runner._record_issue(
                    "dcim.virtualchassis",
                    str(exc),
                    row,
                    exception=exc,
                    context=exc.context,
                    defaults=exc.defaults,
                )
                continue
            exc = ForwardSearchError(
                f"Unable to find device `{device_name}` for virtual chassis assignment.",
                model_string="dcim.virtualchassis",
                context={"device": device_name},
                data=row,
            )
            runner._mark_dependency_failed("dcim.virtualchassis", row)
            runner.logger.increment_statistics(
                "dcim.virtualchassis",
                outcome="failed",
            )
            runner._record_issue(
                "dcim.virtualchassis",
                str(exc),
                row,
                exception=exc,
                context=exc.context,
                defaults=exc.defaults,
            )
            continue

        position = row["vc_position"]
        position_key = (vc.pk, position)
        position_conflict = occupied_positions.get(position_key)
        if position_conflict is not None and position_conflict.pk != device.pk:
            exc = ForwardSyncDataError(
                f"Virtual chassis `{vc_name}` already has device "
                f"`{position_conflict.name}` at position `{position}`.",
                model_string="dcim.virtualchassis",
                context={
                    "device": device_name,
                    "virtual_chassis": vc_name,
                    "vc_position": position,
                    "conflicting_device": position_conflict.name,
                },
                data=row,
            )
            runner._mark_dependency_failed("dcim.virtualchassis", row)
            runner.logger.increment_statistics(
                "dcim.virtualchassis",
                outcome="failed",
            )
            runner._record_issue(
                "dcim.virtualchassis",
                str(exc),
                row,
                exception=exc,
                context=exc.context,
                defaults=exc.defaults,
            )
            continue

        if device.virtual_chassis_id == vc.pk and device.vc_position == position:
            # Already a member at this position — don't re-PATCH every sync.
            occupied_positions[position_key] = device
            runner.logger.increment_statistics(
                "dcim.virtualchassis",
                outcome="unchanged",
            )
            continue
        device.virtual_chassis = vc
        device.vc_position = position
        device.full_clean()
        devices_to_update.append(device)
        occupied_positions[position_key] = device
        runner.logger.increment_statistics(
            "dcim.virtualchassis",
            outcome="applied",
        )
        runner.events_clearer.increment()

    with transaction.atomic():
        if devices_to_update:
            Device.objects.bulk_update(
                devices_to_update,
                fields=["virtual_chassis", "vc_position"],
                batch_size=1000,
            )

    runner.events_clearer.clear()
    return True


# Encodes a null value for a nullable identity field (e.g. ipam.prefix global
# table where vrf is None) so it still forms a stable, matchable lookup key
# instead of bailing to None (which would create duplicates).
LOOKUP_NULL_SENTINEL = "\x00null"


def lookup_key_from_object(obj, lookup_set, *, model_string=None, nullable_fields=()):
    return _build_lookup_key(
        lambda field_name: getattr(obj, field_name, None),
        lookup_set,
        model_string,
        nullable_fields,
    )


def lookup_key_from_values(
    values, lookup_set, *, model_string=None, nullable_fields=()
):
    return _build_lookup_key(values.get, lookup_set, model_string, nullable_fields)


def _build_lookup_key(getter, lookup_set, model_string, nullable_fields):
    parts = []
    for field_name in lookup_set:
        value = _normalize_lookup_component(
            model_string, field_name, getter(field_name)
        )
        if value in ("", None):
            if field_name in nullable_fields:
                parts.append(LOOKUP_NULL_SENTINEL)
                continue
            return None
        parts.append(str(value))
    return "|".join(parts)


def _normalize_lookup_component(model_string, field_name, value):
    if hasattr(value, "pk"):
        return value.pk
    # NetBox stores ipam.prefix as a canonical CIDR IPNetwork (host bits cleared
    # on save), while incoming rows are raw strings — normalize both sides so the
    # lookup key matches regardless of input form.
    if (
        model_string == "ipam.prefix"
        and field_name == "prefix"
        and value not in ("", None)
    ):
        import netaddr

        try:
            return str(netaddr.IPNetwork(str(value)).cidr)
        except (ValueError, netaddr.AddrFormatError):
            return value
    return value


def lookup_key_value(value):
    if hasattr(value, "pk"):
        return value.pk
    return value


def bulk_orm_apply_tree_models(
    *,
    runner,
    model_string: str,
    model,
    fields: tuple[str, ...],
    lookup_sets: tuple[tuple[str, ...], ...],
    normalized_rows: list[dict[str, Any]],
    nullable_lookup_fields: tuple[str, ...] = (),
):
    from django.db import transaction
    from django.db.models import Q

    from .sync_primitives import _model_field_value_matches

    create_only_fields = CREATE_ONLY_UPDATE_FIELDS_BY_MODEL.get(
        model_string, frozenset()
    )

    with transaction.atomic():
        lookup_values = {
            field_name: []
            for field_name in {
                field for lookup_set in lookup_sets for field in lookup_set
            }
        }
        for values in normalized_rows:
            for field_name in lookup_values:
                value = values.get(field_name)
                if value not in ("", None):
                    lookup_values[field_name].append(value)

        existing_qs = model.objects.none()
        if any(lookup_values.values()):
            query = Q()
            for field_name, values in lookup_values.items():
                if values:
                    query |= Q(**{f"{field_name}__in": values})
            existing_qs = model.objects.filter(query).order_by("pk")

        lookup_cache = {lookup_set: {} for lookup_set in lookup_sets}
        for obj in existing_qs:
            for lookup_set in lookup_sets:
                key = lookup_key_from_object(
                    obj,
                    lookup_set,
                    model_string=model_string,
                    nullable_fields=nullable_lookup_fields,
                )
                if key is not None and key not in lookup_cache[lookup_set]:
                    lookup_cache[lookup_set][key] = obj

        for values in normalized_rows:
            existing = None
            for lookup_set in lookup_sets:
                lookup_key = lookup_key_from_values(
                    values,
                    lookup_set,
                    model_string=model_string,
                    nullable_fields=nullable_lookup_fields,
                )
                if lookup_key is None:
                    continue
                if lookup_key in lookup_cache[lookup_set]:
                    existing = lookup_cache[lookup_set][lookup_key]
                if existing is not None:
                    break
            if existing is None:
                obj = model(**values)
                obj.full_clean()
                obj.save()
                runner.logger.increment_statistics(model_string, outcome="applied")
                runner.events_clearer.increment()
                for lookup_set in lookup_sets:
                    lookup_key = lookup_key_from_values(
                        values,
                        lookup_set,
                        model_string=model_string,
                        nullable_fields=nullable_lookup_fields,
                    )
                    if lookup_key is not None:
                        lookup_cache[lookup_set][lookup_key] = obj
                continue

            changed = False
            for field_name in fields:
                if field_name in create_only_fields:
                    continue
                incoming = values.get(field_name)
                if not _model_field_value_matches(
                    model, existing, field_name, incoming
                ):
                    setattr(existing, field_name, incoming)
                    changed = True
            if changed:
                # Existing objects already satisfy DB constraints; skip
                # validate_unique/validate_constraints (both issue DB queries).
                existing.clean_fields()
                existing.clean()
                existing.save()
                runner.logger.increment_statistics(model_string, outcome="applied")
                runner.events_clearer.increment()
                continue
            runner.logger.increment_statistics(model_string, outcome="unchanged")
    runner.events_clearer.clear()
    return True
