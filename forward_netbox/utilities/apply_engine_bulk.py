from collections import defaultdict
from typing import Any

from netbox_branching.contextvars import active_branch
from netbox_branching.models import ChangeDiff

# Fields the bulk engines must set on CREATE but preserve on UPDATE, matching the
# adapter's intent. Platform-map manufacturer values are authoritative in this
# engine, so there are no create-only Platform fields.
CREATE_ONLY_UPDATE_FIELDS_BY_MODEL: dict[str, frozenset] = {}

# Chunk size for synthesizing branch ObjectChanges after a bulk write: bounds the
# serialized batch + the ObjectChange list so memory does not grow with the
# model's total row count (a single shard can be ~500k rows).
EMIT_OBJECT_CHANGE_CHUNK = 1000

_APPLY_LOOKUP_CHUNK_SIZE = 500


def _chunks(items, size=_APPLY_LOOKUP_CHUNK_SIZE):
    for index in range(0, len(items), size):
        yield items[index : index + size]


def _device_scoped_name_query(pairs):
    from django.db.models import Q

    grouped_names_by_device = {}
    for device_name, interface_name in pairs:
        grouped_names_by_device.setdefault(device_name, set()).add(interface_name)
    query = Q()
    for device_name, interface_names in grouped_names_by_device.items():
        query |= Q(
            device__name=device_name,
            name__in=sorted(interface_names),
        )
    return query


def _isolate_bulk_objects(
    model, objects, operation, runner, model_string, *, fields=None
):
    """Re-apply objects one at a time after a bulk write hit a constraint error.

    A failed ``bulk_create``/``bulk_update`` rolls back the whole batch, so one
    bad row would otherwise fail the entire shard. This saves each object in its
    own savepoint — good rows apply, the offending row(s) are recorded as
    ingestion issues — restoring the per-row resilience the adapter path has.
    """
    from django.db import DEFAULT_DB_ALIAS
    from django.db import transaction

    branch = active_branch.get()
    using = branch.connection_name if branch is not None else DEFAULT_DB_ALIAS

    for obj in objects:
        try:
            with transaction.atomic(using=using):
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
    return active_branch.get() is not None


def _active_write_alias():
    from django.db import DEFAULT_DB_ALIAS

    branch = active_branch.get()
    return branch.connection_name if branch is not None else DEFAULT_DB_ALIAS


def _serializer_prefetch_fields(model):
    from extras.utils import is_taggable

    fields = [
        field.name
        for field in model._meta.many_to_many
        if field.remote_field.through._meta.auto_created
    ]
    if is_taggable(model) and "tags" not in fields:
        fields.append("tags")
    return fields


def _rebuild_prefix_hierarchies(vrf_ids, objects, *, using):
    """Rebuild affected NetBox prefix trees after a bulk write."""
    from django.db import transaction
    from ipam.models import Prefix
    from ipam.utils import rebuild_prefixes

    with transaction.atomic(using=using):
        for vrf_id in sorted(
            set(vrf_ids), key=lambda value: (value is not None, value or 0)
        ):
            rebuild_prefixes(vrf_id)
        objects_by_pk = {
            obj.pk: obj for obj in objects if getattr(obj, "pk", None) is not None
        }
        refreshed = {}
        for pk_batch in _chunks(list(objects_by_pk), size=1000):
            refreshed.update(
                {
                    pk: (depth, children)
                    for pk, depth, children in Prefix.objects.using(using)
                    .filter(pk__in=pk_batch)
                    .values_list("pk", "_depth", "_children")
                }
            )
        for pk, obj in objects_by_pk.items():
            if pk in refreshed:
                obj._depth, obj._children = refreshed[pk]


def _current_change_diff_data(model, object_ids):
    from django.db.models import prefetch_related_objects
    from netbox_branching.utilities import deactivate_branch

    with deactivate_branch():
        objects = list(model.objects.filter(pk__in=object_ids))
        prefetch_fields = _serializer_prefetch_fields(model)
        if prefetch_fields and objects:
            prefetch_related_objects(objects, *prefetch_fields)
        return {
            obj.pk: obj.serialize_object(exclude=["created", "last_updated"])
            for obj in objects
        }


def _sync_branch_change_diffs(branch, object_changes, action):
    """Batch the ChangeDiff state normally produced by post_save signals."""
    from core.choices import ObjectChangeActionChoices
    from django.utils import timezone

    if not object_changes:
        return
    object_type = object_changes[0].changed_object_type
    object_ids = [change.changed_object_id for change in object_changes]
    existing = {
        diff.object_id: diff
        for diff in ChangeDiff.objects.using(branch.connection_name)
        .select_related("object_type")
        .filter(
            branch=branch,
            object_type_id=object_type.pk,
            object_id__in=object_ids,
        )
    }
    needs_current = [
        object_id
        for object_id in object_ids
        if object_id not in existing
        and action != ObjectChangeActionChoices.ACTION_CREATE
    ]
    current_by_pk = _current_change_diff_data(object_type.model_class(), needs_current)
    create_diffs = []
    update_diffs = []
    now = timezone.now()
    for change in object_changes:
        diff = existing.get(change.changed_object_id)
        if diff is None:
            diff = ChangeDiff(
                branch=branch,
                object_type=object_type,
                object_id=change.changed_object_id,
                object_repr=(change.object_repr or "")[:200],
                action=action,
                original=change.prechange_data_clean or None,
                modified=change.postchange_data_clean or None,
                current=current_by_pk.get(change.changed_object_id),
                last_updated=now,
            )
            diff._update_conflicts()
            create_diffs.append(diff)
            continue
        diff.object_repr = (change.object_repr or "")[:200]
        if diff.action != ObjectChangeActionChoices.ACTION_CREATE:
            diff.action = action
        diff.modified = change.postchange_data_clean or None
        diff.last_updated = now
        diff._update_conflicts()
        update_diffs.append(diff)
    if create_diffs:
        ChangeDiff.objects.using(branch.connection_name).bulk_create(
            create_diffs,
            batch_size=EMIT_OBJECT_CHANGE_CHUNK,
        )
    if update_diffs:
        ChangeDiff.objects.using(branch.connection_name).bulk_update(
            update_diffs,
            fields=[
                "object_repr",
                "action",
                "modified",
                "conflicts",
                "last_updated",
            ],
            batch_size=EMIT_OBJECT_CHANGE_CHUNK,
        )


def emit_branch_object_changes(created, updated, deleted=()):
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
    the merge's ``deserialize`` consumes). Django's serializer otherwise issues
    one query per many-to-many field per object, so prefetch every serialized
    relationship once per chunk before calling the model's authoritative
    ``to_objectchange`` override. CREATE uses the freshly-saved instance (pk
    back-filled by bulk_create); UPDATE requires the caller to have
    ``snapshot()``-ed before mutating so ``prechange_data`` is correct. DELETE
    is emitted before the bulk delete while the authoritative object still
    exists. No-op (returns False) when not staging into a branch or without a
    request context.
    """
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

    user = request.user
    user_name = getattr(user, "username", "") or ""
    request_id = getattr(request, "id", None)

    def _flush(objs, action):
        # Build + write ObjectChanges in chunks so the OC list never grows with
        # the model's total row count (a single shard can be ~500k rows).
        for start in range(0, len(objs), EMIT_OBJECT_CHANGE_CHUNK):
            chunk = objs[start : start + EMIT_OBJECT_CHANGE_CHUNK]
            prefetch_fields = _serializer_prefetch_fields(type(chunk[0]))
            if prefetch_fields:
                prefetch_related_objects(chunk, *prefetch_fields)
            ocs = []
            for obj in chunk:
                if action == ObjectChangeActionChoices.ACTION_DELETE and not getattr(
                    obj, "_prechange_snapshot", None
                ):
                    obj.snapshot()
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
            # bulk_create bypasses Branching's post_save receiver. Mirror its
            # CREATE and UPDATE semantics in batches on this same connection so
            # branch rows, ObjectChanges, and review/conflict metadata are atomic.
            _sync_branch_change_diffs(branch, ocs, action)

    created = list(created)
    updated = list(updated)
    deleted = list(deleted)
    if created:
        _flush(created, ObjectChangeActionChoices.ACTION_CREATE)
    if updated:
        _flush(updated, ObjectChangeActionChoices.ACTION_UPDATE)
    if deleted:
        _flush(deleted, ObjectChangeActionChoices.ACTION_DELETE)
    return True


class _BulkDeleteNeedsIsolation(Exception):
    pass


def bulk_orm_delete_prefixes(runner, rows: list[dict[str, Any]]):
    """Delete a Prefix shard transactionally while preserving branch evidence."""
    branch = active_branch.get()
    if branch is None:
        return False

    from django.db import IntegrityError
    from django.db import transaction
    from django.db.models.deletion import ProtectedError
    from django.db.models.deletion import RestrictedError
    from ipam.models import Prefix
    from ipam.models import VRF
    from netbox.context import current_request
    from utilities.exceptions import AbortRequest

    from .bulk_delete import collector_delete_without_model_signals
    from .sync_reporting import _increment_ingestion_delete_totals

    rows = list(rows)
    if not rows:
        return True
    runner.logger.log_info(
        f"Deleting {len(rows)} rows for ipam.prefix.",
        obj=runner.sync,
    )

    requested_vrf_names = {
        row.get("vrf") for row in rows if row.get("vrf") not in ("", None)
    }
    vrfs_by_name = {}
    for batch in _chunks(sorted(requested_vrf_names)):
        vrfs_by_name.update(
            {vrf.name: vrf for vrf in VRF.objects.filter(name__in=batch)}
        )

    identities_by_vrf = defaultdict(set)
    requested_identities = []
    for row in rows:
        prefix = row.get("prefix")
        vrf_name = row.get("vrf")
        if not prefix or (vrf_name and vrf_name not in vrfs_by_name):
            requested_identities.append(None)
            continue
        vrf_id = vrfs_by_name[vrf_name].pk if vrf_name else None
        identity = (str(prefix), vrf_id)
        requested_identities.append(identity)
        identities_by_vrf[vrf_id].add(str(prefix))

    found_by_identity = {}
    for vrf_id, prefixes in identities_by_vrf.items():
        for batch in _chunks(sorted(prefixes)):
            queryset = Prefix.objects.filter(prefix__in=batch)
            queryset = (
                queryset.filter(vrf_id=vrf_id)
                if vrf_id is not None
                else queryset.filter(vrf__isnull=True)
            )
            for prefix in queryset:
                found_by_identity[(str(prefix.prefix), prefix.vrf_id)] = prefix

    delete_objects = []
    seen_pks = set()
    for identity in requested_identities:
        obj = found_by_identity.get(identity)
        if obj is None or obj.pk in seen_pks:
            continue
        seen_pks.add(obj.pk)
        delete_objects.append(obj)

    skipped_count = len(rows) - len(delete_objects)
    if delete_objects:
        using = branch.connection_name
        affected_vrf_ids = {obj.vrf_id for obj in delete_objects}
        try:
            with transaction.atomic(using=using):
                emit_branch_object_changes((), (), delete_objects)
                request_token = current_request.set(None)
                try:
                    try:
                        collector_delete_without_model_signals(
                            Prefix.objects.using(using).filter(
                                pk__in=[obj.pk for obj in delete_objects]
                            ),
                            signal_free_models={Prefix},
                        )
                    except (
                        AbortRequest,
                        IntegrityError,
                        ProtectedError,
                        RestrictedError,
                    ) as exc:
                        raise _BulkDeleteNeedsIsolation from exc
                finally:
                    current_request.reset(request_token)
                _rebuild_prefix_hierarchies(affected_vrf_ids, (), using=using)
        except _BulkDeleteNeedsIsolation as exc:
            runner.logger.log_warning(
                "Bulk delete for ipam.prefix encountered a protected or constrained "
                f"row ({exc.__cause__}); retrying row-by-row to isolate it.",
                obj=runner.sync,
            )
            return False

        runner.logger.increment_statistics(
            "ipam.prefix", outcome="applied", amount=len(delete_objects)
        )
        _increment_ingestion_delete_totals(runner, len(delete_objects))
        for _ in delete_objects:
            runner.events_clearer.increment()
    if skipped_count:
        runner.logger.increment_statistics(
            "ipam.prefix", outcome="skipped", amount=skipped_count
        )
    runner.logger.log_info(
        "Finished deleting rows for ipam.prefix.",
        obj=runner.sync,
    )
    runner.events_clearer.clear()
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
            "lookup_sets": (
                ("manufacturer", "slug"),
                ("manufacturer", "model"),
            ),
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
        sites = []
        for batch in _chunks(list(site_values)):
            sites.extend(Site.objects.filter(Q(slug__in=batch) | Q(name__in=batch)))
        site_by_slug = {site.slug: site for site in sites if site.slug}
        site_by_name = {site.name: site for site in sites if site.name}
    if model_string in {"dcim.devicetype", "dcim.platform"}:
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
        manufacturers = []
        for batch in _chunks(list(manufacturer_values)):
            manufacturers.extend(
                Manufacturer.objects.filter(Q(slug__in=batch) | Q(name__in=batch))
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
        existing_vrf_names = set()
        for batch in _chunks(list(requested_vrf_names)):
            existing_vrf_names.update(
                VRF.objects.filter(name__in=batch).values_list("name", flat=True)
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
        vrf_by_name = {}
        for batch in _chunks(list(requested_vrf_names)):
            for vrf in VRF.objects.filter(name__in=batch):
                if vrf.name:
                    vrf_by_name[vrf.name] = vrf

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
            manufacturer = None
            if row.get("manufacturer"):
                manufacturer = manufacturer_by_slug.get(
                    row.get("manufacturer_slug")
                ) or manufacturer_by_name.get(row.get("manufacturer"))
            # The grouped Platform query emits a blank manufacturer for a
            # cross-vendor platform. None is authoritative and clears a stale
            # owner; a unique manufacturer resolves to the canonical FK.
            normalized["manufacturer"] = manufacturer
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

    if model_string in {"dcim.devicerole", "dcim.platform"}:
        # These low-volume models retain the adapter-equivalent save path.
        return bulk_orm_apply_tree_models(
            runner=runner,
            model_string=model_string,
            model=model,
            fields=fields,
            lookup_sets=lookup_sets,
            normalized_rows=normalized_rows,
            nullable_lookup_fields=nullable_lookup_fields,
        )

    existing_by_lookup = {lookup_set: {} for lookup_set in lookup_sets}
    if any(lookup_values.values()):
        for field_name, values in lookup_values.items():
            for batch in _chunks(list(values)):
                for obj in model.objects.filter(**{f"{field_name}__in": batch}):
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
    prefix_vrf_ids = set()
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
            if model_string == "ipam.prefix":
                prefix_vrf_ids.add(getattr(values.get("vrf"), "pk", None))
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
        changed_values = []
        for field_name in fields:
            if field_name in create_only_fields:
                continue
            incoming = values.get(field_name)
            # Use the adapter's value matcher: it compares relations by id and
            # special-cases typed fields (e.g. ipam.prefix IPNetwork vs string),
            # so a re-applied row does not churn just because the stored type
            # differs from the incoming string.
            if not _model_field_value_matches(model, existing, field_name, incoming):
                changed_values.append((field_name, incoming))
        if changed_values and getattr(existing, "pk", None) is not None:
            if model_string == "ipam.prefix":
                prefix_vrf_ids.add(existing.vrf_id)
                prefix_vrf_ids.add(getattr(values.get("vrf"), "pk", None))
            if branch_active:
                # Snapshot only a row that will actually change. On repeat syncs,
                # unchanged rows avoid serializer and relationship queries.
                existing.snapshot()
            for field_name, incoming in changed_values:
                setattr(existing, field_name, incoming)
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

    isolated_write = False
    using = _active_write_alias()
    with transaction.atomic(using=using):
        try:
            with transaction.atomic(using=using):
                if create_objects:
                    model.objects.bulk_create(create_objects, batch_size=1000)
                if update_objects:
                    model.objects.bulk_update(
                        update_objects,
                        fields=list(fields),
                        batch_size=1000,
                    )
        except IntegrityError as exc:
            if branch_active:
                # Branch rows, ObjectChanges, and ChangeDiffs are one transaction.
                # Do not partially isolate through signal-driven writes on a
                # second connection.
                raise
            isolated_write = True
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
                model,
                update_objects,
                "update",
                runner,
                model_string,
                fields=fields,
            )
        if model_string == "ipam.prefix" and (create_objects or update_objects):
            # NetBox's per-row Prefix signal is quadratic for large initial loads.
            # The supported bulk rebuild computes the same cached hierarchy once per
            # affected VRF, then refreshed instances serialize the correct branch state.
            _rebuild_prefix_hierarchies(
                prefix_vrf_ids,
                [*create_objects, *update_objects],
                using=using,
            )
        # Bulk write succeeded and recorded no post_save, so synthesize the branch
        # ObjectChanges the merge replays from (no-op direct-to-main). Isolated saves
        # already emitted their own changes. Evidence is mandatory: any emission
        # error aborts the branch transaction instead of stranding rows.
        if branch_active and not isolated_write:
            emit_branch_object_changes(create_objects, update_objects)
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
    interface_pairs = {
        (row.get("device"), row.get("interface"))
        for row in rows
        if row.get("device") not in ("", None)
        and row.get("interface") not in ("", None)
    }
    mac_values = {
        row.get("mac") or row.get("mac_address")
        for row in rows
        if (row.get("mac") or row.get("mac_address")) not in ("", None)
    }

    devices_by_name = {}
    for batch in _chunks(list(device_names)):
        for device in Device.objects.filter(name__in=batch):
            devices_by_name[device.name] = device
    interfaces_by_key = {}
    # Pair chunking avoids a device/interface Cartesian product across batches.
    for batch in _chunks(list(interface_pairs)):
        query = _device_scoped_name_query(batch)
        for interface in Interface.objects.select_related("device").filter(query):
            interfaces_by_key[(interface.device.name, interface.name)] = interface
    macs_by_address = {}
    for batch in _chunks(list(mac_values)):
        for mac in MACAddress.objects.filter(mac_address__in=batch):
            macs_by_address[_canonical_mac(mac.mac_address)] = mac

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
    using = _active_write_alias()
    with transaction.atomic(using=using):
        try:
            with transaction.atomic(using=using):
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
            if branch_active:
                raise
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
    """Batched apply for dcim.interface.

    Plain interfaces are resolved with adapter-parity semantics (device skip/
    fail, optional mtu/speed/description, access/tagged mode + untagged VLAN) and
    written with bulk_create/bulk_update. LAG memberships are resolved after all
    interfaces in the shard have been created, then assigned with one bulk
    update in the same transaction. Existing cabled interfaces becoming LAGs,
    including a parent discovered through a member row, use an isolated atomic
    sequence which removes the cable before changing the type. Existing rows
    load from the DB, so fields absent from a row are written back unchanged
    (no clearing), matching the adapter upsert.
    """
    from dcim.models import Device
    from dcim.models import Interface
    from django.core.exceptions import ObjectDoesNotExist
    from django.db import transaction

    from ..exceptions import ForwardDependencySkipError
    from ..exceptions import ForwardSearchError
    from .interface_naming import canonical_interface_key
    from .sync_interface import _interface_untagged_vlan
    from .sync_primitives import forget_lookup_object

    update_field_names = [
        "type",
        "enabled",
        "mtu",
        "speed",
        "description",
        "mode",
        "untagged_vlan",
        "lag",
    ]

    def _validate_interface(interface):
        # A device created earlier in the same native branch has no row in the
        # main schema. Django's ForeignKey field validation resolves its
        # queryset outside the active branch and rejects that valid branch-only
        # device. The device was already resolved explicitly above and the
        # branch FK enforces it at write time, so validate every other field and
        # the model contract without repeating that cross-schema lookup.
        interface.full_clean(
            exclude={"device"},
            validate_unique=False,
            validate_constraints=False,
        )

    device_names = {row.get("device") for row in rows if row.get("device")}
    devices_by_name = {}
    for batch in _chunks(list(device_names)):
        for device in Device.objects.filter(name__in=batch):
            devices_by_name[device.name] = device

    create_objects = {}
    update_objects = {}
    # Pending bulk creates indexed by canonical (device.pk, (type, number)) so a
    # second row whose name canonicalizes to one already staged (e.g. Forward
    # emits both ``po9`` and ``Port-channel9``) collapses onto the first instead
    # of staging a duplicate the lookup can't yet see (it isn't committed).
    pending_canonical = {}
    # Existing cabled interfaces becoming LAGs retain the established
    # cable-removal behavior through an isolated branch transaction. Ordinary
    # LAG memberships are collected and resolved in a second bulk phase.
    cabled_lag_rows = []
    lag_links = []
    row_outcomes = []
    snapshotted_pks = set()

    def _snapshot_once(interface):
        if (
            branch_active
            and getattr(interface, "pk", None) is not None
            and interface.pk not in snapshotted_pks
        ):
            interface.snapshot()
            snapshotted_pks.add(interface.pk)

    def _apply_cabled_lag_row(row, cabled_interface):
        # Keep cable deletion and the row's complete bulk mutation/evidence on
        # the branch alias in one transaction. The cable's normal model signals
        # still run, but request tracking is suppressed because evidence is
        # emitted explicitly on the same alias before recursively applying the
        # now-uncabled row through this bulk path.
        from netbox.context import current_request

        try:
            with transaction.atomic(using=_active_write_alias()):
                cable = getattr(cabled_interface, "cable", None)
                if cable is None:
                    return bulk_orm_apply_interface(runner, [row])
                if branch_active:
                    emit_branch_object_changes([], [], [cable])
                request_token = current_request.set(None)
                try:
                    cable.delete()
                finally:
                    current_request.reset(request_token)
                cabled_interface.cable_id = None
                cabled_interface._state.fields_cache.pop("cable", None)
                forget_lookup_object(runner, cabled_interface)
                return bulk_orm_apply_interface(runner, [row])
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
            forget_lookup_object(runner, cabled_interface)
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

    def _reject_lag_self_parent(row, device):
        exc = ForwardSearchError(
            f"Interface `{row['name']}` on device `{device.name}` cannot be "
            "its own LAG parent.",
            model_string="dcim.interface",
            context={
                "device": device.name,
                "name": row["name"],
                "lag": row.get("lag"),
            },
            data=row,
        )
        runner._mark_dependency_failed("dcim.interface", row)
        runner.logger.increment_statistics("dcim.interface", outcome="failed")
        runner._record_issue(
            "dcim.interface",
            str(exc),
            row,
            exception=exc,
            context=getattr(exc, "context", None),
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

        # Reject raw or canonically equivalent self-parenting before the member
        # enters any create/update collection. Otherwise `Po1` with
        # `lag=Port-channel1` is reported failed but still written.
        if row.get("lag"):
            name_key = canonical_interface_key(row["name"])
            lag_key = canonical_interface_key(row["lag"])
            if row["lag"] == row["name"] or (
                name_key is not None and name_key == lag_key
            ):
                _reject_lag_self_parent(row, device)
                continue

            existing_parent = runner._lookup_interface(device, row["lag"])
            if (
                existing_parent is not None
                and existing_parent.type != "lag"
                and getattr(existing_parent, "cable", None) is not None
            ):
                cabled_lag_rows.append((row, existing_parent))
                continue

        # Cable removal remains deliberately row-oriented; it is destructive
        # and uncommon, unlike ordinary LAG membership at customer scale.
        needs_cabled_conversion = (
            row.get("type") == "lag"
            and existing is not None
            and getattr(existing, "cable", None) is not None
        )
        if needs_cabled_conversion:
            cabled_lag_rows.append((row, existing))
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
                _validate_interface(interface)
                create_objects[key] = interface
                if canon_key is not None:
                    pending_canonical[canon_key] = interface
            outcome = ["applied"]
            row_outcomes.append(outcome)
            if row.get("lag"):
                lag_links.append((row, device, interface, outcome))
            continue

        # Existing interface: only write when a field actually changes,
        # otherwise every sync re-PATCHes unchanged interfaces.
        changed_values = []
        for field, value in defaults.items():
            if _interface_field_differs(existing, field, value):
                changed_values.append((field, value))
        outcome = ["unchanged"]
        row_outcomes.append(outcome)
        if changed_values:
            _snapshot_once(existing)
            for field, value in changed_values:
                setattr(existing, field, value)
            _validate_interface(existing)
            update_objects[existing.pk] = existing
            outcome[0] = "applied"
        if row.get("lag"):
            lag_links.append((row, device, existing, outcome))

    # Resolve every LAG relationship against the existing DB rows and this
    # shard's pending creates. The adapter creates a missing parent as a minimal
    # enabled LAG; preserve that behavior, but do it once per parent.
    resolved_lag_links = []
    for row, device, member, outcome in lag_links:
        lag_name = row["lag"]
        parent = create_objects.get((device.pk, lag_name))
        if parent is None:
            canon = canonical_interface_key(lag_name)
            if canon is not None:
                parent = pending_canonical.get((device.pk, canon))
        if parent is None:
            parent = runner._lookup_interface(device, lag_name)
        if parent is None:
            parent = Interface(
                device=device,
                name=lag_name,
                type="lag",
                enabled=True,
                mtu=None,
                description="",
                speed=None,
            )
            _validate_interface(parent)
            create_objects[(device.pk, lag_name)] = parent
            canon = canonical_interface_key(lag_name)
            if canon is not None:
                pending_canonical[(device.pk, canon)] = parent
            outcome[0] = "applied"
        elif parent.type != "lag":
            _snapshot_once(parent)
            parent.type = "lag"
            _validate_interface(parent)
            if getattr(parent, "pk", None) is not None:
                update_objects[parent.pk] = parent
            outcome[0] = "applied"

        if parent is member:
            # Canonical spellings can identify the same interface even when the
            # raw strings differ. Treat that as the same invalid self-parent as
            # an exact spelling.
            runner._mark_dependency_failed("dcim.interface", row)
            runner._record_issue(
                "dcim.interface",
                f"Interface `{row['name']}` on device `{device.name}` cannot be "
                "its own LAG parent.",
                row,
                exception=ForwardSearchError(
                    "interface cannot be its own LAG parent",
                    model_string="dcim.interface",
                ),
            )
            outcome[0] = "failed"
            continue

        if parent.pk is None or member.lag_id != parent.pk:
            _snapshot_once(member)
            if getattr(member, "pk", None) is not None:
                update_objects[member.pk] = member
            outcome[0] = "applied"
            resolved_lag_links.append((member, parent))

    from django.db import IntegrityError

    using = _active_write_alias()
    with transaction.atomic(using=using):
        try:
            with transaction.atomic(using=using):
                if create_objects:
                    Interface.objects.bulk_create(
                        list(create_objects.values()), batch_size=1000
                    )
                created_lag_members = []
                for member, parent in resolved_lag_links:
                    member.lag = parent
                    if member.pk in update_objects:
                        continue
                    created_lag_members.append(member)
                if update_objects:
                    Interface.objects.bulk_update(
                        list(update_objects.values()),
                        fields=update_field_names,
                        batch_size=1000,
                    )
                if created_lag_members:
                    Interface.objects.bulk_update(
                        created_lag_members,
                        fields=["lag"],
                        batch_size=1000,
                    )
        except IntegrityError as exc:
            if branch_active:
                raise
            runner.logger.log_warning(
                f"Bulk write for dcim.interface hit a constraint error ({exc}); "
                "retrying row-by-row to isolate the offending row(s).",
                obj=runner.sync,
            )
            _isolate_bulk_objects(
                Interface,
                list(create_objects.values()),
                "create",
                runner,
                "dcim.interface",
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

    # Apply isolated cabled conversions after unrelated rows are committed. Each
    # conversion re-enters this bulk path after its cable is removed, so its
    # parent/member/evidence state is all-or-nothing without slowing ordinary
    # LAG memberships.
    for row, cabled_lag_parent in cabled_lag_rows:
        _apply_cabled_lag_row(row, cabled_lag_parent)

    for (outcome,) in row_outcomes:
        runner.logger.increment_statistics("dcim.interface", outcome=outcome)
        if outcome == "applied":
            runner.events_clearer.increment()

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
    synthesis. Opt-in device scope tags are resolved once and their missing
    TaggedItem relations are bulk-created before final Device serialization.
    Rows that need adapter sequencing (a missing parent the adapter would create
    on demand or virtual-chassis membership) delegate row-by-row to
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
    from django.utils.text import slugify
    from extras.models import Tag
    from extras.models import TaggedItem

    from ..exceptions import ForwardDependencySkipError
    from ..exceptions import ForwardSearchError
    from .sync_device import _scope_tags_enabled
    from .sync_device import apply_dcim_device
    from .sync_device import record_device_identity_candidate

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

    scope_tags_enabled = _scope_tags_enabled(runner)
    scope_tags_by_name = {}
    scope_names = set()
    if scope_tags_enabled:
        scope_names = {
            tag_name
            for row in rows
            for tag_name in runner._scope_matched_tags.get(row.get("name"), [])
        }

    # Endpoint-only platforms are absent from the network.devices-backed
    # Platform map. Their NQE rows explicitly mark manufacturer ownership as
    # authoritative (the Platform identity is the canonical manufacturer), so
    # repair/create those few dependencies before the bulk lookup indexes them.
    for row in rows:
        if row.get("platform") and row.get("platform_manufacturer_authoritative"):
            runner._ensure_platform(
                {
                    "name": row["platform"],
                    "slug": row.get("platform_slug"),
                    "manufacturer": row.get("manufacturer"),
                    "manufacturer_slug": row.get("manufacturer_slug"),
                },
                manufacturer_authoritative=True,
            )

    def _index(model, slug_values, name_values):
        objs = []
        for batch in _chunks(list(slug_values)):
            objs.extend(model.objects.filter(slug__in=batch))
        for batch in _chunks(list(name_values)):
            objs.extend(model.objects.filter(name__in=batch))
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
    dts = []
    for batch in _chunks(list(dt_slug_v)):
        dts.extend(
            DeviceType.objects.filter(slug__in=batch).select_related("manufacturer")
        )
    for batch in _chunks(list(dt_model_v)):
        dts.extend(
            DeviceType.objects.filter(model__in=batch).select_related("manufacturer")
        )
    dt_by_slug = {}
    dt_by_model = {}
    for device_type in dts:
        manufacturer_keys = {
            device_type.manufacturer.slug,
            device_type.manufacturer.name,
        }
        for manufacturer_key in manufacturer_keys:
            if device_type.slug:
                dt_by_slug[(manufacturer_key, device_type.slug)] = device_type
            if device_type.model:
                dt_by_model[(manufacturer_key, device_type.model)] = device_type

    existing_by_name = defaultdict(list)
    existing_device_names = {r["name"] for r in rows if r.get("name")}
    for batch in _chunks(list(existing_device_names)):
        for device in Device.objects.filter(name__in=batch):
            existing_by_name[device.name].append(device)

    create_objects = {}
    update_objects = {}
    identity_devices = {}
    row_devices = []
    row_outcomes = []
    branch_active = _branch_is_active()
    snapshotted_device_ids = set()

    def _snapshot_device_once(device):
        if (
            branch_active
            and getattr(device, "pk", None) is not None
            and device.pk not in snapshotted_device_ids
        ):
            device.snapshot()
            snapshotted_device_ids.add(device.pk)

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
        device_type = None
        manufacturer_keys = (
            row.get("manufacturer_slug"),
            row.get("manufacturer"),
        )
        for manufacturer_key in manufacturer_keys:
            if not manufacturer_key:
                continue
            device_type = dt_by_slug.get(
                (manufacturer_key, row.get("device_type_slug"))
            ) or dt_by_model.get((manufacturer_key, row.get("device_type")))
            if device_type is not None:
                break
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
            "status": row["status"],
        }
        if row.get("serial") not in (None, ""):
            defaults["serial"] = row["serial"]
        try:
            matching = [
                device
                for device in existing_by_name.get(row["name"], [])
                if device.site_id == site.pk
            ]
            if len(matching) > 1:
                raise ForwardSearchError(
                    f"Multiple NetBox devices named `{row['name']}` exist in "
                    f"site `{site.name}`.",
                    model_string="dcim.device",
                    context={"name": row["name"], "site": site.name},
                    data=row,
                )
            existing = matching[0] if matching else None
            if existing is None:
                create_key = (row["name"], site.pk)
                device = create_objects.get(create_key)
                if device is None:
                    device = Device(**defaults)
                    device.full_clean(validate_unique=False, validate_constraints=False)
                    create_objects[create_key] = device
                outcome = ["applied"]
                row_outcomes.append(outcome)
                row_devices.append((row, device, outcome))
                continue
            identity_devices[existing.pk] = existing
            changed_values = []
            for field, value in defaults.items():
                if _device_field_differs(existing, field, value):
                    changed_values.append((field, value))
            if not changed_values:
                outcome = ["unchanged"]
                row_outcomes.append(outcome)
                row_devices.append((row, existing, outcome))
                continue
            _snapshot_device_once(existing)
            for field, value in changed_values:
                setattr(existing, field, value)
            existing.full_clean()
            if getattr(existing, "pk", None):
                update_objects[existing.pk] = existing
            outcome = ["applied"]
            row_outcomes.append(outcome)
            row_devices.append((row, existing, outcome))
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

    using = _active_write_alias()

    def _ensure_scope_tags_in_transaction():
        """Resolve scope tags without framework signal writes on another alias."""
        if not scope_tags_enabled or not scope_names:
            return {}
        created = []
        updated = []
        resolved = {}
        for name in sorted(scope_names):
            slug = slugify(name) or slugify(name.replace(".", "-"))
            if not slug:
                resolved[name] = None
                continue
            matches = list(
                Tag.objects.using(using)
                .filter(Q(name=name) | Q(slug=slug))
                .order_by("pk")[:2]
            )
            if len(matches) > 1:
                raise ForwardSearchError(
                    f"Multiple NetBox tags match scope tag `{name}` / `{slug}`.",
                    model_string="extras.taggeditem",
                    context={"name": name, "slug": slug},
                )
            if not matches:
                tag = Tag(name=name, slug=slug, color="9e9e9e")
                tag.full_clean(validate_unique=False, validate_constraints=False)
                created.append(tag)
                resolved[name] = tag
                continue
            tag = matches[0]
            changed = False
            for field_name, value in (
                ("name", name),
                ("slug", slug),
                ("color", "9e9e9e"),
            ):
                if getattr(tag, field_name) == value:
                    continue
                if branch_active and not changed:
                    tag.snapshot()
                setattr(tag, field_name, value)
                changed = True
            if changed:
                tag.full_clean(validate_unique=False, validate_constraints=False)
                updated.append(tag)
            resolved[name] = tag
        if created:
            Tag.objects.using(using).bulk_create(created, batch_size=1000)
        if updated:
            Tag.objects.using(using).bulk_update(
                updated,
                fields=["name", "slug", "color"],
                batch_size=1000,
            )
        if branch_active and (created or updated):
            emit_branch_object_changes(created, updated)

        cache = getattr(runner, "_scope_tag_objs", None)
        if not isinstance(cache, dict):
            cache = runner._scope_tag_objs = {}
        cache.update(resolved)
        slug_cache = getattr(runner, "_tag_by_slug_cache", None)
        name_cache = getattr(runner, "_tag_by_name_cache", None)
        for tag in resolved.values():
            if tag is None:
                continue
            if isinstance(slug_cache, dict):
                slug_cache[tag.slug] = tag
            if isinstance(name_cache, dict):
                name_cache[tag.name] = tag
        return {name: tag for name, tag in resolved.items() if tag is not None}

    with transaction.atomic(using=using):
        try:
            with transaction.atomic(using=using):
                # Tag creation is part of the same atomic branch mutation as
                # Device, TaggedItem, and ObjectChange writes. An assignment or
                # evidence failure must not leave an orphan managed Tag change.
                scope_tags_by_name = _ensure_scope_tags_in_transaction()
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

                if scope_tags_enabled and scope_tags_by_name:
                    device_content_type = runner._content_type_for(Device)
                    created_device_ids = {
                        device.pk for device in create_objects.values()
                    }
                    wanted = []
                    for row, device, outcome in row_devices:
                        for name in runner._scope_matched_tags.get(row["name"], []):
                            tag = scope_tags_by_name.get(name)
                            if tag is not None:
                                wanted.append((device, tag, outcome))
                    device_ids = {device.pk for device, _, _ in wanted}
                    tag_ids = {tag.pk for _, tag, _ in wanted}
                    existing_assignments = set()
                    if device_ids and tag_ids:
                        existing_assignments = set(
                            TaggedItem.objects.using(using)
                            .filter(
                                content_type=device_content_type,
                                object_id__in=device_ids,
                                tag_id__in=tag_ids,
                            )
                            .values_list("object_id", "tag_id")
                        )
                    assignment_objects = []
                    seen_assignments = set(existing_assignments)
                    for device, tag, outcome in wanted:
                        key = (device.pk, tag.pk)
                        if key in seen_assignments:
                            continue
                        if device.pk not in created_device_ids:
                            _snapshot_device_once(device)
                            update_objects[device.pk] = device
                        assignment_objects.append(
                            TaggedItem(
                                content_type=device_content_type,
                                object_id=device.pk,
                                tag=tag,
                            )
                        )
                        runner._device_tag_ids_cache.setdefault(device.pk, set()).add(
                            tag.pk
                        )
                        seen_assignments.add(key)
                        outcome[0] = "applied"
                    if assignment_objects:
                        TaggedItem.objects.using(using).bulk_create(
                            assignment_objects,
                            batch_size=1000,
                        )
        except IntegrityError as exc:
            if branch_active:
                raise
            runner.logger.log_warning(
                f"Bulk write for dcim.device hit a constraint error ({exc}); retrying "
                "row-by-row to isolate the offending row(s).",
                obj=runner.sync,
            )
            _isolate_bulk_objects(
                Device,
                list(create_objects.values()),
                "create",
                runner,
                "dcim.device",
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

    for device in list(create_objects.values()) + list(identity_devices.values()):
        if getattr(device, "pk", None):
            record_device_identity_candidate(runner, device)

    for (outcome,) in row_outcomes:
        runner.logger.increment_statistics("dcim.device", outcome=outcome)
        if outcome == "applied":
            runner.events_clearer.increment()

    runner.events_clearer.clear()
    return True


def bulk_orm_apply_ipaddress(runner, rows: list[dict[str, Any]]):
    """Batched apply for ipam.ipaddress with adapter-parity semantics.

    Mirrors ``apply_ipam_ipaddress`` (sync_ipam.py) row-for-row — device and
    interface resolution, dependency skip/fail, network-id/broadcast skips, VRF
    ensure, and the null-VRF net_host vs (address, vrf) coalesce — but collects
    resolved IPAddress objects and writes them with bulk_create/bulk_update at
    the end instead of saving per row. The apply-engine decision limits this
    path to the parity-tested model set.
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
    interface_pairs = {
        (row.get("device"), row.get("interface"))
        for row in rows
        if row.get("device") and row.get("interface")
    }
    devices_by_name = {}
    for batch in _chunks(list(device_names)):
        for device in Device.objects.filter(name__in=batch):
            devices_by_name[device.name] = device
    interfaces_by_key = {}
    # Pair chunking avoids a device/interface Cartesian product across batches.
    for batch in _chunks(list(interface_pairs)):
        query = _device_scoped_name_query(batch)
        for interface in Interface.objects.select_related("device").filter(query):
            interfaces_by_key[(interface.device.name, interface.name)] = interface

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
        for batch in _chunks(list(host_ips)):
            host_filter = reduce(
                operator.or_, (Q(address__net_host=host) for host in batch)
            )
            # order_by("pk") + first-wins so a duplicate global IP (same host,
            # same VRF) resolves to the lowest pk deterministically.
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

        changed_values = []
        if str(ip.address) != str(row["address"]):
            changed_values.append(("address", row["address"]))
        if ip.vrf_id != vrf_id:
            changed_values.append(("vrf", vrf))
        if ip.status != row["status"]:
            changed_values.append(("status", row["status"]))
        if ip.assigned_object_type_id != interface_ct.pk:
            changed_values.append(("assigned_object_type", interface_ct))
        if ip.assigned_object_id != interface.pk:
            changed_values.append(("assigned_object_id", interface.pk))
        if not changed_values:
            runner.logger.increment_statistics("ipam.ipaddress", outcome="unchanged")
            continue
        # `ip` may be a not-yet-saved in-memory create from an earlier duplicate
        # row. Snapshot only a persisted object that will actually change.
        if branch_active and ip.pk is not None:
            ip.snapshot()
        for field, value in changed_values:
            setattr(ip, field, value)
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

    using = _active_write_alias()
    with transaction.atomic(using=using):
        try:
            with transaction.atomic(using=using):
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
            if branch_active:
                raise
            runner.logger.log_warning(
                f"Bulk write for ipam.ipaddress hit a constraint error ({exc}); "
                "retrying row-by-row to isolate the offending row(s).",
                obj=runner.sync,
            )
            _isolate_bulk_objects(
                IPAddress,
                list(create_objects.values()),
                "create",
                runner,
                "ipam.ipaddress",
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
    from django.db import IntegrityError
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

    existing_vcs = {}
    for batch in _chunks(list(vc_names)):
        for vc in VirtualChassis.objects.filter(name__in=batch):
            existing_vcs[vc.name] = vc
    existing_devices = {}
    for batch in _chunks(list(device_names)):
        for device in Device.objects.filter(name__in=batch):
            existing_devices[device.name] = device
    create_vcs = []
    update_vcs = []
    vcs_by_name = dict(existing_vcs)

    for row, vc_name in usable_rows:
        domain = row.get("vc_domain", row.get("domain", ""))
        vc = vcs_by_name.get(vc_name)
        if vc is None:
            vc = VirtualChassis(name=vc_name, domain=domain)
            try:
                vc.full_clean()
            except Exception as exc:  # noqa: BLE001 - isolate one row
                runner._record_issue(
                    "dcim.virtualchassis",
                    "Virtual-chassis row failed validation; isolated so the "
                    f"shard continues: {exc}",
                    row,
                    exception=exc,
                )
                runner.logger.increment_statistics(
                    "dcim.virtualchassis", outcome="failed"
                )
                continue
            create_vcs.append(vc)
            vcs_by_name[vc_name] = vc
            continue
        if vc.domain != domain:
            previous_domain = vc.domain
            vc.domain = domain
            try:
                vc.full_clean()
            except Exception as exc:  # noqa: BLE001 - isolate one row
                vc.domain = previous_domain
                if vc in create_vcs:
                    create_vcs.remove(vc)
                    vcs_by_name.pop(vc_name, None)
                if vc in update_vcs:
                    update_vcs.remove(vc)
                runner._record_issue(
                    "dcim.virtualchassis",
                    "Virtual-chassis row failed validation; isolated so the "
                    f"shard continues: {exc}",
                    row,
                    exception=exc,
                )
                runner.logger.increment_statistics(
                    "dcim.virtualchassis", outcome="failed"
                )
                continue
            update_vcs.append(vc)

    try:
        with transaction.atomic():
            if create_vcs:
                VirtualChassis.objects.bulk_create(create_vcs, batch_size=1000)
            if update_vcs:
                VirtualChassis.objects.bulk_update(
                    update_vcs,
                    fields=["domain"],
                    batch_size=1000,
                )
    except IntegrityError as exc:
        runner.logger.log_warning(
            f"Bulk write for dcim.virtualchassis hit a constraint error ({exc}); "
            "retrying row-by-row to isolate the offending row(s).",
            obj=runner.sync,
        )
        _isolate_bulk_objects(
            VirtualChassis,
            create_vcs,
            "create",
            runner,
            "dcim.virtualchassis",
        )
        _isolate_bulk_objects(
            VirtualChassis,
            update_vcs,
            "update",
            runner,
            "dcim.virtualchassis",
            fields=["domain"],
        )

    vcs_by_name = {}
    for batch in _chunks(list(vc_names)):
        for vc in VirtualChassis.objects.filter(name__in=batch):
            vcs_by_name[vc.name] = vc
    occupied_positions = {}
    virtual_chassis_ids = [
        vc.pk for vc in vcs_by_name.values() if getattr(vc, "pk", None)
    ]
    for batch in _chunks(virtual_chassis_ids):
        for device in Device.objects.filter(
            virtual_chassis_id__in=batch,
            vc_position__isnull=False,
        ):
            occupied_positions[(device.virtual_chassis_id, device.vc_position)] = device
    devices_to_update = []

    for row, vc_name in usable_rows:
        vc = vcs_by_name.get(vc_name)
        if vc is None:
            continue
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

    try:
        with transaction.atomic():
            if devices_to_update:
                Device.objects.bulk_update(
                    devices_to_update,
                    fields=["virtual_chassis", "vc_position"],
                    batch_size=1000,
                )
    except IntegrityError as exc:
        runner.logger.log_warning(
            f"Bulk device membership write for dcim.virtualchassis hit a "
            f"constraint error ({exc}); retrying row-by-row to isolate the "
            "offending row(s).",
            obj=runner.sync,
        )
        _isolate_bulk_objects(
            Device,
            devices_to_update,
            "update",
            runner,
            "dcim.virtualchassis",
            fields=["virtual_chassis", "vc_position"],
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

        existing_objects = {}
        if any(lookup_values.values()):
            # Single combined-OR prefetch (one SELECT, reused across every row
            # in the shard). This simple-model path handles bounded reference
            # tables, so the lookup-cache-reuse test intentionally pins one
            # unchunked query.
            from django.db.models import Q

            query = Q()
            for field_name, values in lookup_values.items():
                if values:
                    query |= Q(**{f"{field_name}__in": values})
            for obj in model.objects.filter(query).order_by("pk"):
                existing_objects[obj.pk] = obj

        lookup_cache = {lookup_set: {} for lookup_set in lookup_sets}
        for obj in sorted(existing_objects.values(), key=lambda item: item.pk):
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
                pre_row_events = runner.events_clearer.snapshot()
                try:
                    with transaction.atomic():
                        obj.full_clean()
                        obj.save()
                except Exception as exc:  # noqa: BLE001 - isolate one row
                    runner.events_clearer.restore(pre_row_events)
                    runner._record_issue(
                        model_string,
                        "Tree-model row failed; isolated so the shard "
                        f"continues: {exc}",
                        values,
                        exception=exc,
                    )
                    runner.logger.increment_statistics(model_string, outcome="failed")
                    continue
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

            pre_row_events = runner.events_clearer.snapshot()
            try:
                with transaction.atomic():
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
            except Exception as exc:  # noqa: BLE001 - isolate one row
                runner.events_clearer.restore(pre_row_events)
                existing.refresh_from_db()
                runner._record_issue(
                    model_string,
                    f"Tree-model row failed; isolated so the shard continues: {exc}",
                    values,
                    exception=exc,
                )
                runner.logger.increment_statistics(model_string, outcome="failed")
                continue
            if changed:
                runner.logger.increment_statistics(model_string, outcome="applied")
                runner.events_clearer.increment()
                continue
            runner.logger.increment_statistics(model_string, outcome="unchanged")
    runner.events_clearer.clear()
    return True
