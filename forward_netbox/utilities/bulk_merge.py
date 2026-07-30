# Bulk branch merge: collapse a branch's ObjectChanges to net per-object state,
# then apply CREATE batches via bulk_create instead of replaying every change
# one-by-one. This is the core of the large-dataset ingest redesign: a bootstrap
# sync of ~1M creates merges as bulk_create sub-batches instead of ~1M
# individual ObjectChange.apply() saves.
#
# Correctness uses the framework's SquashMergeStrategy collapse and dependency
# graph plus this plugin's explicit 1.1.1 cycle splitting:
#   - _collapse_changes: N changes/object -> one net CollapsedChange
#   - local cycle splitting + _build_fk_dependency_graph: FK ordering graph
# Only the *application* is changed (bulk for create-able models, plus bounded
# Prefix UPDATE/DELETE batches for the Forward-owned fields; other updates,
# deletes, and MPTT/tree models retain ObjectChange.apply()), AND two of the
# framework's per-object/quadratic steps are replaced with scale-safe variants
# because the single-branch design removes the 10k-per-branch cap that used to
# hide them (see scale-validation findings):
#   - ordering: the framework topological sort is O(V^2) (rescans all remaining
#     nodes per layer and discards across all nodes per processed key) and hangs
#     on a single large model batch (e.g. 539k interfaces) even with no edges.
#     _order_collapsed_changes_fast does a real O((V+E) log V) Kahn sort.
#   - skip-missing: the framework issues one existence query per UPDATE (N+1);
#     _skip_updates_missing_in_main_batched groups by model with chunked pk__in.
#   - flush: CREATE batches are sub-batched by count so RAM, transaction size,
#     lock duration, and the existence pk__in are all bounded, and committed
#     sub-batches act as resume checkpoints.
import heapq
import logging
import time
from collections import Counter
from collections import defaultdict

from django.contrib.contenttypes.fields import GenericForeignKey
from django.contrib.contenttypes.models import ContentType
from django.db import DEFAULT_DB_ALIAS
from django.db import models
from django.db import OperationalError
from django.db import transaction
from django.db.models import prefetch_related_objects
from django.db.models import Q
from django.db.models.deletion import ProtectedError
from mptt.models import MPTTModel
from netbox_branching.merge_strategies.squash import ActionType
from netbox_branching.merge_strategies.squash import CollapsedChange
from netbox_branching.merge_strategies.squash import SquashMergeStrategy
from netbox_branching.models import ChangeDiff
from netbox_branching.signals import squash_dependency_graph_built
from netbox_branching.utilities import _FILE_NOT_FOUND_EXCEPTIONS
from netbox_branching.utilities import deactivate_branch
from rq.timeouts import JobTimeoutException
from utilities.serialization import deserialize_object

from .bulk_delete import lock_tables_for_writes
from .merge_profiling import profile_scope

# SQL chunk size handed to bulk_create (splits the multi-row INSERT).
BULK_MERGE_BATCH_SIZE = 1000
# Max collapsed CREATEs held in RAM / committed per sub-batch transaction. Bounds
# peak memory, transaction/lock duration, and the existence pk__in list, and
# gives per-sub-batch resume checkpoints. Independent of model row count.
BULK_MERGE_FLUSH_THRESHOLD = 5000
# Chunk size for streaming the raw ObjectChange queryset into collapse so the
# full ~1M-row result set is never materialized at once.
BULK_MERGE_INPUT_CHUNK_SIZE = 2000
RELATIONSHIP_LOCK_RETRY_ATTEMPTS = 20
RELATIONSHIP_LOCK_RETRY_MAX_DELAY_SECONDS = 0.5

# NetBox serializes these read-only relationship summaries into ObjectChange
# payloads. They can legitimately change when a later branch-owned child row is
# applied, so they are not scalar provenance. The child objects retain their own
# branch lineage checks; writable scalar and custom-field values still compare
# fail-closed below.
RESUME_DERIVED_AUDIT_FIELDS = {
    "dcim.cable": frozenset({"a_terminations", "b_terminations"}),
    "dcim.device": frozenset({"inventory_item_count", "module_bay_count"}),
}

_ACTION_PRIORITY = {
    ActionType.DELETE: 0,
    ActionType.UPDATE: 1,
    ActionType.CREATE: 2,
    ActionType.SKIP: 3,
}


def _merge_priority(collapsed_change):
    # Materialize branch-created Tags before object updates can resolve tag
    # names through django-taggit and create a different main-side primary key.
    if (
        collapsed_change.final_action == ActionType.CREATE
        and getattr(
            getattr(getattr(collapsed_change, "model_class", None), "_meta", None),
            "label_lower",
            "",
        )
        == "extras.tag"
    ):
        return 0.5
    return _ACTION_PRIORITY.get(collapsed_change.final_action, 99)


def _tag_identities_conflict(left, right):
    left = left or {}
    right = right or {}
    left_name = str(left.get("name") or "")
    right_name = str(right.get("name") or "")
    left_slug = str(left.get("slug") or "")
    right_slug = str(right.get("slug") or "")
    return bool(
        (left_name and left_name == right_name)
        or (left_slug and left_slug == right_slug)
    )


def _serialized_tag_identities(data):
    identities = set()
    for value in (data or {}).get("tags") or []:
        if isinstance(value, dict):
            for key in ("name", "slug"):
                if value.get(key):
                    identities.add(str(value[key]))
            continue
        if value is not None:
            identities.add(str(value))
    return identities


def _add_tag_identity_release_dependencies(collapsed_changes):
    """Order Tag identity release, creation, then tag-bearing object changes."""
    tag_changes = [
        change
        for change in collapsed_changes.values()
        if getattr(
            getattr(getattr(change, "model_class", None), "_meta", None),
            "label_lower",
            "",
        )
        == "extras.tag"
    ]
    creates = [
        change for change in tag_changes if change.final_action == ActionType.CREATE
    ]
    releases = [
        change
        for change in tag_changes
        if change.final_action in {ActionType.UPDATE, ActionType.DELETE}
    ]
    for create in creates:
        for release in releases:
            if _tag_identities_conflict(
                create.postchange_data, release.prechange_data
            ) and not _tag_identities_conflict(
                create.postchange_data, release.postchange_data
            ):
                create.depends_on.add(release.key)
                release.depended_by.add(create.key)
        create_data = create.postchange_data or {}
        create_identities = {
            str(create_data.get(key))
            for key in ("name", "slug")
            if create_data.get(key)
        }
        for tagged_change in collapsed_changes.values():
            if tagged_change is create or not create_identities.intersection(
                _serialized_tag_identities(tagged_change.postchange_data)
            ):
                continue
            tagged_change.depends_on.add(create.key)
            create.depended_by.add(tagged_change.key)


# Deferred self-referential nullable FKs: NetBox creates the parent with these
# NULL, then sets them in a SECOND save once the target exists (e.g.
# ipam/forms/bulk_import.py sets device.primary_ip4 after the IP is created and
# assigned). The squash collapse folds that trailing UPDATE back into the parent
# CREATE, so the CREATE declares a hard FK to an in-branch IP whose interface FKs
# back to the device — a 3-node create cycle (device -> ipaddress -> interface ->
# device) that the framework's 2-node _split_bidirectional_cycles cannot break,
# wedging the topological sort. Splitting these back out of the CREATE (NULL on
# create + a trailing UPDATE) restores the acyclic create order NetBox itself
# relies on. Keyed by "<app_label>.<model>".
_DEFERRED_CREATE_FK_FIELDS = {
    "dcim.device": ("primary_ip4", "primary_ip6", "oob_ip"),
    "virtualization.virtualmachine": ("primary_ip4", "primary_ip6"),
    "dcim.virtualchassis": ("master",),
    # Interface self-references (LAG member -> parent, bridge, sub-interface
    # parent) point at ANOTHER dcim.interface. During a bulk-create flush each
    # row is full_clean()'d before the batch commits, and Interface.clean()
    # dereferences self.lag/self.bridge/self.parent — so a member whose parent is
    # in the same uncommitted batch raises Interface.DoesNotExist. Defer these to
    # a trailing UPDATE that runs after the parent row is committed.
    "dcim.interface": ("lag", "bridge", "parent"),
}

logger = logging.getLogger("forward_netbox.bulk_merge")


def _serialized_fk_id(value):
    if isinstance(value, dict):
        return value.get("id", value.get("pk"))
    return value


def _affected_prefix_vrf_ids(collapsed_changes):
    vrf_ids = set()
    for collapsed in collapsed_changes:
        if collapsed.model_class._meta.label_lower != "ipam.prefix":
            continue
        for data in (collapsed.prechange_data, collapsed.postchange_data):
            if data is not None and "vrf" in data:
                vrf_ids.add(_serialized_fk_id(data.get("vrf")))
    return vrf_ids


def _rebuild_main_prefix_hierarchies(vrf_ids):
    if not vrf_ids:
        return
    from ipam.utils import rebuild_prefixes

    # Merge must repair the authoritative main hierarchy even if a caller has a
    # branch context active. Prefix signals cannot do this for bulk-created rows.
    with deactivate_branch(), transaction.atomic():
        for vrf_id in sorted(
            vrf_ids, key=lambda value: (value is not None, value or 0)
        ):
            rebuild_prefixes(vrf_id)


def _sync_global_change_diffs_impl(object_changes, action):
    """Mirror Branching's global ObjectChange receiver after a bulk audit write."""
    if not object_changes:
        return
    from core.choices import ObjectChangeActionChoices
    from django.utils import timezone
    from netbox_branching.choices import BranchStatusChoices

    object_type = object_changes[0].changed_object_type
    current_by_id = {
        change.changed_object_id: change.postchange_data_clean or None
        for change in object_changes
    }
    diffs = list(
        ChangeDiff.objects.filter(
            object_type=object_type,
            object_id__in=current_by_id,
            branch__status=BranchStatusChoices.READY,
        )
    )
    now = timezone.now()
    for diff in diffs:
        diff.current = current_by_id[diff.object_id]
        diff.last_updated = now
        if (
            action == ObjectChangeActionChoices.ACTION_DELETE
            and diff.action == ObjectChangeActionChoices.ACTION_UPDATE
        ):
            diff._update_conflicts()
    if diffs:
        ChangeDiff.objects.bulk_update(
            diffs,
            fields=["current", "last_updated", "conflicts"],
            batch_size=BULK_MERGE_BATCH_SIZE,
        )


def _sync_global_change_diffs(object_changes, action):
    model = (
        object_changes[0].changed_object_type.model_class()._meta.label_lower
        if object_changes
        else ""
    )
    with profile_scope(
        "changediff_resolution",
        model=model,
        # This is Forward's set-based equivalent of Branching's global
        # ObjectChange receiver. The ChangeDiff contract is upstream-owned,
        # but this implementation and its query shape are actionable here.
        owner="ours",
        rows=len(object_changes),
    ):
        return _sync_global_change_diffs_impl(object_changes, action)


def _emit_main_object_changes_impl(
    objects,
    action,
    request,
    branch,
    *,
    message=None,
    allow_unchanged=False,
):
    """Write authoritative main audits and their Branching lineage."""
    objects = list(objects)
    if not objects:
        return
    if request is None or getattr(request, "user", None) is None:
        raise RuntimeError("Bulk merge audit requires the invoking user.")
    if branch is None or getattr(branch, "pk", None) is None:
        raise RuntimeError("Bulk merge audit requires the source branch.")

    from core.choices import ObjectChangeActionChoices
    from core.models import ObjectChange
    from django.db.models import prefetch_related_objects
    from netbox_branching.models import AppliedChange

    from .apply_engine_bulk import _serializer_prefetch_fields

    for start in range(0, len(objects), BULK_MERGE_BATCH_SIZE):
        chunk = objects[start : start + BULK_MERGE_BATCH_SIZE]
        prefetch_fields = _serializer_prefetch_fields(type(chunk[0]))
        if prefetch_fields:
            prefetch_related_objects(chunk, *prefetch_fields)
        object_changes = []
        for obj in chunk:
            if action == ObjectChangeActionChoices.ACTION_DELETE and not getattr(
                obj, "_prechange_snapshot", None
            ):
                obj.snapshot()
            change = obj.to_objectchange(action)
            if change is None:
                continue
            if not allow_unchanged and _serialized_audit_values_equal(
                change.prechange_data, change.postchange_data
            ):
                continue
            if message is not None:
                change.message = message
            change.user = request.user
            change.user_name = getattr(request.user, "username", "") or ""
            change.request_id = getattr(request, "id", None)
            object_changes.append(change)
        if not object_changes:
            continue
        with transaction.atomic():
            ObjectChange.objects.bulk_create(
                object_changes,
                batch_size=BULK_MERGE_BATCH_SIZE,
            )
            AppliedChange.objects.bulk_create(
                [
                    AppliedChange(change=change, branch=branch)
                    for change in object_changes
                ],
                batch_size=BULK_MERGE_BATCH_SIZE,
            )
            _sync_global_change_diffs(object_changes, action)


def _emit_main_object_changes(
    objects,
    action,
    request,
    branch,
    *,
    message=None,
    allow_unchanged=False,
):
    objects = list(objects)
    model = objects[0]._meta.label_lower if objects else ""
    with profile_scope(
        "audit_and_lineage",
        model=model,
        owner="ours",
        rows=len(objects),
    ):
        return _emit_main_object_changes_impl(
            objects,
            action,
            request,
            branch,
            message=message,
            allow_unchanged=allow_unchanged,
        )


class _PrefixDeleteNeedsIsolation(Exception):
    pass


class _BulkMergeAuditError(Exception):
    pass


def _defer_self_referential_create_fks(to_process, change_logger):
    """Split deferred self-referential nullable FKs out of collapsed CREATEs.

    Mirrors the framework's ``_split_bidirectional_cycles`` idiom (NULL the FK on
    the CREATE, re-apply it as a synthetic trailing UPDATE carrying the full
    original data so ``get_merge_data`` emits only the deferred-FK delta) but for
    the known deferred set rather than only direct 2-node reciprocal cycles. Run
    BEFORE ``_build_fk_dependency_graph`` so the cycle-forming CREATE->target edge
    is never recorded; the synthetic UPDATE then orders naturally after both the
    parent CREATE and the target CREATE.
    """
    for key, collapsed in list(to_process.items()):
        # Cheap model-key gate first, before touching the change's attributes (the
        # ordering-complexity tests stub changes as keyless SimpleNamespaces). Real
        # collapsed keys are (model_label, pk) — require len>=2 so key[0]/key[1] below
        # are always safe.
        if not isinstance(key, tuple) or len(key) < 2:
            continue
        fields = _DEFERRED_CREATE_FK_FIELDS.get(key[0])
        if not fields:
            continue
        if collapsed.final_action != ActionType.CREATE:
            continue
        postchange = collapsed.postchange_data
        if not postchange:
            continue
        deferred = [f for f in fields if postchange.get(f)]
        if not deferred:
            continue
        original_postchange = dict(postchange)
        for field_name in deferred:
            postchange[field_name] = None
        update_key = (key[0], key[1], "defer_self_ref_fk")
        update_collapsed = CollapsedChange(update_key, collapsed.model_class)
        update_collapsed.change_count = 1
        update_collapsed.final_action = ActionType.UPDATE
        # prechange = the now-NULLed create state, postchange = full original with
        # the FK set, so the pre->post merge delta is exactly the deferred fields.
        update_collapsed.prechange_data = dict(collapsed.postchange_data)
        update_collapsed.postchange_data = original_postchange
        update_collapsed.last_change = collapsed.last_change
        update_collapsed._forward_original_change_key = key
        # Order the UPDATE strictly after the parent CREATE. _build_fk_dependency_graph
        # only links an UPDATE to a CREATE through an explicit FK in the data, and a
        # device has no FK to itself — without this the UPDATE would otherwise rely on
        # the transitive chain (update -> ip -> interface -> device), which holds only
        # when the IP is on one of the device's own interfaces. Make it unconditional.
        update_collapsed.depends_on.add(key)
        collapsed.depended_by.add(update_key)
        to_process[update_key] = update_collapsed
        change_logger.debug(
            "Deferred %s on %s create to a trailing UPDATE to break a create cycle.",
            deferred,
            key,
        )


def _is_deferred_fk_update(collapsed_change):
    key = getattr(collapsed_change, "key", None)
    return (
        isinstance(key, tuple)
        and len(key) == 3
        and key[2] == "defer_self_ref_fk"
        and key[0] in _DEFERRED_CREATE_FK_FIELDS
        and collapsed_change.final_action == ActionType.UPDATE
    )


def _create_has_fk_to(collapsed, target_model_class, target_obj_id):
    """True when ``collapsed``'s postchange data holds an FK to the target object.

    NetBox Branching 1.1.1 does not expose the pair-cycle helper used by the
    plugin's custom sorter, so the exact runtime contract owns this concrete-FK
    and GenericForeignKey check.
    """
    if not collapsed.postchange_data:
        return False
    for field in collapsed.model_class._meta.get_fields():
        if not isinstance(field, models.ForeignKey):
            continue
        fk_value = collapsed.postchange_data.get(field.name)
        if not fk_value:
            continue
        if field.related_model == target_model_class and fk_value == target_obj_id:
            return True
    target_ct_id = ContentType.objects.get_for_model(target_model_class).pk
    for field in collapsed.model_class._meta.private_fields:
        if not isinstance(field, GenericForeignKey):
            continue
        # ObjectChange data may store the CT FK as either the field name or
        # its *_id column.
        ct_value = collapsed.postchange_data.get(
            field.ct_field
        ) or collapsed.postchange_data.get(f"{field.ct_field}_id")
        fk_value = collapsed.postchange_data.get(field.fk_field)
        if ct_value == target_ct_id and fk_value == target_obj_id:
            return True
    return False


def _split_bidirectional_create_cycles(collapsed_changes, change_logger):
    """Split CREATE pairs joined by bidirectional FKs (A -> B and B -> A).

    The exact 1.1.1 runtime breaks these inside its own ordering pass, which this
    module replaces with an O((V+E) log V) sorter. This pre-pass NULLs one
    nullable FK on CREATE and appends a synthetic UPDATE that restores it after
    both rows exist.
    """
    creates = {
        key: c
        for key, c in collapsed_changes.items()
        if c.final_action == ActionType.CREATE
    }
    for key_a, create_a in list(creates.items()):
        if not getattr(create_a, "postchange_data", None):
            continue
        for field in create_a.model_class._meta.get_fields():
            if not (isinstance(field, models.ForeignKey) and field.null):
                continue
            fk_value = create_a.postchange_data.get(field.name)
            if not fk_value:
                continue
            related_model = field.related_model
            target_ct = ContentType.objects.get_for_model(related_model)
            app_label, model = target_ct.natural_key()
            key_b = (f"{app_label}.{model}", fk_value)
            create_b = creates.get(key_b)
            if create_b is None:
                continue
            if not _create_has_fk_to(create_b, create_a.model_class, key_a[1]):
                continue
            change_logger.info(
                "  Detected bidirectional cycle: %s:%s <-> %s:%s (via %s)",
                create_a.model_class.__name__,
                key_a[1],
                create_b.model_class.__name__,
                key_b[1],
                field.name,
            )
            original_postchange = dict(create_a.postchange_data)
            create_a.postchange_data[field.name] = None
            update_key = (key_a[0], key_a[1], f"update_{field.name}")
            update_collapsed = CollapsedChange(update_key, create_a.model_class)
            update_collapsed.change_count = 1
            update_collapsed.final_action = ActionType.UPDATE
            update_collapsed.prechange_data = dict(create_a.postchange_data)
            update_collapsed.postchange_data = original_postchange
            update_collapsed.last_change = create_a.last_change
            update_collapsed._forward_original_change_key = key_a
            collapsed_changes[update_key] = update_collapsed
            break


def _is_bulk_safe(model_class) -> bool:
    """A model may be bulk_create'd on merge only if it is not an MPTT tree.

    MPTT models (region, site/tenant/contact groups, etc.) must save per
    object so the tree fields recompute against the destination tree; bulk_create
    bypasses that and corrupts the hierarchy.
    """
    return not issubclass(model_class, MPTTModel)


def _full_clean_fast(instance, change_logger):
    """full_clean without the per-row validate_unique/validate_constraints DB
    queries (mirrors the framework's full_clean_with_file_check otherwise).

    Database constraints still enforce uniqueness for new rows, while resumed
    rows are locked and converged by primary key. Field-level validation is kept.
    This removes the merge's dominant per-row cost at scale.
    """
    try:
        instance.full_clean(validate_unique=False, validate_constraints=False)
    except _FILE_NOT_FOUND_EXCEPTIONS as exc:  # pragma: no cover - file backends
        if hasattr(exc, "response"):
            status = exc.response.get("ResponseMetadata", {}).get("HTTPStatusCode")
            if status not in (403, 404):
                raise
        change_logger.warning(f"Ignoring missing file: {exc}")


def _deserialize(model_class, collapsed, change_logger):
    """Build a validated, unsaved instance from a collapsed CREATE change."""
    data = collapsed.postchange_data or {}
    pk = collapsed.key[1]
    deserialized = deserialize_object(model_class, data, pk=pk)
    _full_clean_fast(deserialized.object, change_logger)
    return deserialized


def _is_lock_not_available(exc):
    current = exc
    while current is not None:
        if getattr(current, "sqlstate", None) == "55P03":
            return True
        current = getattr(current, "__cause__", None)
    return False


def _relationship_lock_retry_delay(attempt):
    return min(
        0.05 * (2**attempt),
        RELATIONSHIP_LOCK_RETRY_MAX_DELAY_SECONDS,
    )


def _lock_relationship_writes(model_class, field_names):
    """Serialize writes to relevant M2M tables until commit."""
    through_tables = sorted(
        {
            descriptor.through._meta.db_table
            for field_name in field_names
            if (descriptor := getattr(model_class, field_name, None)) is not None
            and getattr(descriptor, "through", None) is not None
        }
    )
    lock_tables_for_writes(through_tables, using=DEFAULT_DB_ALIAS)


def _retry_row_lock(operation):
    for attempt in range(RELATIONSHIP_LOCK_RETRY_ATTEMPTS):
        try:
            return operation()
        except OperationalError as exc:
            if not _is_lock_not_available(exc) or (
                attempt + 1 == RELATIONSHIP_LOCK_RETRY_ATTEMPTS
            ):
                raise
            time.sleep(_relationship_lock_retry_delay(attempt))
    raise AssertionError("unreachable")


def _converge_existing_create(
    target,
    deserialized,
    source_data,
    change_logger,
    request,
    branch,
    *,
    require_branch_lineage=False,
    lineage_message=None,
    attest_if_unchanged=False,
    refuse_changes=False,
):
    """Apply a resumed/colliding CREATE with atomic main audit evidence."""

    def converge():
        with transaction.atomic():
            _lock_relationship_writes(
                target.__class__,
                (deserialized.m2m_data or {}).keys(),
            )
            locked_target = target.__class__.objects.select_for_update(nowait=True).get(
                pk=target.pk
            )
            if require_branch_lineage:
                _assert_existing_create_resume_provenance(
                    locked_target,
                    branch,
                    require_create=False,
                )
            return _converge_existing_create_locked(
                locked_target,
                deserialized,
                source_data,
                change_logger,
                request,
                branch,
                lineage_message=lineage_message,
                attest_if_unchanged=attest_if_unchanged,
                refuse_changes=refuse_changes,
            )

    return _retry_row_lock(converge)


def _converge_existing_create_locked(
    target,
    deserialized,
    source_data,
    change_logger,
    request,
    branch,
    *,
    lineage_message=None,
    attest_if_unchanged=False,
    refuse_changes=False,
):
    """Converge an existing CREATE whose destination row is already locked."""
    from core.choices import ObjectChangeActionChoices

    expected = deserialized.object
    scalar_changes = []
    for field in target._meta.concrete_fields:
        if (
            field.primary_key
            or getattr(field, "auto_created", False)
            or getattr(field, "auto_now", False)
            or getattr(field, "auto_now_add", False)
            or (field.name not in source_data and field.attname not in source_data)
        ):
            continue
        value = getattr(expected, field.attname)
        current_value = getattr(target, field.attname)
        values_match = (
            _serialized_audit_values_equal(current_value, value)
            if isinstance(field, models.JSONField)
            else current_value == value
        )
        if not values_match:
            scalar_changes.append((field, value))

    m2m_changes = []
    for accessor, values in (deserialized.m2m_data or {}).items():
        values = list(values)
        expected_ids = {getattr(value, "pk", value) for value in values}
        manager = getattr(target, accessor)
        current_ids = set(manager.values_list("pk", flat=True))
        if current_ids != expected_ids:
            m2m_changes.append((manager, values))

    if refuse_changes and (scalar_changes or m2m_changes):
        changed_fields = [field.name for field, _value in scalar_changes]
        changed_fields.extend(
            getattr(manager, "prefetch_cache_name", "relationships")
            for manager, _ in m2m_changes
        )
        raise _ExistingCreateResumeConflict(
            f"Existing {target._meta.label_lower}:{target.pk} is not an exact "
            "match for the unclaimed branch tag in fields: "
            + ", ".join(changed_fields[:10])
        )

    if not scalar_changes and not m2m_changes:
        if attest_if_unchanged:
            target.snapshot()
            _emit_main_object_changes(
                [target],
                ObjectChangeActionChoices.ACTION_UPDATE,
                request,
                branch,
                message=lineage_message,
                allow_unchanged=True,
            )
        return False

    target.snapshot()
    for field, value in scalar_changes:
        setattr(target, field.attname, value)
    _full_clean_fast(target, change_logger)
    if scalar_changes:
        target.save(update_fields=[field.name for field, _ in scalar_changes])
    for manager, values in m2m_changes:
        manager.set(values)
    _emit_main_object_changes(
        [target],
        ObjectChangeActionChoices.ACTION_UPDATE,
        request,
        branch,
        message=lineage_message,
    )
    return True


class _ExistingCreateResumeConflict(RuntimeError):
    pass


class _ApplyOneFailure:
    """Carry an isolated fallback exception to logical failure aggregation."""

    def __init__(self, exception):
        self.exception = exception


def _alternate_create_lineage_message(model_class, source_pk):
    return (
        "Forward NetBox alternate CREATE identity for "
        f"{model_class._meta.label_lower}:{source_pk}."
    )


def _alternate_tag_targets_by_source_pk(model_class, source_pks, branch):
    """Recover alternate Tag identities attested by an earlier merge attempt."""
    if getattr(model_class._meta, "label_lower", "") != "extras.tag":
        return {}

    from netbox_branching.models import AppliedChange

    messages_by_source_pk = {
        source_pk: _alternate_create_lineage_message(model_class, source_pk)
        for source_pk in source_pks
    }
    source_pk_by_message = {
        message: source_pk for source_pk, message in messages_by_source_pk.items()
    }
    targets = defaultdict(set)
    for message, target_pk in AppliedChange.objects.filter(
        branch=branch,
        change__changed_object_type=ContentType.objects.get_for_model(model_class),
        change__message__in=source_pk_by_message,
    ).values_list("change__message", "change__changed_object_id"):
        targets[source_pk_by_message[message]].add(target_pk)
    return targets


def _existing_create_resume_evidence(model_class, target_ids, branch):
    """Load branch CREATE and latest-audit evidence for object IDs in bulk."""
    from core.choices import ObjectChangeActionChoices
    from core.models import ObjectChange
    from netbox_branching.models import AppliedChange

    target_ids = list(target_ids)
    if not target_ids:
        return {}
    object_type = ContentType.objects.get_for_model(model_class)
    latest_changes = list(
        ObjectChange.objects.filter(
            changed_object_type=object_type,
            changed_object_id__in=target_ids,
        )
        .order_by("changed_object_id", "-time", "-pk")
        .distinct("changed_object_id")
    )
    latest_by_object_id = {
        change.changed_object_id: change for change in latest_changes
    }
    latest_applied_ids = set(
        AppliedChange.objects.filter(
            branch=branch,
            change_id__in=[change.pk for change in latest_changes],
        ).values_list("change_id", flat=True)
    )
    create_object_ids = set(
        AppliedChange.objects.filter(
            branch=branch,
            change__changed_object_type=object_type,
            change__changed_object_id__in=target_ids,
            change__action=ObjectChangeActionChoices.ACTION_CREATE,
        ).values_list("change__changed_object_id", flat=True)
    )
    return {
        target_id: {
            "has_create": target_id in create_object_ids,
            "latest_change": latest_by_object_id.get(target_id),
            "latest_is_applied": (
                latest_by_object_id.get(target_id) is not None
                and latest_by_object_id[target_id].pk in latest_applied_ids
            ),
        }
        for target_id in target_ids
    }


def _assert_existing_create_resume_provenance(
    target,
    branch,
    *,
    evidence=None,
    require_create=True,
):
    """Fail closed unless ``target`` is an unchanged create from this branch."""
    if evidence is None:
        evidence = _existing_create_resume_evidence(
            target.__class__,
            [target.pk],
            branch,
        ).get(target.pk, {})
    if require_create and not evidence.get("has_create", False):
        raise _ExistingCreateResumeConflict(
            f"Existing {target._meta.label_lower}:{target.pk} has no create "
            f"provenance for branch {branch.pk}."
        )

    latest_change = evidence.get("latest_change")
    if latest_change is None or not evidence.get("latest_is_applied", False):
        raise _ExistingCreateResumeConflict(
            f"Existing {target._meta.label_lower}:{target.pk} changed after "
            f"branch {branch.pk} applied it."
        )

    current_data = target.serialize_object(exclude=["created", "last_updated"])
    prior_applied_data = latest_change.postchange_data_clean
    serialized_mismatches = sorted(
        field_name
        for field_name, expected_value in prior_applied_data.items()
        if not _serialized_audit_values_equal(
            current_data.get(field_name),
            expected_value,
        )
    )
    mismatched_fields = _typed_audit_mismatches(
        target,
        prior_applied_data,
        serialized_mismatches,
    )
    if mismatched_fields:
        raise _ExistingCreateResumeConflict(
            f"Existing {target._meta.label_lower}:{target.pk} no longer "
            "matches its latest branch-applied audit in fields: "
            + ", ".join(mismatched_fields[:10])
        )
    return latest_change


def _serialized_audit_values_equal(current, expected):
    """Compare serialized audit values without Python bool/int coercion."""
    if type(current) is not type(expected):
        return False
    if isinstance(current, dict):
        return current.keys() == expected.keys() and all(
            _serialized_audit_values_equal(current[key], expected[key])
            for key in current
        )
    if isinstance(current, (list, tuple)):
        return len(current) == len(expected) and all(
            _serialized_audit_values_equal(left, right)
            for left, right in zip(current, expected, strict=True)
        )
    return current == expected


def _typed_audit_mismatches(target, prior_applied_data, field_names):
    """Discard representation-only drift after a strict serialized comparison."""
    derived_fields = RESUME_DERIVED_AUDIT_FIELDS.get(
        target._meta.label_lower,
        frozenset(),
    )
    field_names = [name for name in field_names if name not in derived_fields]
    if not field_names:
        return []
    try:
        deserialized = deserialize_object(
            target.__class__,
            prior_applied_data,
            pk=target.pk,
        )
    except JobTimeoutException:
        raise
    except Exception:  # noqa: BLE001 - ambiguous audit data must fail closed
        return field_names

    concrete_fields = {
        name: field
        for field in target._meta.concrete_fields
        for name in (field.name, field.attname)
    }
    mismatches = []
    for field_name in field_names:
        field = concrete_fields.get(field_name)
        if isinstance(field, models.DecimalField) and getattr(
            target, field.attname
        ) == getattr(
            deserialized.object,
            field.attname,
        ):
            continue
        mismatches.append(field_name)
    return mismatches


def _revalidate_resumed_relationships(
    items,
    locked_by_pk,
    model_class,
    branch,
    prefetch_fields,
    initial_fingerprints,
    initial_audit_pks,
    mutated_pks,
    change_logger,
):
    """Refresh relationship state and provenance immediately before success."""
    if not items or not prefetch_fields:
        return items, []
    targets = [locked_by_pk[collapsed.key[1]] for collapsed in items]
    for target in targets:
        cache = getattr(target, "_prefetched_objects_cache", {})
        for field_name in prefetch_fields:
            cache.pop(field_name, None)
    prefetch_related_objects(targets, *prefetch_fields)
    refreshed_fingerprints = _relationship_fingerprints(targets, prefetch_fields)
    evidence_by_pk = _existing_create_resume_evidence(
        model_class,
        [target.pk for target in targets],
        branch,
    )
    succeeded = []
    failed = []
    for collapsed in items:
        target = locked_by_pk[collapsed.key[1]]
        try:
            evidence = evidence_by_pk.get(target.pk) or {}
            latest_change = evidence.get("latest_change")
            if target.pk in mutated_pks:
                _assert_existing_create_resume_provenance(
                    target,
                    branch,
                    evidence=evidence,
                )
            elif (
                not evidence.get("has_create", False)
                or latest_change is None
                or not evidence.get("latest_is_applied", False)
                or latest_change.pk != initial_audit_pks.get(target.pk)
            ):
                raise _ExistingCreateResumeConflict(
                    f"Existing {target._meta.label_lower}:{target.pk} changed "
                    "during relationship revalidation."
                )
            elif refreshed_fingerprints[target.pk] != initial_fingerprints[target.pk]:
                raise _ExistingCreateResumeConflict(
                    f"Existing {target._meta.label_lower}:{target.pk} relationship "
                    "state changed during resume."
                )
        except JobTimeoutException:
            raise
        except Exception as exc:  # noqa: BLE001 - preserve per-row isolation
            change_logger.warning(
                "Bulk merge: relationship revalidation failed for %s:%s: %s",
                target._meta.label_lower,
                target.pk,
                exc,
            )
            failed.append(collapsed)
        else:
            succeeded.append(collapsed)
    return succeeded, failed


def _relationship_fingerprints(targets, field_names):
    return {
        target.pk: {
            field_name: tuple(
                sorted(
                    (
                        related._meta.label_lower,
                        related.pk,
                    )
                    for related in getattr(target, field_name).all()
                )
            )
            for field_name in field_names
        }
        for target in targets
    }


def _resume_create_matches_applied_state(collapsed, latest_change):
    """Return whether branch-desired fields already match the applied audit."""
    desired_data = collapsed.postchange_data or {}
    applied_data = latest_change.postchange_data_clean or {}
    return all(
        _serialized_audit_values_equal(
            applied_data.get(field_name),
            desired_value,
        )
        for field_name, desired_value in desired_data.items()
        if field_name not in {"created", "last_updated"}
    )


def _converge_resumed_creates(
    items,
    model_class,
    change_logger,
    request,
    branch,
):
    """Verify and converge one bounded same-model retry batch under row locks."""
    if not items:
        return [], []
    item_by_pk = {collapsed.key[1]: collapsed for collapsed in items}
    succeeded = []
    failed = []
    from .apply_engine_bulk import _serializer_prefetch_fields

    prefetch_fields = _serializer_prefetch_fields(model_class)
    with transaction.atomic():
        _lock_relationship_writes(model_class, prefetch_fields)
        locked_targets = list(
            model_class.objects.select_for_update(skip_locked=True)
            .filter(pk__in=item_by_pk)
            .order_by("pk")
        )
        locked_by_pk = {target.pk: target for target in locked_targets}
        if prefetch_fields:
            prefetch_related_objects(locked_targets, *prefetch_fields)
        initial_fingerprints = _relationship_fingerprints(
            locked_targets,
            prefetch_fields,
        )
        evidence_by_pk = _existing_create_resume_evidence(
            model_class,
            locked_by_pk,
            branch,
        )
        initial_audit_pks = {}
        mutated_pks = set()
        for pk, collapsed in item_by_pk.items():
            target = locked_by_pk.get(pk)
            if target is None:
                failed.append(collapsed)
                continue
            try:
                with transaction.atomic():
                    latest_change = _assert_existing_create_resume_provenance(
                        target,
                        branch,
                        evidence=evidence_by_pk.get(pk),
                    )
                    initial_audit_pks[pk] = latest_change.pk
                    if not _resume_create_matches_applied_state(
                        collapsed, latest_change
                    ):
                        deserialized = _deserialize(
                            model_class,
                            collapsed,
                            change_logger,
                        )
                        if _converge_existing_create_locked(
                            target,
                            deserialized,
                            collapsed.postchange_data or {},
                            change_logger,
                            request,
                            branch,
                        ):
                            mutated_pks.add(pk)
            except JobTimeoutException:
                raise
            except Exception:  # noqa: BLE001 - preserve per-row merge isolation
                failed.append(collapsed)
            else:
                succeeded.append(collapsed)
        succeeded, relationship_failures = _revalidate_resumed_relationships(
            succeeded,
            locked_by_pk,
            model_class,
            branch,
            prefetch_fields,
            initial_fingerprints,
            initial_audit_pks,
            mutated_pks,
            change_logger,
        )
        failed.extend(relationship_failures)
    return succeeded, failed


def _converge_resumed_create(
    target,
    deserialized,
    source_data,
    change_logger,
    request,
    branch,
):
    """Lock and converge a create only after proving branch-owned lineage."""

    def converge():
        with transaction.atomic():
            _lock_relationship_writes(
                target.__class__,
                (deserialized.m2m_data or {}).keys(),
            )
            locked_target = target.__class__.objects.select_for_update(nowait=True).get(
                pk=target.pk
            )
            _assert_existing_create_resume_provenance(locked_target, branch)
            return _converge_existing_create_locked(
                locked_target,
                deserialized,
                source_data,
                change_logger,
                request,
                branch,
            )

    return _retry_row_lock(converge)


def _resume_existing_create(
    collapsed_change,
    change_logger,
    request,
    branch,
):
    """Converge a CREATE already committed by an interrupted merge."""
    model_class = collapsed_change.model_class
    target = model_class.objects.filter(pk=collapsed_change.key[1]).first()
    if target is None:
        evidence = _existing_create_resume_evidence(
            model_class,
            [collapsed_change.key[1]],
            branch,
        ).get(collapsed_change.key[1], {})
        if evidence.get("has_create", False):
            raise _ExistingCreateResumeConflict(
                f"Existing {model_class._meta.label_lower}:"
                f"{collapsed_change.key[1]} was deleted after branch "
                f"{branch.pk} created it."
            )
        return False
    deserialized = _deserialize(model_class, collapsed_change, change_logger)
    _converge_resumed_create(
        target,
        deserialized,
        collapsed_change.postchange_data or {},
        change_logger,
        request,
        branch,
    )
    return True


def _skip_updates_missing_in_main_batched(collapsed_changes, change_logger):
    """Batched replacement for SquashMergeStrategy._skip_updates_missing_in_main.

    The framework runs one ``.exists()`` per collapsed UPDATE (N+1: hundreds of
    thousands of round-trips on a re-sync). Group UPDATE keys by model and check
    existence with chunked ``pk__in``, marking objects missing from main as SKIP.
    """
    by_model = defaultdict(list)
    for collapsed in collapsed_changes.values():
        if collapsed.final_action == ActionType.UPDATE:
            by_model[collapsed.model_class].append(collapsed)
    for model_class, items in by_model.items():
        pks = [c.key[1] for c in items]
        present = set()
        for i in range(0, len(pks), BULK_MERGE_FLUSH_THRESHOLD):
            chunk = pks[i : i + BULK_MERGE_FLUSH_THRESHOLD]
            present.update(
                model_class.objects.using(DEFAULT_DB_ALIAS)
                .filter(pk__in=chunk)
                .values_list("pk", flat=True)
            )
        missing_pks = []
        for collapsed in items:
            if collapsed.key[1] in present:
                continue
            missing_pks.append(collapsed.key[1])
            collapsed.final_action = ActionType.SKIP
        if missing_pks:
            sample = ", ".join(str(pk) for pk in missing_pks[:10])
            change_logger.info(
                "Skipping %d UPDATE(s) for %s because the objects were deleted "
                "in main (sample PKs: %s%s)",
                len(missing_pks),
                model_class.__name__,
                sample,
                ", ..." if len(missing_pks) > 10 else "",
            )


def _skip_protecting_reference_blocked_deletes(collapsed_changes, change_logger):
    """Skip deletes the database will refuse, instead of failing them at apply.

    `protecting_reference_blocked_deletes` identifies a delete whose PROTECT or
    RESTRICT reference *survives* the run. Scheduling it anyway guarantees a
    `ProtectedError` at apply time, which fails the row — and per the merge
    failure dead end a single failed row permanently blocks baseline promotion.
    So one operator-owned object the plugin does not manage could wedge an
    entire sync's convergence bookkeeping indefinitely.

    The plugin cannot resolve these itself. The three PROTECT references into
    `dcim.Device` are `dcim.VirtualChassis.master`,
    `dcim.VirtualDeviceContext.device` and `virtualization.VirtualMachine.device`,
    and only virtualchassis is a synced model — the other two are operator data.
    Deleting them to clear the path would destroy records the operator owns, and
    the sync has no mandate for that.

    So report and skip: the delete intent stays exact and visible as an issue,
    the row stays in drift where an operator can act on it, and the merge is not
    wedged by a condition it cannot fix. Ordering already handles the case where
    the referencing row *is* deleted in the same run; only survivors reach here.

    Returns the skipped keys mapped to what holds them.
    """
    blocked = protecting_reference_blocked_deletes(collapsed_changes)
    if not blocked:
        return {}
    for key, references in blocked.items():
        collapsed_changes[key].final_action = ActionType.SKIP
        held_by = ", ".join(f"{label} ({count})" for label, count in references)
        change_logger.warning(
            "Skipping DELETE for %s %s: still referenced by %s. The database "
            "would refuse it, and those rows are not being deleted in this run.",
            key[0],
            key[1],
            held_by,
        )
    change_logger.info(
        "Skipped %d delete(s) held by a surviving protected reference.",
        len(blocked),
    )
    return blocked


def _add_destination_fk_release_dependencies(collapsed_changes, change_logger):
    """Order destination FK releases before deletes even without a branch preimage.

    Branch-native bulk UPDATE ObjectChanges can have an empty ``prechange_data``
    when the destination row came from the direct first-baseline path.  The
    Branching dependency graph then cannot see that an UPDATE releases a
    protected FK and may attempt the referenced DELETE first.  Read the current
    destination FK values in model batches and restore only the missing release
    edges.  An UPDATE that retains the reference gets no edge, so the protected
    DELETE still fails strictly instead of being silently skipped.
    """
    deletes_by_model = defaultdict(dict)
    updates_by_model = defaultdict(list)
    for collapsed in collapsed_changes.values():
        if collapsed.final_action == ActionType.DELETE:
            deletes_by_model[collapsed.model_class][collapsed.key[1]] = collapsed
        elif collapsed.final_action == ActionType.UPDATE:
            updates_by_model[collapsed.model_class].append(collapsed)

    if not deletes_by_model or not updates_by_model:
        return

    for model_class, updates in updates_by_model.items():
        release_fields = [
            field
            for field in model_class._meta.get_fields()
            if isinstance(field, models.ForeignKey)
            and field.related_model in deletes_by_model
        ]
        if not release_fields:
            continue

        updates_by_pk = {collapsed.key[1]: collapsed for collapsed in updates}
        destination_values = {}
        update_pks = list(updates_by_pk)
        value_fields = [field.attname for field in release_fields]
        for offset in range(0, len(update_pks), BULK_MERGE_FLUSH_THRESHOLD):
            chunk = update_pks[offset : offset + BULK_MERGE_FLUSH_THRESHOLD]
            destination_values.update(
                {
                    row["pk"]: row
                    for row in model_class.objects.using(DEFAULT_DB_ALIAS)
                    .filter(pk__in=chunk)
                    .values("pk", *value_fields)
                }
            )

        edge_count = 0
        for pk, update in updates_by_pk.items():
            current = destination_values.get(pk)
            if current is None:
                continue
            for field in release_fields:
                current_fk = current[field.attname]
                delete = deletes_by_model[field.related_model].get(current_fk)
                if delete is None:
                    continue
                if field.name not in (update.postchange_data or {}):
                    continue
                desired_fk = _serialized_fk_id(
                    (update.postchange_data or {}).get(field.name)
                )
                if desired_fk == current_fk:
                    continue
                delete.depends_on.add(update.key)
                update.depended_by.add(delete.key)
                edge_count += 1
        if edge_count:
            change_logger.debug(
                "Added %d destination FK-release dependency edge(s) for %s.",
                edge_count,
                model_class._meta.label_lower,
            )


def describe_protecting_references(model_class, pk):
    """Which surviving rows still reference this one, as ``[(label, count)]``.

    A `ProtectedError` names the exception but not what an operator has to act
    on. Read the protecting relations directly so a refused delete reports the
    model and count holding it. Never raises: diagnostics must not turn a
    recorded failure into an unhandled one.
    """
    found = []
    try:
        for relation in model_class._meta.related_objects:
            field = relation.field
            if getattr(field.remote_field, "on_delete", None) not in (
                models.PROTECT,
                models.RESTRICT,
            ):
                continue
            count = (
                relation.related_model.objects.using(DEFAULT_DB_ALIAS)
                .filter(**{field.name: pk})
                .count()
            )
            if count:
                found.append((relation.related_model._meta.label, count))
    except JobTimeoutException:
        raise
    except Exception:  # noqa: BLE001 - a diagnostic must never mask the failure
        return []
    return sorted(found)


def protecting_reference_blocked_deletes(collapsed_changes):
    """Deletes that a surviving PROTECT/RESTRICT reference would reject.

    Delete protection is otherwise plugin-ownership based, so a row that the
    database itself refuses to delete is still scheduled and fails at apply
    time with `ProtectedError`, leaving the merge short of convergence. That is
    how a device kept two BGP peers and never converged.

    A reference only blocks when the referencing row is **not changing at all**
    in this run. Two ways it can be changing, and both resolve the block:

    * it is deleted here — ordering puts the child before the parent
      (`_add_protected_child_delete_dependencies`);
    * it is updated here — the update may release the FK, and
      `_add_destination_fk_release_dependencies` orders that release before the
      delete. This read happens against destination state *before* either has
      been applied, so a referrer that is about to release still looks like it
      protects. Treating an updated referrer as blocking would skip a delete
      that in fact succeeds — which is how the DLM protected-version delete,
      the exact case that release-ordering exists for, regressed.

    Erring this way is deliberate: wrongly skipping loses a real delete
    silently, while wrongly allowing one just restores the pre-existing
    `ProtectedError`, which is reported.

    Returns ``{key: [(referencing label, count), ...]}``.
    """
    delete_pks_by_model = defaultdict(dict)
    changing_pks_by_model = defaultdict(set)
    for key, collapsed in collapsed_changes.items():
        if collapsed.final_action == ActionType.DELETE:
            delete_pks_by_model[collapsed.model_class][collapsed.key[1]] = key
            changing_pks_by_model[collapsed.model_class].add(collapsed.key[1])
        elif collapsed.final_action == ActionType.UPDATE:
            changing_pks_by_model[collapsed.model_class].add(collapsed.key[1])
    if not delete_pks_by_model:
        return {}

    blocked = defaultdict(list)
    for model_class, deletes_by_pk in delete_pks_by_model.items():
        target_pks = list(deletes_by_pk)
        for relation in model_class._meta.related_objects:
            field = relation.field
            on_delete = getattr(field.remote_field, "on_delete", None)
            if on_delete not in (models.PROTECT, models.RESTRICT):
                continue
            referencing_model = relation.related_model
            changing = changing_pks_by_model.get(referencing_model, set())
            counts = defaultdict(int)
            for offset in range(0, len(target_pks), BULK_MERGE_FLUSH_THRESHOLD):
                chunk = target_pks[offset : offset + BULK_MERGE_FLUSH_THRESHOLD]
                rows = (
                    referencing_model.objects.using(DEFAULT_DB_ALIAS)
                    .filter(**{f"{field.name}__in": chunk})
                    .values_list("pk", field.attname)
                )
                for referencing_pk, target_pk in rows:
                    if referencing_pk in changing:
                        # Deleted or updated in this run; ordering handles it.
                        continue
                    counts[target_pk] += 1
            for target_pk, count in counts.items():
                blocked[deletes_by_pk[target_pk]].append(
                    (referencing_model._meta.label, count)
                )
    return dict(blocked)


def _protected_child_delete_edges(collapsed_changes):
    """Candidate ``(parent_key, child_key)`` DELETE edges read from destination state.

    Only pairs where *both* rows are being deleted in this run are returned. A
    reference that survives the run is deliberately left alone so the parent
    delete still fails strictly instead of being reordered into a false success.
    """
    deletes_by_model = defaultdict(dict)
    for key, collapsed in collapsed_changes.items():
        if collapsed.final_action == ActionType.DELETE:
            deletes_by_model[collapsed.model_class][collapsed.key[1]] = key
    if not deletes_by_model:
        return []

    edges = []
    for model_class, deletes_by_pk in deletes_by_model.items():
        target_pks = list(deletes_by_pk)
        for relation in model_class._meta.related_objects:
            field = relation.field
            if getattr(field.remote_field, "on_delete", None) not in (
                models.PROTECT,
                models.RESTRICT,
            ):
                continue
            child_deletes = deletes_by_model.get(relation.related_model)
            if not child_deletes:
                continue
            for offset in range(0, len(target_pks), BULK_MERGE_FLUSH_THRESHOLD):
                chunk = target_pks[offset : offset + BULK_MERGE_FLUSH_THRESHOLD]
                rows = (
                    relation.related_model.objects.using(DEFAULT_DB_ALIAS)
                    .filter(**{f"{field.name}__in": chunk})
                    .values_list("pk", field.attname)
                )
                for child_pk, parent_pk in rows:
                    child_key = child_deletes.get(child_pk)
                    if child_key is None:
                        continue
                    parent_key = deletes_by_pk.get(parent_pk)
                    if parent_key is not None and parent_key != child_key:
                        edges.append((parent_key, child_key))
    return edges


def _acyclic_delete_edges(
    collapsed_changes,
    candidate_edges,
    change_logger,
    *,
    edge_label="protected-delete",
):
    """Drop candidate edges that would make the ordering graph unsortable.

    A cycle makes `_order_collapsed_changes_fast` raise and fails the *entire*
    merge, which is far worse than one delete the database refuses. Mutually
    protecting rows are undeletable by the database anyway, so dropping the edge
    only returns that pair to the strict failure it would have had regardless.

    Edges are ``(dependent_key, dependency_key)``. The algorithm is not specific
    to deletes; `edge_label` only names the edge class in the warning.
    """
    nodes = {key for edge in candidate_edges for key in edge}
    for key in list(nodes):
        nodes.update(
            dep for dep in collapsed_changes[key].depends_on if dep in collapsed_changes
        )
    existing = {
        key: {
            dep
            for dep in collapsed_changes[key].depends_on
            if dep in nodes and dep != key
        }
        for key in nodes
    }
    accepted = list(candidate_edges)
    for _attempt in range(3):
        depends = {key: set(deps) for key, deps in existing.items()}
        for parent_key, child_key in accepted:
            depends[parent_key].add(child_key)
        indeg = {key: len(deps) for key, deps in depends.items()}
        successors = defaultdict(list)
        for key, deps in depends.items():
            for dep in deps:
                successors[dep].append(key)
        queue = [key for key, count in indeg.items() if count == 0]
        seen = 0
        while queue:
            key = queue.pop()
            seen += 1
            for succ in successors.get(key, ()):
                indeg[succ] -= 1
                if indeg[succ] == 0:
                    queue.append(succ)
        if seen == len(depends):
            return accepted
        cyclic = {key for key, count in indeg.items() if count > 0}
        kept = [
            edge for edge in accepted if edge[0] not in cyclic and edge[1] not in cyclic
        ]
        if len(kept) == len(accepted):
            return []
        change_logger.warning(
            "Bulk merge: dropped %d %s ordering edge(s) that would "
            "have made the change graph unsortable; those rows stay mutually "
            "protected and their deletes are reported rather than reordered.",
            len(accepted) - len(kept),
            edge_label,
        )
        accepted = kept
        if not accepted:
            return []
    return []


def _add_protected_child_delete_dependencies(collapsed_changes, change_logger):
    """Order child DELETEs before parent DELETEs using destination state.

    The framework builds child-before-parent DELETE edges from each child's
    ``prechange_data`` (`_build_fk_dependency_graph` step 3). Branch-native bulk
    DELETE ObjectChanges can carry an empty ``prechange_data`` when the row came
    from the direct first-baseline path — the same gap
    `_add_destination_fk_release_dependencies` exists to close for UPDATEs. The
    edge is then never created, the parent DELETE is attempted while a child row
    still references it, and the database refuses it with ``ProtectedError``. The
    parent survives, and every later sync repeats the failure: that is how a
    device kept two BGP peers and never converged.

    Reading the protecting references from the destination needs no branch
    preimage, so the edge is restored whether or not a preimage exists.
    """
    edges = _protected_child_delete_edges(collapsed_changes)
    if not edges:
        return 0
    accepted = _acyclic_delete_edges(collapsed_changes, edges, change_logger)
    added = 0
    for parent_key, child_key in accepted:
        parent = collapsed_changes[parent_key]
        if child_key in parent.depends_on:
            continue
        parent.depends_on.add(child_key)
        collapsed_changes[child_key].depended_by.add(parent_key)
        added += 1
    if added:
        change_logger.debug(
            "Restored %d child-before-parent DELETE ordering edge(s) from "
            "destination protecting references.",
            added,
        )
    return added


def _add_ip_adoption_dependencies(collapsed_changes, change_logger):
    """Order an IP's assignment UPDATE before the UPDATE that adopts it.

    ``Device.clean()`` requires ``primary_ip4``/``primary_ip6``/``oob_ip`` to
    resolve to an interface of that device (``vc_interfaces``), read from the
    *destination*. So the change that assigns the IP to the interface has to be
    merged before the change that points the device at the IP.

    The framework graph never orders these. ``_build_fk_dependency_graph`` builds
    UPDATE->CREATE, CREATE->CREATE, DELETE->UPDATE and DELETE->DELETE edges — it
    has no UPDATE->UPDATE case at all, and neither did this module. While both
    rows are new the ordering still holds by accident, through the CREATE chain
    (device -> ip -> interface). But when the device *and* the IP already exist
    in main and the branch only re-points them — a Mgmt_-tagged device adopting
    an address that is being moved onto its interface in the same sync — the two
    UPDATEs carry no edge between them and fall back to the sort's timestamp
    tie-break. Whichever ObjectChange was recorded first wins.

    Device-first raises ``ValidationError({'primary_ip4': 'The specified IP
    address (...) is not assigned to this device.'})``. It is a race, not a
    deterministic failure, which is why it presented as a single device out of
    thousands and never reproduced.
    """
    ip_updates = {
        collapsed.key[1]: collapsed
        for collapsed in collapsed_changes.values()
        if collapsed.final_action == ActionType.UPDATE
        and collapsed.model_class._meta.label_lower == "ipam.ipaddress"
    }
    if not ip_updates:
        return 0

    candidate_edges = []
    for key, collapsed in collapsed_changes.items():
        if collapsed.final_action != ActionType.UPDATE:
            continue
        # key[0] is the model label for both real UPDATE keys and the synthetic
        # ("<label>", pk, "defer_self_ref_fk") keys the create-cycle split adds.
        fields = _DEFERRED_CREATE_FK_FIELDS.get(key[0])
        if not fields:
            continue
        postchange = collapsed.postchange_data or {}
        for field_name in fields:
            field = collapsed.model_class._meta.get_field(field_name)
            related = getattr(field, "related_model", None)
            if related is None or related._meta.label_lower != "ipam.ipaddress":
                continue
            adopted = _serialized_fk_id(postchange.get(field_name))
            if not adopted:
                continue
            ip_change = ip_updates.get(adopted)
            if ip_change is None or ip_change.key == key:
                continue
            if ip_change.key in collapsed.depends_on:
                continue
            candidate_edges.append((key, ip_change.key))

    if not candidate_edges:
        return 0

    accepted = _acyclic_delete_edges(
        collapsed_changes,
        candidate_edges,
        change_logger,
        edge_label="IP-adoption",
    )
    added = 0
    for dependent_key, dependency_key in accepted:
        dependent = collapsed_changes[dependent_key]
        if dependency_key in dependent.depends_on:
            continue
        dependent.depends_on.add(dependency_key)
        collapsed_changes[dependency_key].depended_by.add(dependent_key)
        added += 1
    if added:
        change_logger.debug(
            "Ordered %d IP assignment change(s) before the device/VM update "
            "that adopts the address as a primary or OOB IP.",
            added,
        )
    return added


def _order_collapsed_changes_fast(collapsed_changes, change_logger, operation):
    """O((V+E) log V) replacement for ``_order_collapsed_changes``.

    Reuses the framework's SKIP filtering, bidirectional-cycle splitting, FK
    dependency-graph build, and the ``squash_dependency_graph_built`` signal;
    only the topological sort is replaced. The framework sort is O(V^2) — it
    rescans every remaining node per layer and discards the processed key from
    every remaining node — and hangs on a single large model batch even with no
    edges. This uses Kahn's algorithm with a reverse-adjacency index built from
    ``depends_on`` and a heap so ties keep Tag CREATEs first, then the standard
    DELETE->UPDATE->CREATE (then time) preference. Returns an ordered list of
    CollapsedChange.
    """
    to_process = {
        k: v for k, v in collapsed_changes.items() if v.final_action != ActionType.SKIP
    }
    if not to_process:
        return []

    # The exact 1.1.1 runtime breaks cycles inside its own sorter rather than
    # exposing a helper. This custom O((V+E) log V) sorter owns the equivalent
    # deterministic pair split before reusing the framework dependency graph.
    with profile_scope(
        "dependency_cycle_preparation", owner="ours", rows=len(to_process)
    ):
        _split_bidirectional_create_cycles(to_process, change_logger)
        # Then split deferred self-referential FKs (primary_ip4/6, oob_ip, VC master)
        # out of CREATEs — these form 3-node cycles the framework's 2-node split misses
        # (device -> primary_ip -> assigned interface -> device). Must run before the
        # graph build below so the cycle edge is never created.
        _defer_self_referential_create_fks(to_process, change_logger)
        _add_tag_identity_release_dependencies(to_process)
        deletes = sorted(
            (v for v in to_process.values() if v.final_action == ActionType.DELETE),
            key=lambda c: c.last_change.time,
        )
        updates = sorted(
            (v for v in to_process.values() if v.final_action == ActionType.UPDATE),
            key=lambda c: c.last_change.time,
        )
        creates = sorted(
            (v for v in to_process.values() if v.final_action == ActionType.CREATE),
            key=lambda c: c.last_change.time,
        )
    with profile_scope(
        "dependency_graph",
        owner="upstream_netbox_branching",
        rows=len(to_process),
    ):
        SquashMergeStrategy._build_fk_dependency_graph(
            deletes, updates, creates, change_logger
        )
        # Both restore edges the framework can only derive from a branch
        # preimage, which branch-native bulk changes may not carry. They must
        # run after the framework graph build and before the signal, so a
        # receiver sees the completed graph.
        _add_destination_fk_release_dependencies(to_process, change_logger)
        _add_protected_child_delete_dependencies(to_process, change_logger)
        # UPDATE->UPDATE ordering, which the framework graph has no case for.
        _add_ip_adoption_dependencies(to_process, change_logger)
        squash_dependency_graph_built.send(
            sender=SquashMergeStrategy,
            collapsed_changes=to_process,
            operation=operation,
        )

    # Authoritative reverse index + in-degree, both from depends_on filtered to
    # in-scope keys (so a dependency on a SKIP'd/absent node never wedges).
    with profile_scope("dependency_ordering", owner="ours", rows=len(to_process)):
        successors = defaultdict(list)
        indeg = {}
        for key, collapsed in to_process.items():
            deps = [d for d in collapsed.depends_on if d in to_process]
            indeg[key] = len(deps)
            for dep in deps:
                successors[dep].append(key)

        heap = []
        seq = 0
        for key, collapsed in to_process.items():
            if indeg[key] == 0:
                heapq.heappush(
                    heap,
                    (
                        _merge_priority(collapsed),
                        collapsed.last_change.time,
                        seq,
                        key,
                    ),
                )
                seq += 1

        ordered = []
        while heap:
            _, _, _, key = heapq.heappop(heap)
            ordered.append(to_process[key])
            for succ in successors.get(key, ()):
                indeg[succ] -= 1
                if indeg[succ] == 0:
                    sv = to_process[succ]
                    heapq.heappush(
                        heap,
                        (
                            _merge_priority(sv),
                            sv.last_change.time,
                            seq,
                            succ,
                        ),
                    )
                    seq += 1

    if len(ordered) != len(to_process):
        remaining = {
            k: set(v.depends_on) for k, v in to_process.items() if indeg.get(k, 0) > 0
        }
        SquashMergeStrategy._log_cycle_details(
            remaining,
            to_process,
            change_logger,
        )
        raise RuntimeError(
            f"Cycle detected in dependency graph. {len(remaining)} changes are "
            f"involved in circular dependencies and cannot be ordered."
        )
    return ordered


def bulk_merge_changes(
    branch,
    changes,
    request,
    user,
    change_logger=None,
    *,
    apply_one,
    record_applied=None,
    record_failed=None,
    result_metadata=None,
    set_based_decision=None,
):
    """Merge branch changes into main regardless of caller branch context."""
    with deactivate_branch():
        return _bulk_merge_changes_main(
            branch,
            changes,
            request,
            user,
            change_logger,
            apply_one=apply_one,
            record_applied=record_applied,
            record_failed=record_failed,
            result_metadata=result_metadata,
            set_based_decision=set_based_decision,
        )


def _bulk_merge_changes_main(
    branch,
    changes,
    request,
    user,
    change_logger=None,
    *,
    apply_one,
    record_applied=None,
    record_failed=None,
    result_metadata=None,
    set_based_decision=None,
):
    """Merge ``changes`` (a branch's unmerged ObjectChanges) into main.

    ``apply_one(collapsed_change)`` is the caller's per-object fallback (the
    existing savepoint-isolated ObjectChange.apply path); it returns True on
    success, ``_ApplyOneFailure`` with isolated exception evidence on failure,
    or False without exception evidence.
    ``record_applied(model)`` is an optional per-applied-row callback for
    stats/heartbeat.
    ``record_failed(collapsed_change, exc)`` records failures rejected before
    ``apply_one`` can safely run; it must not mutate the destination object.

    Returns ``(applied, failed, models_touched)``.
    """
    change_logger = change_logger or logger
    # Stream the raw queryset so the full ~1M-row ObjectChange result set (each
    # row carrying two JSONFields) is never resident at once.
    change_iter = (
        changes.iterator(chunk_size=BULK_MERGE_INPUT_CHUNK_SIZE)
        if hasattr(changes, "iterator")
        else changes
    )
    with profile_scope("collapse_changes", owner="upstream_netbox_branching"):
        collapsed, _ = SquashMergeStrategy._collapse_changes(
            change_iter,
            change_logger,
        )
    logical_actions = {
        key: change.final_action.value for key, change in collapsed.items()
    }
    with profile_scope("skip_missing_updates", owner="ours", rows=len(collapsed)):
        _skip_updates_missing_in_main_batched(collapsed, change_logger)
    # Before ordering: a delete held by a surviving PROTECT reference can never
    # succeed, and failing it would block baseline promotion over data the
    # plugin does not own.
    with profile_scope(
        "skip_protected_blocked_deletes", owner="ours", rows=len(collapsed)
    ):
        protected_blocked_deletes = _skip_protecting_reference_blocked_deletes(
            collapsed, change_logger
        )
    ordered = _order_collapsed_changes_fast(collapsed, change_logger, "merge")
    affected_prefix_vrf_ids = _affected_prefix_vrf_ids(ordered)

    applied = 0
    failed = 0
    models_touched = set()
    batch = []  # collapsed CREATE changes for the current model (not yet built)
    batch_model = None
    prefix_batch = []
    prefix_batch_action = None
    deferred_fk_batch = []
    deferred_fk_batch_model = None
    set_based_mac_batch = []
    logical_components = defaultdict(set)
    logical_models = {}
    component_results = {}
    component_failures = {}
    component_failure_order = []
    accounted_logical_keys = set()
    original_logical_changes = dict(collapsed)

    # Start with every original collapsed branch change, including UPDATEs that
    # became destination no-ops because the object was concurrently removed from
    # main. The sorter adds synthetic components for deferred FKs and reciprocal
    # create cycles; those components belong to their original logical key.
    for logical_key, collapsed_change in collapsed.items():
        logical_components[logical_key].add(logical_key)
        logical_models[logical_key] = collapsed_change.model_class
    for collapsed_change in ordered:
        logical_key = getattr(
            collapsed_change,
            "_forward_original_change_key",
            collapsed_change.key,
        )
        logical_components[logical_key].add(collapsed_change.key)
        logical_models.setdefault(logical_key, collapsed_change.model_class)

    def _record_result(
        collapsed_change,
        model_class,
        succeeded,
        *,
        failure=None,
    ):
        """Account one original branch change after all internal work finishes."""
        nonlocal applied, failed
        logical_key = getattr(
            collapsed_change,
            "_forward_original_change_key",
            collapsed_change.key,
        )
        component_key = collapsed_change.key
        if component_key in component_results:
            raise RuntimeError(
                f"Bulk merge component {component_key!r} was accounted twice."
            )
        component_results[component_key] = bool(succeeded)
        if not succeeded:
            component_failures[component_key] = failure
            component_failure_order.append(component_key)
        required = logical_components[logical_key]
        if not required.issubset(component_results):
            return
        if logical_key in accounted_logical_keys:
            raise RuntimeError(
                f"Bulk merge logical change {logical_key!r} was accounted twice."
            )
        accounted_logical_keys.add(logical_key)
        if all(component_results[key] for key in required):
            applied += 1
            if record_applied:
                record_applied(logical_models.get(logical_key, model_class))
            return
        failed += 1
        if record_failed:
            ordered_failure_keys = []
            if logical_key in component_failures:
                ordered_failure_keys.append(logical_key)
            ordered_failure_keys.extend(
                key
                for key in component_failure_order
                if key in required and key != logical_key
            )
            failure_exc = next(
                (
                    component_failures[key]
                    for key in ordered_failure_keys
                    if component_failures[key] is not None
                ),
                RuntimeError("Bulk merge rejected the collapsed change."),
            )
            record_failed(
                original_logical_changes.get(logical_key, collapsed_change),
                failure_exc,
            )

    def _record_success(collapsed_change, model_class):
        _record_result(collapsed_change, model_class, True)

    def _record_failure(
        collapsed_change,
        *,
        failure=None,
    ):
        _record_result(
            collapsed_change,
            collapsed_change.model_class,
            False,
            failure=failure,
        )

    def _apply_via_fallback(collapsed_change, model_class):
        # Isolate a single object's apply failure (e.g. a row deleted in main
        # while the branch modified it, or any per-object validation conflict) so
        # one bad change never fails the whole sync — the merge must be resilient
        # for steady-state diffs. Mirrors the bulk-create IntegrityError isolation.
        model_label = model_class._meta.label_lower
        try:
            with profile_scope(
                "fallback_orchestration",
                model=model_label,
                owner="ours",
                rows=1,
            ):
                resumed = False
                if collapsed_change.final_action == ActionType.CREATE:
                    with profile_scope(
                        "replay_existing_create_check",
                        model=model_label,
                        owner="ours",
                        rows=1,
                    ):
                        resumed = _resume_existing_create(
                            collapsed_change,
                            change_logger,
                            request,
                            branch,
                        )
                if resumed:
                    _record_success(collapsed_change, model_class)
                    return
                outcome = apply_one(collapsed_change)
        except JobTimeoutException:
            raise
        except Exception as exc:  # noqa: BLE001 - isolate one object, keep merging
            change_logger.warning(
                "Bulk merge: isolating %s change for %s after apply error: %s",
                getattr(getattr(collapsed_change, "final_action", None), "value", "?"),
                getattr(model_class, "__name__", model_class),
                exc,
            )
            if isinstance(exc, ProtectedError):
                # Name what actually holds the row. Without this the operator
                # sees only ProtectedError and cannot tell why the sync never
                # converged.
                references = describe_protecting_references(
                    model_class, collapsed_change.key[1]
                )
                if references:
                    change_logger.warning(
                        "Bulk merge: %s %s cannot be deleted while referenced by "
                        "%s. Remove or reassign those references, or exclude the "
                        "row from this sync's scope.",
                        getattr(model_class, "__name__", model_class),
                        collapsed_change.key[1],
                        ", ".join(f"{label} x{count}" for label, count in references),
                    )
            _record_failure(collapsed_change, failure=exc)
            return
        if isinstance(outcome, _ApplyOneFailure):
            _record_failure(collapsed_change, failure=outcome.exception)
            return
        if outcome:
            _record_success(collapsed_change, model_class)
            return
        _record_failure(collapsed_change)

    def _flush_impl():
        nonlocal batch, batch_model
        if not batch:
            return
        model_class = batch_model
        pending = batch
        batch = []
        batch_model = None

        # Resume idempotency: a fast bulk merge that stopped after an earlier
        # committed sub-batch re-runs the whole branch. Existing rows are
        # converged to the collapsed CREATE state before being attested; primary
        # key existence alone is not evidence that scalar and M2M state matches.
        all_pks = [c.key[1] for c in pending]
        existing_by_pk = {
            item.pk: item for item in model_class.objects.filter(pk__in=all_pks)
        }
        missing_pks = [pk for pk in all_pks if pk not in existing_by_pk]
        missing_evidence_by_pk = _existing_create_resume_evidence(
            model_class,
            missing_pks,
            branch,
        )
        alternate_tag_ids_by_source_pk = _alternate_tag_targets_by_source_pk(
            model_class,
            all_pks,
            branch,
        )
        alternate_tag_targets_by_pk = {
            target.pk: target
            for target in model_class.objects.filter(
                pk__in={
                    target_pk
                    for target_pks in alternate_tag_ids_by_source_pk.values()
                    for target_pk in target_pks
                }
            )
        }
        # Tags collide by NAME, not pk: while the merge applies device UPDATEs
        # (ordered before CREATEs), netbox_branching sets device tags by name,
        # which get_or_creates the tag on main with a NEW pk. The branch's tag
        # CREATE then violates the unique name/slug constraint. Treat a
        # same-named (or same-slug) main-side tag as the identity target, then
        # converge its mutable fields and M2M state in this merge.
        tag_targets_by_name: dict[str, list] = defaultdict(list)
        tag_targets_by_slug: dict[str, list] = defaultdict(list)
        if getattr(model_class._meta, "label_lower", "") == "extras.tag":
            pending_names = [
                str((c.postchange_data or {}).get("name") or "") for c in pending
            ]
            pending_slugs = [
                str((c.postchange_data or {}).get("slug") or "") for c in pending
            ]
            name_filter = Q(name__in=[n for n in pending_names if n]) | Q(
                slug__in=[s for s in pending_slugs if s]
            )
            for target in model_class.objects.filter(name_filter):
                tag_targets_by_name[str(target.name)].append(target)
                tag_targets_by_slug[str(target.slug)].append(target)
        objects = []
        built = []  # (collapsed, deserialized) for rows we actually create
        resumed = []  # same-pk rows verified together under ordered row locks
        for collapsed_change in pending:
            target = existing_by_pk.get(collapsed_change.key[1])
            if target is None and missing_evidence_by_pk.get(
                collapsed_change.key[1], {}
            ).get("has_create", False):
                conflict = _ExistingCreateResumeConflict(
                    "Refusing to recreate deleted branch-owned "
                    f"{model_class._meta.label_lower}:"
                    f"{collapsed_change.key[1]}."
                )
                change_logger.warning("Bulk merge: %s", conflict)
                _record_failure(collapsed_change, failure=conflict)
                continue
            alternate_target_ids = alternate_tag_ids_by_source_pk.get(
                collapsed_change.key[1], set()
            )
            if len(alternate_target_ids) > 1:
                conflict = _ExistingCreateResumeConflict(
                    "Multiple alternate identities are attested for "
                    f"{model_class._meta.label_lower}:"
                    f"{collapsed_change.key[1]}."
                )
                change_logger.warning("Bulk merge: %s", conflict)
                _record_failure(collapsed_change, failure=conflict)
                continue
            alternate_resume = bool(alternate_target_ids)
            if alternate_resume:
                alternate_target_pk = next(iter(alternate_target_ids))
                if target is not None and target.pk != alternate_target_pk:
                    conflict = _ExistingCreateResumeConflict(
                        "Conflicting original and alternate identities exist for "
                        f"{model_class._meta.label_lower}:"
                        f"{collapsed_change.key[1]}."
                    )
                    change_logger.warning("Bulk merge: %s", conflict)
                    _record_failure(collapsed_change, failure=conflict)
                    continue
                target = alternate_tag_targets_by_pk.get(alternate_target_pk)
                if target is None:
                    conflict = _ExistingCreateResumeConflict(
                        "Alternate identity "
                        f"{model_class._meta.label_lower}:{alternate_target_pk} "
                        f"for source {collapsed_change.key[1]} was deleted."
                    )
                    change_logger.warning("Bulk merge: %s", conflict)
                    _record_failure(collapsed_change, failure=conflict)
                    continue
            if tag_targets_by_name or tag_targets_by_slug:
                data = collapsed_change.postchange_data or {}
                tag_targets = {
                    item.pk: item
                    for item in (
                        tag_targets_by_name.get(str(data.get("name") or ""), [])
                        + tag_targets_by_slug.get(str(data.get("slug") or ""), [])
                    )
                }
                if len(tag_targets) > 1 and not alternate_resume:
                    conflict = _ExistingCreateResumeConflict(
                        "Multiple destination identities match "
                        f"{model_class._meta.label_lower}:"
                        f"{collapsed_change.key[1]}."
                    )
                    change_logger.warning("Bulk merge: %s", conflict)
                    _record_failure(collapsed_change, failure=conflict)
                    continue
                if tag_targets and not alternate_resume:
                    target = next(iter(tag_targets.values()))
            if target is not None and target.pk == collapsed_change.key[1]:
                resumed.append(collapsed_change)
                continue
            try:
                deserialized = _deserialize(
                    model_class, collapsed_change, change_logger
                )
            except JobTimeoutException:
                raise
            except Exception as exc:  # noqa: BLE001 - isolate invalid payload
                if target is not None:
                    _record_failure(collapsed_change, failure=exc)
                    continue
                _apply_via_fallback(collapsed_change, model_class)
                continue
            if target is not None:
                try:
                    lineage_message = _alternate_create_lineage_message(
                        model_class,
                        collapsed_change.key[1],
                    )
                    _converge_existing_create(
                        target,
                        deserialized,
                        collapsed_change.postchange_data or {},
                        change_logger,
                        request,
                        branch,
                        require_branch_lineage=alternate_resume,
                        lineage_message=lineage_message,
                        attest_if_unchanged=not alternate_resume,
                        refuse_changes=not alternate_resume,
                    )
                except JobTimeoutException:
                    raise
                except Exception as exc:  # noqa: BLE001 - fail closed on identity
                    _record_failure(collapsed_change, failure=exc)
                    continue
                _record_success(collapsed_change, model_class)
                continue
            built.append((collapsed_change, deserialized))
            objects.append(deserialized.object)

        resumed_ok, resumed_failed = _converge_resumed_creates(
            resumed,
            model_class,
            change_logger,
            request,
            branch,
        )
        for collapsed_change in resumed_ok:
            _record_success(collapsed_change, model_class)
        for collapsed_change in resumed_failed:
            _apply_via_fallback(collapsed_change, model_class)

        if not objects:
            return

        try:
            # Each sub-batch commits in its own transaction: bounded locks/WAL,
            # and a constraint failure isolates to this sub-batch instead of
            # rolling back an entire 500k-row model.
            with transaction.atomic():
                model_class.objects.bulk_create(
                    objects, batch_size=BULK_MERGE_BATCH_SIZE
                )
                for _, deserialized in built:
                    for accessor, values in (deserialized.m2m_data or {}).items():
                        getattr(deserialized.object, accessor).set(values)
                if model_class._meta.label_lower == "ipam.prefix":
                    _rebuild_main_prefix_hierarchies(
                        _affected_prefix_vrf_ids(
                            collapsed_change for collapsed_change, _ in built
                        )
                    )
                    refreshed = {
                        pk: (depth, children)
                        for pk, depth, children in model_class.objects.filter(
                            pk__in=[obj.pk for obj in objects]
                        ).values_list("pk", "_depth", "_children")
                    }
                    for obj in objects:
                        obj._depth, obj._children = refreshed[obj.pk]
                try:
                    from core.choices import ObjectChangeActionChoices

                    _emit_main_object_changes(
                        objects,
                        ObjectChangeActionChoices.ACTION_CREATE,
                        request,
                        branch,
                    )
                except JobTimeoutException:
                    raise
                except Exception as exc:  # audit is part of the atomic mutation
                    raise _BulkMergeAuditError from exc
        except _BulkMergeAuditError:
            raise
        except JobTimeoutException:
            raise
        except Exception as exc:  # noqa: BLE001 - roll back scalar and M2M state
            # A scalar or relationship write failed and rolled the entire
            # sub-batch back. Fall back to the per-object path so good rows apply
            # and the offender is isolated as an issue.
            change_logger.warning(
                "Bulk merge create batch for %s failed with %s; isolating per object.",
                getattr(model_class, "__name__", model_class),
                exc,
            )
            for collapsed_change, _ in built:
                _apply_via_fallback(collapsed_change, model_class)
            return
        for collapsed_change, _ in built:
            _record_success(collapsed_change, model_class)

    def _flush():
        if not batch:
            return None
        model_label = batch_model._meta.label_lower if batch_model is not None else ""
        row_count = len(batch)
        with profile_scope(
            "bulk_application",
            model=model_label,
            owner="ours",
            rows=row_count,
        ):
            return _flush_impl()

    def _record_bulk_success(items, model_class):
        for collapsed_change in items:
            _record_success(collapsed_change, model_class)

    def _flush_deferred_fks_impl():
        nonlocal deferred_fk_batch, deferred_fk_batch_model
        if not deferred_fk_batch:
            return
        from core.choices import ObjectChangeActionChoices

        pending = deferred_fk_batch
        model_class = deferred_fk_batch_model
        deferred_fk_batch = []
        deferred_fk_batch_model = None
        allowed_fields = set(_DEFERRED_CREATE_FK_FIELDS[model_class._meta.label_lower])
        targets_by_pk = {
            target.pk: target
            for target in model_class.objects.filter(
                pk__in=[change.key[1] for change in pending]
            )
        }
        prepared = []
        noops = []
        fallback = []
        update_fields = set()
        for collapsed_change in pending:
            target = targets_by_pk.get(collapsed_change.key[1])
            if target is None:
                fallback.append(collapsed_change)
                continue
            incoming = collapsed_change.postchange_data or {}
            changed = []
            try:
                for field_name in allowed_fields:
                    if field_name not in incoming:
                        continue
                    field = model_class._meta.get_field(field_name)
                    value = _serialized_fk_id(incoming.get(field_name))
                    if getattr(target, field.attname) != value:
                        changed.append((field, value))
                if not changed:
                    noops.append(collapsed_change)
                    continue
                target.snapshot()
                for field, value in changed:
                    setattr(target, field.attname, value)
                    update_fields.add(field.name)
                _full_clean_fast(target, change_logger)
                prepared.append((collapsed_change, target))
            except JobTimeoutException:
                raise
            except Exception:  # noqa: BLE001 - isolate invalid relationship rows
                fallback.append(collapsed_change)

        if prepared:
            try:
                with transaction.atomic():
                    model_class.objects.bulk_update(
                        [target for _, target in prepared],
                        fields=sorted(update_fields),
                        batch_size=BULK_MERGE_BATCH_SIZE,
                    )
                    _emit_main_object_changes(
                        [target for _, target in prepared],
                        ObjectChangeActionChoices.ACTION_UPDATE,
                        request,
                        branch,
                    )
            except JobTimeoutException:
                raise
            except Exception:  # noqa: BLE001 - preserve row-level merge isolation
                fallback.extend(change for change, _ in prepared)
                prepared = []

        _record_bulk_success(noops, model_class)
        _record_bulk_success([change for change, _ in prepared], model_class)
        for collapsed_change in fallback:
            _apply_via_fallback(collapsed_change, model_class)

    def _flush_deferred_fks():
        if not deferred_fk_batch:
            return None
        model_label = (
            deferred_fk_batch_model._meta.label_lower
            if deferred_fk_batch_model is not None
            else ""
        )
        row_count = len(deferred_fk_batch)
        with profile_scope(
            "deferred_fk_application",
            model=model_label,
            owner="ours",
            rows=row_count,
        ):
            return _flush_deferred_fks_impl()

    def _flush_set_based_mac_impl():
        nonlocal set_based_mac_batch
        if not set_based_mac_batch:
            return
        from .merge_set_based import apply_set_based_mac_range

        pending = set_based_mac_batch
        set_based_mac_batch = []
        result = apply_set_based_mac_range(
            branch=branch,
            collapsed_changes=pending,
            request=request,
            decision=set_based_decision,
        )
        for collapsed_change in result.applied:
            _record_success(collapsed_change, collapsed_change.model_class)
        for collapsed_change in result.fallback:
            _apply_via_fallback(collapsed_change, collapsed_change.model_class)

    def _flush_set_based_mac():
        if not set_based_mac_batch:
            return None
        with profile_scope(
            "set_based_mac_application",
            model="dcim.macaddress",
            owner="ours",
            rows=len(set_based_mac_batch),
        ):
            return _flush_set_based_mac_impl()

    def _flush_prefix_impl():
        nonlocal prefix_batch, prefix_batch_action
        if not prefix_batch:
            return
        from core.choices import ObjectChangeActionChoices
        from django.db import IntegrityError
        from django.db.models.deletion import ProtectedError, RestrictedError
        from ipam.models import Prefix
        from netbox.context import current_request
        from utilities.exceptions import AbortRequest

        from .bulk_delete import collector_delete_without_model_signals

        pending = prefix_batch
        action_value = prefix_batch_action
        prefix_batch = []
        prefix_batch_action = None
        by_pk = {
            obj.pk: obj
            for obj in Prefix.objects.filter(
                pk__in=[change.key[1] for change in pending]
            )
        }

        if action_value == "update":
            prepared = []
            noops = []
            fallback = []
            for collapsed_change in pending:
                target = by_pk.get(collapsed_change.key[1])
                if target is None:
                    fallback.append(collapsed_change)
                    continue
                try:
                    dummy = collapsed_change.generate_object_change()
                    dummy.migrate(branch)
                    merge_data = dummy.get_merge_data()
                    # Forward's Prefix contract owns only status as mutable state;
                    # retain the framework path for manual branch edits to any
                    # richer field so its generic merge semantics remain exact.
                    if set(merge_data) - {"status"}:
                        fallback.append(collapsed_change)
                        continue
                    if not merge_data or target.status == merge_data.get("status"):
                        noops.append(collapsed_change)
                        continue
                    target.snapshot()
                    target.status = merge_data["status"]
                    target.full_clean(
                        validate_unique=False,
                        validate_constraints=False,
                    )
                    prepared.append((collapsed_change, target))
                except JobTimeoutException:
                    raise
                except Exception:  # noqa: BLE001 - preserve per-row merge isolation
                    fallback.append(collapsed_change)

            if prepared:
                try:
                    with transaction.atomic():
                        Prefix.objects.bulk_update(
                            [target for _, target in prepared],
                            fields=["status"],
                            batch_size=BULK_MERGE_BATCH_SIZE,
                        )
                        _emit_main_object_changes(
                            [target for _, target in prepared],
                            ObjectChangeActionChoices.ACTION_UPDATE,
                            request,
                            branch,
                        )
                except IntegrityError:
                    fallback.extend(change for change, _ in prepared)
                    prepared = []
            _record_bulk_success(noops, Prefix)
            _record_bulk_success([change for change, _ in prepared], Prefix)
            for collapsed_change in fallback:
                _apply_via_fallback(collapsed_change, Prefix)
            return

        missing = [change for change in pending if change.key[1] not in by_pk]
        present = [change for change in pending if change.key[1] in by_pk]
        _record_bulk_success(missing, Prefix)
        if not present:
            return
        targets = [by_pk[change.key[1]] for change in present]
        vrf_ids = {target.vrf_id for target in targets}
        try:
            with transaction.atomic():
                _emit_main_object_changes(
                    targets,
                    ObjectChangeActionChoices.ACTION_DELETE,
                    request,
                    branch,
                )
                request_token = current_request.set(None)
                try:
                    try:
                        collector_delete_without_model_signals(
                            Prefix.objects.filter(
                                pk__in=[target.pk for target in targets]
                            ),
                            signal_free_models={Prefix},
                        )
                    except (
                        AbortRequest,
                        IntegrityError,
                        ProtectedError,
                        RestrictedError,
                    ) as exc:
                        raise _PrefixDeleteNeedsIsolation from exc
                finally:
                    current_request.reset(request_token)
                _rebuild_main_prefix_hierarchies(vrf_ids)
        except _PrefixDeleteNeedsIsolation as exc:
            change_logger.warning(
                "Bulk merge delete batch for Prefix encountered a protected or "
                "constrained row (%s); isolating per object.",
                exc.__cause__,
            )
            for collapsed_change in present:
                _apply_via_fallback(collapsed_change, Prefix)
            return
        _record_bulk_success(present, Prefix)

    def _flush_prefix():
        if not prefix_batch:
            return None
        row_count = len(prefix_batch)
        action = str(prefix_batch_action or "")
        with profile_scope(
            f"prefix_{action}_application",
            model="ipam.prefix",
            owner="ours",
            rows=row_count,
        ):
            return _flush_prefix_impl()

    for logical_key, collapsed_change in collapsed.items():
        if collapsed_change.final_action == ActionType.SKIP:
            _record_success(collapsed_change, logical_models[logical_key])

    timed_out = False
    try:
        for collapsed_change in ordered:
            action = collapsed_change.final_action
            action_value = getattr(action, "value", action)
            if not action_value or action_value == "skip":
                continue
            model_class = collapsed_change.model_class
            models_touched.add(model_class)

            use_set_based_mac = bool(
                set_based_decision is not None
                and set_based_decision.enabled
                and model_class._meta.label_lower == "dcim.macaddress"
            )
            if use_set_based_mac:
                _flush()
                _flush_prefix()
                _flush_deferred_fks()
                set_based_mac_batch.append(collapsed_change)
                if len(set_based_mac_batch) >= BULK_MERGE_FLUSH_THRESHOLD:
                    _flush_set_based_mac()
                continue

            _flush_set_based_mac()

            if _is_deferred_fk_update(collapsed_change):
                _flush()
                _flush_prefix()
                if deferred_fk_batch and deferred_fk_batch_model is not model_class:
                    _flush_deferred_fks()
                deferred_fk_batch_model = model_class
                deferred_fk_batch.append(collapsed_change)
                if len(deferred_fk_batch) >= BULK_MERGE_FLUSH_THRESHOLD:
                    _flush_deferred_fks()
                continue

            _flush_deferred_fks()
            if model_class._meta.label_lower == "ipam.prefix" and action_value in {
                "update",
                "delete",
            }:
                _flush()
                if prefix_batch and prefix_batch_action != action_value:
                    _flush_prefix()
                prefix_batch_action = action_value
                prefix_batch.append(collapsed_change)
                if len(prefix_batch) >= BULK_MERGE_FLUSH_THRESHOLD:
                    _flush_prefix()
            elif action_value == "create" and _is_bulk_safe(model_class):
                _flush_prefix()
                if batch and batch_model is not model_class:
                    _flush()
                batch_model = model_class
                batch.append(collapsed_change)
                # Sub-batch by count so objects[]/built[] and the transaction never
                # grow with the model's total row count.
                if len(batch) >= BULK_MERGE_FLUSH_THRESHOLD:
                    _flush()
            else:
                # Other UPDATE / DELETE / MPTT create -> framework per-object apply.
                _flush()
                _flush_prefix()
                _apply_via_fallback(collapsed_change, model_class)

        _flush()
        _flush_prefix()
        _flush_deferred_fks()
        _flush_set_based_mac()
    except JobTimeoutException:
        timed_out = True
        raise
    finally:
        # Every Prefix create batch repairs its own hierarchy before commit. This
        # final pass also covers update/delete fallback and non-timeout failures.
        # RQ timeout control flow must unwind immediately so recovery retains
        # the original exception and the worker deadline remains enforceable.
        if not timed_out:
            _rebuild_main_prefix_hierarchies(affected_prefix_vrf_ids)
    missing_components = set().union(*logical_components.values()) - set(
        component_results
    )
    if missing_components:
        raise RuntimeError(
            "Bulk merge returned without accounting components: "
            + ", ".join(repr(key) for key in sorted(missing_components, key=repr))
        )
    if result_metadata is not None:
        result_metadata.update(
            logical_total=len(logical_components),
            logical_action_counts=dict(Counter(logical_actions.values())),
            # Schema identifiers and counts only — never the referencing rows.
            protected_blocked_deletes=[
                {
                    "model": key[0],
                    "held_by": [
                        {"model": label, "count": count} for label, count in references
                    ],
                }
                for key, references in sorted(
                    protected_blocked_deletes.items(), key=lambda item: repr(item[0])
                )
            ],
        )
    return applied, failed, models_touched
