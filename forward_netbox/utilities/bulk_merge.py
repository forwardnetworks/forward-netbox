# Bulk branch merge: collapse a branch's ObjectChanges to net per-object state,
# then apply CREATE batches via bulk_create instead of replaying every change
# one-by-one. This is the core of the large-dataset ingest redesign: a bootstrap
# sync of ~1M creates merges as bulk_create sub-batches instead of ~1M
# individual ObjectChange.apply() saves.
#
# Correctness is borrowed from the framework's SquashMergeStrategy:
#   - _collapse_changes      : N changes/object -> one net CollapsedChange
#   - _split_bidirectional_cycles / _build_fk_dependency_graph : FK ordering graph
# Only the *application* is changed (bulk for create-able models, per-object
# ObjectChange.apply() for MPTT/tree models and UPDATE/DELETE), AND two of the
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
from collections import defaultdict

from django.db import DEFAULT_DB_ALIAS
from django.db import IntegrityError
from django.db import transaction
from django.db.models import Q
from mptt.models import MPTTModel
from netbox_branching.merge_strategies.squash import ActionType
from netbox_branching.merge_strategies.squash import CollapsedChange
from netbox_branching.merge_strategies.squash import SquashMergeStrategy
from netbox_branching.signals import squash_dependency_graph_built

try:  # deserialize lives in NetBox core utilities
    from utilities.serialization import deserialize_object
except Exception:  # pragma: no cover - import guard for tooling outside NetBox
    deserialize_object = None

try:  # mirror full_clean_with_file_check's file-not-found suppression
    from netbox_branching.utilities import _FILE_NOT_FOUND_EXCEPTIONS
except Exception:  # pragma: no cover - branching internals moved
    _FILE_NOT_FOUND_EXCEPTIONS = ()

# SQL chunk size handed to bulk_create (splits the multi-row INSERT).
BULK_MERGE_BATCH_SIZE = 1000
# Max collapsed CREATEs held in RAM / committed per sub-batch transaction. Bounds
# peak memory, transaction/lock duration, and the existence pk__in list, and
# gives per-sub-batch resume checkpoints. Independent of model row count.
BULK_MERGE_FLUSH_THRESHOLD = 5000
# Chunk size for streaming the raw ObjectChange queryset into collapse so the
# full ~1M-row result set is never materialized at once.
BULK_MERGE_INPUT_CHUNK_SIZE = 2000

_ACTION_PRIORITY = {
    ActionType.DELETE: 0,
    ActionType.UPDATE: 1,
    ActionType.CREATE: 2,
    ActionType.SKIP: 3,
}

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


def _is_bulk_safe(model_class) -> bool:
    """A model may be bulk_create'd on merge only if it is not an MPTT tree.

    MPTT models (prefix, region, site/tenant/contact groups, …) must save per
    object so the tree fields recompute against the destination tree; bulk_create
    bypasses that and corrupts the hierarchy.
    """
    return not issubclass(model_class, MPTTModel)


def _full_clean_fast(instance, change_logger):
    """full_clean without the per-row validate_unique/validate_constraints DB
    queries (mirrors the framework's full_clean_with_file_check otherwise).

    Safe here because the bulk merge already skipped pks present in main, so the
    CREATE is known-new; a genuine constraint violation still surfaces via the
    bulk_create IntegrityError -> per-object fallback. Field-level validation is
    kept. This removes the merge's dominant per-row cost at scale.
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
    if hasattr(model_class, "deserialize_object"):
        deserialized = model_class.deserialize_object(data, pk=pk)
    else:
        deserialized = deserialize_object(model_class, data, pk=pk)
    _full_clean_fast(deserialized.object, change_logger)
    return deserialized


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
        for collapsed in items:
            if collapsed.key[1] not in present:
                change_logger.info(
                    "  Skipping UPDATE for %s:%s (object deleted in main)",
                    collapsed.model_class.__name__,
                    collapsed.key[1],
                )
                collapsed.final_action = ActionType.SKIP


def _order_collapsed_changes_fast(collapsed_changes, change_logger, operation):
    """O((V+E) log V) replacement for ``_order_collapsed_changes``.

    Reuses the framework's SKIP filtering, bidirectional-cycle splitting, FK
    dependency-graph build, and the ``squash_dependency_graph_built`` signal;
    only the topological sort is replaced. The framework sort is O(V^2) — it
    rescans every remaining node per layer and discards the processed key from
    every remaining node — and hangs on a single large model batch even with no
    edges. This uses Kahn's algorithm with a reverse-adjacency index built from
    ``depends_on`` and a heap so ties keep the DELETE->UPDATE->CREATE (then time)
    preference. Returns an ordered list of CollapsedChange.
    """
    to_process = {
        k: v for k, v in collapsed_changes.items() if v.final_action != ActionType.SKIP
    }
    if not to_process:
        return []

    # Build the FK dependency graph exactly as the framework does (populates each
    # CollapsedChange.depends_on). Cheap O(V * fields); reused verbatim.
    SquashMergeStrategy._split_bidirectional_cycles(to_process, change_logger)
    # Then split deferred self-referential FKs (primary_ip4/6, oob_ip, VC master)
    # out of CREATEs — these form 3-node cycles the framework's 2-node split misses
    # (device -> primary_ip -> assigned interface -> device). Must run before the
    # graph build below so the cycle edge is never created.
    _defer_self_referential_create_fks(to_process, change_logger)
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
    SquashMergeStrategy._build_fk_dependency_graph(
        deletes, updates, creates, change_logger
    )
    squash_dependency_graph_built.send(
        sender=SquashMergeStrategy,
        collapsed_changes=to_process,
        operation=operation,
    )

    # Authoritative reverse index + in-degree, both from depends_on filtered to
    # in-scope keys (so a dependency on a SKIP'd/absent node never wedges).
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
                    _ACTION_PRIORITY.get(collapsed.final_action, 99),
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
                        _ACTION_PRIORITY.get(sv.final_action, 99),
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
        SquashMergeStrategy._log_cycle_details(remaining, to_process, change_logger)
        raise Exception(
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
):
    """Merge ``changes`` (a branch's unmerged ObjectChanges) into main.

    ``apply_one(collapsed_change) -> bool`` is the caller's per-object fallback
    (the existing savepoint-isolated ObjectChange.apply path); it returns True on
    success, False if the row was recorded as an issue. ``record_applied(model)``
    is an optional per-applied-row callback for stats/heartbeat.

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
    collapsed, _ = SquashMergeStrategy._collapse_changes(change_iter, change_logger)
    _skip_updates_missing_in_main_batched(collapsed, change_logger)
    ordered = _order_collapsed_changes_fast(collapsed, change_logger, "merge")

    applied = 0
    failed = 0
    models_touched = set()
    batch = []  # collapsed CREATE changes for the current model (not yet built)
    batch_model = None

    def _apply_via_fallback(collapsed_change, model_class):
        nonlocal applied, failed
        # Isolate a single object's apply failure (e.g. a row deleted in main
        # while the branch modified it, or any per-object validation conflict) so
        # one bad change never fails the whole sync — the merge must be resilient
        # for steady-state diffs. Mirrors the bulk-create IntegrityError isolation.
        try:
            ok = apply_one(collapsed_change)
        except Exception as exc:  # noqa: BLE001 - isolate one object, keep merging
            change_logger.warning(
                "Bulk merge: isolating %s change for %s after apply error: %s",
                getattr(getattr(collapsed_change, "final_action", None), "value", "?"),
                getattr(model_class, "__name__", model_class),
                exc,
            )
            failed += 1
            return
        if ok:
            applied += 1
            if record_applied:
                record_applied(model_class)
        else:
            failed += 1

    def _flush():
        nonlocal applied, batch, batch_model
        if not batch:
            return
        model_class = batch_model
        pending = batch
        batch = []
        batch_model = None

        # Resume idempotency: a fast bulk merge that crashed mid-way re-runs the
        # whole branch. Skip pks already present in main so re-created rows do not
        # re-validate (duplicate-unique) or raise duplicate-pk. This must happen
        # BEFORE deserialize/full_clean, which would fail validate_unique against
        # the already-merged row. The pk__in list is bounded by the sub-batch
        # flush threshold, so it never grows with model row count.
        all_pks = [c.key[1] for c in pending]
        existing = set(
            model_class.objects.filter(pk__in=all_pks).values_list("pk", flat=True)
        )
        # Tags collide by NAME, not pk: while the merge applies device UPDATEs
        # (ordered before CREATEs), netbox_branching sets device tags by name,
        # which get_or_creates the tag on main with a NEW pk. The branch's tag
        # CREATE then violates the unique name/slug constraint. Treat a
        # same-named (or same-slug) main-side tag as already merged. If the
        # branch create carried different non-unique attrs (color/description),
        # they converge on the next sync: its branch is provisioned from a main
        # that has the tag, so the apply-time coalesce records an UPDATE.
        existing_names: set[str] = set()
        existing_slugs: set[str] = set()
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
            for name, slug in model_class.objects.filter(name_filter).values_list(
                "name", "slug"
            ):
                existing_names.add(str(name))
                existing_slugs.add(str(slug))
        skipped = 0
        objects = []
        built = []  # (collapsed, deserialized) for rows we actually create
        for collapsed_change in pending:
            if collapsed_change.key[1] in existing:
                skipped += 1
                continue
            if existing_names or existing_slugs:
                data = collapsed_change.postchange_data or {}
                if (
                    str(data.get("name") or "") in existing_names
                    or str(data.get("slug") or "") in existing_slugs
                ):
                    skipped += 1
                    continue
            try:
                deserialized = _deserialize(
                    model_class, collapsed_change, change_logger
                )
            except Exception:  # noqa: BLE001 - validation/deserialize failure
                _apply_via_fallback(collapsed_change, model_class)
                continue
            built.append((collapsed_change, deserialized))
            objects.append(deserialized.object)

        if skipped:
            applied += skipped
            if record_applied:
                for _ in range(skipped):
                    record_applied(model_class)
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
            applied += len(built)
            if record_applied:
                for _ in built:
                    record_applied(model_class)
        except IntegrityError as exc:
            # A row violates a constraint and rolled this sub-batch back. Fall
            # back to the per-object path so good rows apply and the offender is
            # isolated as an issue.
            change_logger.warning(
                "Bulk merge create batch for %s hit %s; isolating per object.",
                getattr(model_class, "__name__", model_class),
                exc,
            )
            for collapsed_change, _ in built:
                _apply_via_fallback(collapsed_change, model_class)

    for collapsed_change in ordered:
        action = collapsed_change.final_action
        action_value = getattr(action, "value", action)
        if not action_value or action_value == "skip":
            continue
        model_class = collapsed_change.model_class
        models_touched.add(model_class)

        if action_value == "create" and _is_bulk_safe(model_class):
            if batch and batch_model is not model_class:
                _flush()
            batch_model = model_class
            batch.append(collapsed_change)
            # Sub-batch by count so objects[]/built[] and the transaction never
            # grow with the model's total row count.
            if len(batch) >= BULK_MERGE_FLUSH_THRESHOLD:
                _flush()
        else:
            # UPDATE / DELETE / MPTT create -> framework per-object apply.
            _flush()
            _apply_via_fallback(collapsed_change, model_class)

    _flush()
    return applied, failed, models_touched
