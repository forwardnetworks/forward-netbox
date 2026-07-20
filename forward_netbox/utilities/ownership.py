from collections import Counter
from collections import defaultdict
from contextlib import contextmanager

from django.contrib.contenttypes.models import ContentType
from django.db import connection
from django.db import transaction
from django.db.models import F
from django.db.models.deletion import ProtectedError
from django.utils import timezone
from django.utils.text import slugify

from .tag_contracts import RESERVED_STATUS_TAG_SLUGS
from .tag_contracts import validate_scope_tag_names


OWNERSHIP_ADVISORY_LOCK_ID = 0x4657444F574E
DEVICE_IDENTITY_CANDIDATES_KEY = "device_identity_candidates"


class OwnershipConflictError(RuntimeError):
    """Raised after durable claims expose incompatible desired relationships."""


def _object_pk(value):
    try:
        return int(value)
    except (TypeError, ValueError):
        return value


@contextmanager
def ownership_write_lock():
    """Serialize global claim materialization across sources and workers."""
    with transaction.atomic():
        with connection.cursor() as cursor:
            cursor.execute(
                "SELECT pg_advisory_xact_lock(%s)",
                [OWNERSHIP_ADVISORY_LOCK_ID],
            )
        yield


def latest_baseline_generation(sync):
    ingestion = sync.latest_baseline_ingestion()
    if ingestion is None:
        return None
    return {
        "generation": ingestion.pk,
        "snapshot_id": str(ingestion.snapshot_id or "").strip(),
    }


def record_device_identity_candidates(ingestion, candidates):
    """Persist staged source-key to Device-PK evidence on the ingestion."""
    normalized = {
        (str(source_key or "").strip(), _object_pk(device_id))
        for source_key, device_id in candidates
        if str(source_key or "").strip() and device_id not in (None, "")
    }
    if not normalized:
        return 0
    with transaction.atomic():
        locked = ingestion.__class__.objects.select_for_update().get(pk=ingestion.pk)
        snapshot_info = dict(locked.snapshot_info or {})
        existing = {
            (
                str(item.get("source_device_key") or "").strip(),
                _object_pk(item.get("device_id")),
            )
            for item in snapshot_info.get(DEVICE_IDENTITY_CANDIDATES_KEY, [])
            if isinstance(item, dict)
        }
        existing.update(normalized)
        snapshot_info[DEVICE_IDENTITY_CANDIDATES_KEY] = [
            {"source_device_key": source_key, "device_id": device_id}
            for source_key, device_id in sorted(existing, key=lambda item: item[0])
        ]
        locked.snapshot_info = snapshot_info
        locked.save(update_fields=["snapshot_info"])
    ingestion.snapshot_info = snapshot_info
    return len(normalized)


def finalize_device_identities_locked(ingestion):
    """Materialize staged identities while the ownership transaction is locked."""
    from dcim.models import Device

    from ..models import ForwardDeviceIdentity

    candidates = (ingestion.snapshot_info or {}).get(DEVICE_IDENTITY_CANDIDATES_KEY, [])
    by_key = defaultdict(set)
    for item in candidates:
        if not isinstance(item, dict):
            continue
        source_key = str(item.get("source_device_key") or "").strip()
        device_id = _object_pk(item.get("device_id"))
        if source_key and device_id not in (None, ""):
            by_key[source_key].add(device_id)
    ambiguous = sorted(
        key for key, device_ids in by_key.items() if len(device_ids) != 1
    )
    if ambiguous:
        raise OwnershipConflictError(
            "Forward device identity is ambiguous for source key(s): "
            + ", ".join(ambiguous[:10])
        )

    desired = {key: next(iter(device_ids)) for key, device_ids in by_key.items()}
    devices = Device.objects.in_bulk(desired.values())
    missing = sorted(
        key for key, device_id in desired.items() if device_id not in devices
    )
    mismatched = sorted(
        key
        for key, device_id in desired.items()
        if device_id in devices and str(devices[device_id].name or "") != key
    )
    if missing or mismatched:
        raise OwnershipConflictError(
            "Forward device identity evidence does not match merged NetBox rows: "
            + ", ".join((missing + mismatched)[:10])
        )

    for source_key, device_id in desired.items():
        current = ForwardDeviceIdentity.objects.filter(
            sync=ingestion.sync,
            source_device_key=source_key,
        ).first()
        if current is not None and current.device_id != device_id:
            if Device.objects.filter(pk=current.device_id).exists():
                raise OwnershipConflictError(
                    f"Forward source key `{source_key}` maps to multiple live NetBox devices."
                )
            current.delete()
        device_identity = (
            ForwardDeviceIdentity.objects.filter(
                sync=ingestion.sync,
                device_id=device_id,
            )
            .exclude(source_device_key=source_key)
            .first()
        )
        if device_identity is not None:
            raise OwnershipConflictError(
                f"NetBox device {device_id} is already mapped to Forward source key "
                f"`{device_identity.source_device_key}`."
            )
        ForwardDeviceIdentity.objects.update_or_create(
            sync=ingestion.sync,
            source_device_key=source_key,
            defaults={
                "device_id": device_id,
                "ingestion_id": ingestion.pk,
                "snapshot_id": str(ingestion.snapshot_id or "").strip(),
            },
        )
    return len(desired)


def resolve_device_identities(sync, source_device_keys, *, generation, snapshot_id):
    """Resolve exact device PKs and persist globally unique pre-identity rows."""
    from dcim.models import Device

    from ..models import ForwardDeviceIdentity

    keys = {str(value or "").strip() for value in source_device_keys}
    keys.discard("")
    identities = {
        item.source_device_key: item.device_id
        for item in ForwardDeviceIdentity.objects.filter(
            sync=sync,
            source_device_key__in=keys,
        )
    }
    unresolved = keys - set(identities)
    candidates = defaultdict(list)
    for device_id, name in Device.objects.filter(name__in=unresolved).values_list(
        "pk", "name"
    ):
        candidates[str(name)].append(device_id)
    ambiguous = sorted(key for key, values in candidates.items() if len(values) > 1)
    missing = sorted(key for key in unresolved if not candidates.get(key))
    adoptable = {
        key: values[0] for key, values in candidates.items() if len(values) == 1
    }
    if adoptable:
        with ownership_write_lock():
            for source_key, device_id in adoptable.items():
                ForwardDeviceIdentity.objects.update_or_create(
                    sync=sync,
                    source_device_key=source_key,
                    defaults={
                        "device_id": device_id,
                        "ingestion_id": int(generation),
                        "snapshot_id": str(snapshot_id or "").strip(),
                    },
                )
        identities.update(adoptable)
    return identities, missing, ambiguous


def _generation_values(sync, generation=None, snapshot_id=None):
    if generation is None:
        latest = latest_baseline_generation(sync)
        if latest is None:
            raise RuntimeError(
                "Ownership reconciliation requires a baseline ingestion."
            )
        generation = latest["generation"]
        snapshot_id = snapshot_id or latest["snapshot_id"]
    return int(generation), str(snapshot_id or "").strip()


def _domain_for_claim_type(claim_type):
    from ..models import ForwardOwnershipReconciliation

    if claim_type == "scope":
        return ForwardOwnershipReconciliation.Domain.SCOPE_TAGS
    return ForwardOwnershipReconciliation.Domain.STATUS_TAGS


def _mark_reconciled(sync, domain, generation, snapshot_id):
    from ..models import ForwardOwnershipReconciliation

    ForwardOwnershipReconciliation.objects.update_or_create(
        sync=sync,
        domain=domain,
        defaults={
            "ingestion_id": generation,
            "snapshot_id": snapshot_id,
            "status": ForwardOwnershipReconciliation.Status.COMPLETED,
            "error_type": "",
            "started_at": timezone.now(),
            "completed_at": timezone.now(),
        },
    )


def _mark_reconciliation_pending(sync, domain, generation, snapshot_id):
    from ..models import ForwardOwnershipReconciliation

    ForwardOwnershipReconciliation.objects.update_or_create(
        sync=sync,
        domain=domain,
        defaults={
            "ingestion_id": generation,
            "snapshot_id": snapshot_id,
            "status": ForwardOwnershipReconciliation.Status.PENDING,
            "error_type": "",
            "started_at": timezone.now(),
            "completed_at": None,
        },
    )


def required_ownership_domains(sync):
    """Return domains this sync must reconcile after its latest ingestion."""
    from ..models import ForwardDeviceTagClaim
    from ..models import ForwardOwnershipReconciliation
    from ..models import ForwardVirtualParentClaim

    required = []
    source_parameters = getattr(sync.source, "parameters", None) or {}
    if (
        source_parameters.get("apply_device_scope_tags")
        or ForwardDeviceTagClaim.objects.filter(sync=sync, claim_type="scope").exists()
        or ForwardOwnershipReconciliation.objects.filter(
            sync=sync,
            domain=ForwardOwnershipReconciliation.Domain.SCOPE_TAGS,
        ).exists()
    ):
        required.append(ForwardOwnershipReconciliation.Domain.SCOPE_TAGS)
    # Backfilled/out-of-scope ownership is part of every completed ingestion,
    # so stale status labels never depend on a retired opt-in control.
    required.append(ForwardOwnershipReconciliation.Domain.STATUS_TAGS)
    if (
        (sync.parameters or {}).get("auto_link_vsys_parents") is not False
        or ForwardVirtualParentClaim.objects.filter(sync=sync).exists()
        or ForwardOwnershipReconciliation.objects.filter(
            sync=sync,
            domain=ForwardOwnershipReconciliation.Domain.VIRTUAL_PARENTS,
        ).exists()
    ):
        required.append(ForwardOwnershipReconciliation.Domain.VIRTUAL_PARENTS)
    return required


def mark_ownership_pending(sync, generation, snapshot_id, *, domains=None):
    """Persist required post-merge work before any queue operation."""
    generation = int(generation)
    domains = list(domains or required_ownership_domains(sync))
    with ownership_write_lock():
        _mark_ownership_pending_locked(sync, generation, snapshot_id, domains)
    return domains


def _mark_ownership_pending_locked(sync, generation, snapshot_id, domains):
    """Write pending rows while the caller holds ``ownership_write_lock``."""
    from ..models import ForwardOwnershipReconciliation

    for domain in domains:
        current = ForwardOwnershipReconciliation.objects.filter(
            sync=sync,
            domain=domain,
        ).first()
        if current is not None and current.generation > generation:
            continue
        if (
            current is not None
            and current.generation == generation
            and current.status == ForwardOwnershipReconciliation.Status.COMPLETED
        ):
            continue
        ForwardOwnershipReconciliation.objects.update_or_create(
            sync=sync,
            domain=domain,
            defaults={
                "ingestion_id": generation,
                "snapshot_id": str(snapshot_id or "").strip(),
                "status": ForwardOwnershipReconciliation.Status.PENDING,
                "error_type": "",
                "started_at": timezone.now(),
                "completed_at": None,
            },
        )


def mark_ownership_failed(sync, generation, domains, exc):
    """Retain aggregate failure evidence without persisting customer data."""
    from ..models import ForwardOwnershipReconciliation

    generation = int(generation or 0)
    if not generation:
        return
    with ownership_write_lock():
        ForwardOwnershipReconciliation.objects.filter(
            sync=sync,
            domain__in=list(domains),
            ingestion_id=generation,
        ).update(
            status=ForwardOwnershipReconciliation.Status.FAILED,
            error_type=exc.__class__.__name__[:100],
            completed_at=timezone.now(),
        )


def _configured_include_tags(source):
    parameters = dict(source.parameters or {})
    include_tags = parameters.get("device_tag_include_tags") or []
    return {str(value).strip() for value in include_tags if str(value).strip()}


def _relevant_sync_ids(domain, *, tag_id=None, excluded_sync_ids=()):
    from extras.models import Tag

    from ..models import ForwardDeviceTagClaim
    from ..models import ForwardIngestion
    from ..models import ForwardOwnershipReconciliation
    from ..models import ForwardSync
    from ..models import ForwardVirtualParentClaim

    excluded_sync_ids = set(excluded_sync_ids)
    baseline_sync_ids = set(
        ForwardIngestion.objects.filter(baseline_ready=True)
        .exclude(snapshot_id="")
        .values_list("sync_id", flat=True)
    )
    scoped_tag = Tag.objects.filter(pk=tag_id).first() if tag_id is not None else None
    configured = set()
    for sync in ForwardSync.objects.select_related("source").only(
        "pk", "parameters", "source__parameters"
    ):
        if sync.pk in excluded_sync_ids:
            continue
        if domain == ForwardOwnershipReconciliation.Domain.SCOPE_TAGS:
            source_parameters = sync.source.parameters or {}
            if source_parameters.get("apply_device_scope_tags"):
                configured_names = _configured_include_tags(sync.source)
                configured_slugs = {
                    slugify(name) or slugify(name.replace(".", "-"))
                    for name in configured_names
                }
                if tag_id is None or (
                    scoped_tag is not None
                    and (
                        scoped_tag.name in configured_names
                        or scoped_tag.slug in configured_slugs
                    )
                ):
                    configured.add(sync.pk)
        elif domain == ForwardOwnershipReconciliation.Domain.STATUS_TAGS:
            configured.add(sync.pk)
        elif (sync.parameters or {}).get("auto_link_vsys_parents") is not False:
            configured.add(sync.pk)

    # A configured sync does not participate in global ownership until it has
    # produced a durable baseline. Claims and reconciliation history below keep
    # previously participating syncs relevant while their state is converging.
    configured &= baseline_sync_ids

    if domain == ForwardOwnershipReconciliation.Domain.VIRTUAL_PARENTS:
        claim_sync_ids = set(
            ForwardVirtualParentClaim.objects.values_list("sync_id", flat=True)
        )
    else:
        if domain == ForwardOwnershipReconciliation.Domain.SCOPE_TAGS:
            claims = ForwardDeviceTagClaim.objects.filter(claim_type="scope")
        else:
            claims = ForwardDeviceTagClaim.objects.exclude(claim_type="scope")
        if tag_id is not None:
            claims = claims.filter(tag_id=tag_id)
        claim_sync_ids = set(claims.values_list("sync_id", flat=True))
    reconciled_sync_ids = set(
        ForwardOwnershipReconciliation.objects.filter(domain=domain).values_list(
            "sync_id", flat=True
        )
    )
    return (configured | claim_sync_ids | reconciled_sync_ids) - excluded_sync_ids


def _domain_is_current(
    domain,
    *,
    tag_id=None,
    excluded_sync_ids=(),
    candidate_sync_id=None,
    candidate_generation=None,
):
    from ..models import ForwardOwnershipReconciliation
    from ..models import ForwardSync

    relevant_ids = _relevant_sync_ids(
        domain,
        tag_id=tag_id,
        excluded_sync_ids=excluded_sync_ids,
    )
    if not relevant_ids:
        return False
    reconciled = {
        sync_id: (generation, status)
        for sync_id, generation, status in (
            ForwardOwnershipReconciliation.objects.filter(
                sync_id__in=relevant_ids,
                domain=domain,
            ).values_list("sync_id", "ingestion_id", "status")
        )
    }
    for sync in ForwardSync.objects.filter(pk__in=relevant_ids):
        latest = latest_baseline_generation(sync)
        if sync.pk == candidate_sync_id:
            if latest is None or latest["generation"] != candidate_generation:
                return False
            continue
        if latest is None or reconciled.get(sync.pk) != (
            latest["generation"],
            ForwardOwnershipReconciliation.Status.COMPLETED,
        ):
            return False
    return True


def _domain_is_ready(domain):
    """Treat an unused domain as current for cross-domain materialization."""
    relevant_ids = _relevant_sync_ids(domain)
    return not relevant_ids or _domain_is_current(domain)


def _ensure_managed_tag(
    tag,
    claim_type,
    *,
    allow_reserved_adoption=False,
    plugin_assignment_ids=(),
):
    from ..models import ForwardManagedDeviceTag
    from ..models import ForwardPreservedDeviceTagAssignment

    managed = ForwardManagedDeviceTag.objects.filter(
        tag=tag,
        claim_type=claim_type,
    ).first()
    if managed is not None:
        return managed
    conflicting = ForwardManagedDeviceTag.objects.filter(tag=tag).first()
    if conflicting is not None:
        raise OwnershipConflictError(
            f"Tag slug `{tag.slug}` is already controlled as "
            f"`{conflicting.claim_type}` and cannot also be `{claim_type}`."
        )
    if (
        claim_type in {"backfilled", "out_of_scope"}
        and tag.slug in RESERVED_STATUS_TAG_SLUGS
        and not allow_reserved_adoption
    ):
        raise OwnershipConflictError(
            f"Tag slug `{tag.slug}` is reserved for Forward status ownership but "
            "already exists without plugin provenance."
        )
    preserved_ids = _tag_assignment_device_ids(tag) - set(plugin_assignment_ids)
    managed = ForwardManagedDeviceTag.objects.create(
        tag=tag,
        claim_type=claim_type,
    )
    ForwardPreservedDeviceTagAssignment.objects.bulk_create(
        [
            ForwardPreservedDeviceTagAssignment(device_id=device_id, tag=tag)
            for device_id in preserved_ids
        ],
        ignore_conflicts=True,
        batch_size=2000,
    )
    return managed


def _tag_assignment_device_ids(tag):
    from dcim.models import Device
    from extras.models import TaggedItem

    device_content_type = ContentType.objects.get_for_model(Device)
    return set(
        TaggedItem.objects.filter(
            content_type=device_content_type,
            tag_id=tag.pk,
        ).values_list("object_id", flat=True)
    )


def _desired_tag_device_ids(
    tag_id,
    claim_type,
    *,
    excluded_sync_ids=(),
):
    from ..models import ForwardDeviceTagClaim

    claims = ForwardDeviceTagClaim.objects.filter(
        tag_id=tag_id,
        claim_type=claim_type,
    ).exclude(sync_id__in=excluded_sync_ids)
    desired_ids = set(claims.values_list("device_id", flat=True))
    if claim_type == ForwardDeviceTagClaim.ClaimType.OUT_OF_SCOPE:
        positively_scoped_ids = set(
            ForwardDeviceTagClaim.objects.filter(claim_type="scope")
            .exclude(sync_id__in=excluded_sync_ids)
            .values_list("device_id", flat=True)
        )
        desired_ids -= positively_scoped_ids
    return desired_ids


def _materialize_managed_tag(
    managed_tag,
    *,
    excluded_sync_ids=(),
    force_current=False,
    candidate_sync_id=None,
    candidate_generation=None,
):
    from dcim.models import Device

    from ..models import ForwardPreservedDeviceTagAssignment

    desired_ids = _desired_tag_device_ids(
        managed_tag.tag_id,
        managed_tag.claim_type,
        excluded_sync_ids=excluded_sync_ids,
    )
    assigned_ids = _tag_assignment_device_ids(managed_tag.tag)
    preserved_assignments = ForwardPreservedDeviceTagAssignment.objects.filter(
        tag_id=managed_tag.tag_id
    )
    preserved_ids = set(preserved_assignments.values_list("device_id", flat=True))
    removed_preserved_ids = preserved_ids - assigned_ids
    if removed_preserved_ids:
        preserved_assignments.filter(device_id__in=removed_preserved_ids).delete()
    desired_ids.update(preserved_ids & assigned_ids)

    added = 0
    for device in Device.objects.filter(pk__in=desired_ids - assigned_ids):
        device.tags.add(managed_tag.tag)
        added += 1

    removed = 0
    domain = _domain_for_claim_type(managed_tag.claim_type)
    current = force_current or _domain_is_current(
        domain,
        tag_id=managed_tag.tag_id,
        excluded_sync_ids=excluded_sync_ids,
        candidate_sync_id=candidate_sync_id,
        candidate_generation=candidate_generation,
    )
    if current:
        for device in Device.objects.filter(pk__in=assigned_ids - desired_ids):
            device.tags.remove(managed_tag.tag)
            removed += 1
    return {
        "assignments_added": added,
        "assignments_removed": removed,
        "current": current,
    }


def ensure_device_tag_claim(
    sync,
    device,
    tag,
    claim_type,
    *,
    generation=None,
    snapshot_id=None,
    add_assignment=None,
):
    """Assert latest-ingestion ownership on main and materialize its tag."""
    from ..models import ForwardDeviceTagClaim

    generation, snapshot_id = _generation_values(sync, generation, snapshot_id)
    with ownership_write_lock():
        managed_tag = _ensure_managed_tag(tag, claim_type)
        claim, created = ForwardDeviceTagClaim.objects.update_or_create(
            sync=sync,
            device=device,
            tag=tag,
            claim_type=claim_type,
            defaults={"ingestion_id": generation, "snapshot_id": snapshot_id},
        )
        assignment_added = False
        if not device.tags.filter(pk=tag.pk).exists():
            if add_assignment is None:
                device.tags.add(tag)
            else:
                add_assignment(device, tag)
            assignment_added = True
        return {
            "claim": claim,
            "claim_created": created,
            "assignment_added": assignment_added,
            "managed_tag": managed_tag,
        }


def release_device_tag_claim(
    sync,
    device,
    tag,
    claim_type,
    *,
    remove_assignment=None,
):
    """Release one sync claim; removal waits for globally current evidence."""
    from ..models import ForwardDeviceTagClaim

    with ownership_write_lock():
        deleted, _ = ForwardDeviceTagClaim.objects.filter(
            sync=sync,
            device=device,
            tag=tag,
            claim_type=claim_type,
        ).delete()
        managed_tag = _ensure_managed_tag(tag, claim_type)
        materialized = _materialize_managed_tag(managed_tag)
        return {
            "claim_released": bool(deleted),
            "assignment_removed": bool(materialized["assignments_removed"]),
        }


def reconcile_source_device_tag_claims(
    sync,
    device_names,
    *,
    slug,
    name,
    color,
    description,
    claim_type,
    generation=None,
    snapshot_id=None,
    mark_domain=True,
    materialize=True,
):
    """Replace this sync's claims for one managed tag from exact snapshot data."""
    from extras.models import Tag

    from ..models import ForwardDeviceTagClaim

    generation, snapshot_id = _generation_values(sync, generation, snapshot_id)
    identities, missing, ambiguous = resolve_device_identities(
        sync,
        device_names,
        generation=generation,
        snapshot_id=snapshot_id,
    )
    if missing or ambiguous:
        raise OwnershipConflictError(
            "Refusing name-only tag mutation because device identity is unresolved "
            "or ambiguous: " + ", ".join((ambiguous + missing)[:10])
        )
    desired_ids = set(identities.values())
    with ownership_write_lock():
        tag = Tag.objects.filter(slug=slug).first()
        tag_created = False
        if tag is None and desired_ids:
            tag = Tag.objects.create(
                slug=slug,
                name=name,
                color=color,
                description=description,
            )
            tag_created = True
        if tag is None:
            finalized = {
                "assignments_added": 0,
                "assignments_removed": 0,
                "current": False,
            }
            if mark_domain:
                finalized = finalize_device_tag_domain(
                    sync,
                    _domain_for_claim_type(claim_type),
                    generation,
                    snapshot_id,
                )
            return {
                "claims_added": 0,
                "claims_released": 0,
                "assignments_added": finalized["assignments_added"],
                "assignments_removed": finalized["assignments_removed"],
                "total": len(desired_ids),
                "current": finalized["current"],
            }

        _ensure_managed_tag(
            tag,
            claim_type,
            allow_reserved_adoption=tag_created,
            plugin_assignment_ids=desired_ids,
        )
        current_ids = set(
            ForwardDeviceTagClaim.objects.filter(
                sync=sync,
                tag=tag,
                claim_type=claim_type,
            ).values_list("device_id", flat=True)
        )
        released, _ = ForwardDeviceTagClaim.objects.filter(
            sync=sync,
            tag=tag,
            claim_type=claim_type,
            device_id__in=current_ids - desired_ids,
        ).delete()
        ForwardDeviceTagClaim.objects.filter(
            sync=sync,
            tag=tag,
            claim_type=claim_type,
            device_id__in=current_ids & desired_ids,
        ).update(ingestion_id=generation, snapshot_id=snapshot_id)
        new_ids = desired_ids - current_ids
        ForwardDeviceTagClaim.objects.bulk_create(
            [
                ForwardDeviceTagClaim(
                    sync=sync,
                    device_id=device_id,
                    tag=tag,
                    claim_type=claim_type,
                    ingestion_id=generation,
                    snapshot_id=snapshot_id,
                )
                for device_id in new_ids
            ],
            batch_size=2000,
        )
        if materialize:
            materialized = finalize_device_tag_domain(
                sync,
                _domain_for_claim_type(claim_type),
                generation,
                snapshot_id,
            )
        else:
            materialized = {
                "assignments_added": 0,
                "assignments_removed": 0,
                "current": False,
            }
        return {
            "claims_added": len(new_ids),
            "claims_released": released,
            **materialized,
            "total": len(desired_ids),
        }


def finalize_device_tag_domain(
    sync,
    domain,
    generation,
    snapshot_id,
    *,
    tag_ids=None,
):
    """Materialize a complete tag domain, then persist its final state."""
    from ..models import ForwardManagedDeviceTag
    from ..models import ForwardOwnershipReconciliation

    claim_types = (
        ["scope"]
        if domain == ForwardOwnershipReconciliation.Domain.SCOPE_TAGS
        else ["backfilled", "out_of_scope"]
    )
    managed_tags = ForwardManagedDeviceTag.objects.filter(claim_type__in=claim_types)
    if tag_ids is not None:
        managed_tags = managed_tags.filter(tag_id__in=set(tag_ids))
    added = 0
    removed = 0
    by_claim_type = defaultdict(
        lambda: {"assignments_added": 0, "assignments_removed": 0}
    )
    current = True
    found = False
    for managed_tag in managed_tags.select_related("tag"):
        found = True
        result = _materialize_managed_tag(
            managed_tag,
            candidate_sync_id=sync.pk,
            candidate_generation=generation,
        )
        added += result["assignments_added"]
        removed += result["assignments_removed"]
        by_claim_type[managed_tag.claim_type]["assignments_added"] += result[
            "assignments_added"
        ]
        by_claim_type[managed_tag.claim_type]["assignments_removed"] += result[
            "assignments_removed"
        ]
        current = current and result["current"]
    if not found:
        current = _domain_is_current(
            domain,
            candidate_sync_id=sync.pk,
            candidate_generation=generation,
        )
    # The row records that this sync's claims are current. Destructive
    # materialization remains gated on every relevant sync reaching its latest
    # baseline, so concurrent reconciliations cannot deadlock each other in a
    # permanently pending state.
    _mark_reconciled(sync, domain, generation, snapshot_id)
    if not current and _domain_is_current(domain):
        current = True
        for managed_tag in managed_tags.select_related("tag"):
            result = _materialize_managed_tag(managed_tag)
            added += result["assignments_added"]
            removed += result["assignments_removed"]
            by_claim_type[managed_tag.claim_type]["assignments_added"] += result[
                "assignments_added"
            ]
            by_claim_type[managed_tag.claim_type]["assignments_removed"] += result[
                "assignments_removed"
            ]
    if (
        domain
        in (
            ForwardOwnershipReconciliation.Domain.SCOPE_TAGS,
            ForwardOwnershipReconciliation.Domain.STATUS_TAGS,
        )
        and _domain_is_ready(ForwardOwnershipReconciliation.Domain.SCOPE_TAGS)
        and _domain_is_ready(ForwardOwnershipReconciliation.Domain.STATUS_TAGS)
    ):
        for managed_tag in ForwardManagedDeviceTag.objects.filter(
            claim_type="out_of_scope"
        ).select_related("tag"):
            result = _materialize_managed_tag(managed_tag, force_current=True)
            added += result["assignments_added"]
            removed += result["assignments_removed"]
            by_claim_type[managed_tag.claim_type]["assignments_added"] += result[
                "assignments_added"
            ]
            by_claim_type[managed_tag.claim_type]["assignments_removed"] += result[
                "assignments_removed"
            ]
    return {
        "assignments_added": added,
        "assignments_removed": removed,
        "current": current,
        "by_claim_type": dict(by_claim_type),
    }


def reconcile_sync_scope_tag_claims(
    sync, matched_tags_by_device, *, generation, snapshot_id
):
    """Replace every configured managed-scope tag claim for one sync generation."""
    from extras.models import Tag

    from ..models import ForwardDeviceTagClaim
    from ..models import ForwardManagedDeviceTag
    from ..models import ForwardOwnershipReconciliation

    generation, snapshot_id = _generation_values(sync, generation, snapshot_id)
    desired_by_name = defaultdict(set)
    for device_name, tag_names in (matched_tags_by_device or {}).items():
        for tag_name in tag_names:
            desired_by_name[str(tag_name)].add(str(device_name))

    with ownership_write_lock():
        configured_names = _configured_include_tags(sync.source)
        normalized_slugs = validate_scope_tag_names(
            configured_names | set(desired_by_name)
        )
        managed_tag_ids = set(
            ForwardManagedDeviceTag.objects.filter(claim_type="scope").values_list(
                "tag_id", flat=True
            )
        )
        old_tag_ids = set(
            ForwardDeviceTagClaim.objects.filter(
                sync=sync,
                claim_type="scope",
            ).values_list("tag_id", flat=True)
        )
        tags = {}
        for name in configured_names | set(desired_by_name):
            tag_slug = normalized_slugs[name]
            tag = Tag.objects.filter(slug=tag_slug).first() if tag_slug else None
            if tag is None:
                tag = Tag.objects.create(
                    name=name,
                    slug=tag_slug,
                    color="9e9e9e",
                )
            tags[name] = tag
            managed_tag_ids.add(tag.pk)

        desired_tag_ids = {tag.pk for tag in tags.values()}
        stale_tag_ids = old_tag_ids - desired_tag_ids
        released = 0
        if stale_tag_ids:
            released, _ = ForwardDeviceTagClaim.objects.filter(
                sync=sync,
                claim_type="scope",
                tag_id__in=stale_tag_ids,
            ).delete()

        desired_by_tag_id = defaultdict(set)
        tag_by_id = {}
        for name, tag in tags.items():
            desired_by_tag_id[tag.pk].update(desired_by_name.get(name, set()))
            tag_by_id[tag.pk] = tag

        added = 0
        for tag_id, tag in tag_by_id.items():
            result = reconcile_source_device_tag_claims(
                sync,
                desired_by_tag_id[tag_id],
                slug=tag.slug,
                name=tag.name,
                color=tag.color,
                description=tag.description,
                claim_type="scope",
                generation=generation,
                snapshot_id=snapshot_id,
                mark_domain=False,
                materialize=False,
            )
            added += result["claims_added"]
            released += result["claims_released"]

        materialized = finalize_device_tag_domain(
            sync,
            ForwardOwnershipReconciliation.Domain.SCOPE_TAGS,
            generation,
            snapshot_id,
            tag_ids=managed_tag_ids | stale_tag_ids,
        )
        return {
            "claims_added": added,
            "claims_released": released,
            **materialized,
        }


def _cleanup_managed_virtual_contexts(virtual_context_ids=None):
    from ..models import ForwardManagedVirtualContext
    from ..models import ForwardVirtualParentClaim

    ownerships = ForwardManagedVirtualContext.objects.select_related("virtual_context")
    if virtual_context_ids is not None:
        ownerships = ownerships.filter(virtual_context_id__in=virtual_context_ids)
    deleted = 0
    preserved_in_use = 0
    for ownership in ownerships:
        virtual_context = ownership.virtual_context
        if ForwardVirtualParentClaim.objects.filter(
            virtual_context_id=virtual_context.pk
        ).exists():
            continue
        if virtual_context.interfaces.exists():
            ownership.delete()
            preserved_in_use += 1
            continue
        try:
            with transaction.atomic():
                ownership.delete()
                virtual_context.delete()
            deleted += 1
        except ProtectedError:
            preserved_in_use += 1
    return {"vdc_deleted": deleted, "vdc_preserved_in_use": preserved_in_use}


def release_prunable_device_ownership(sync, device_ids):
    """Release only this sync's provenance for explicitly reviewed prune rows."""
    from django.db.models import Q

    from ..models import ForwardDeviceIdentity
    from ..models import ForwardPreservedDeviceTagAssignment
    from ..models import ForwardDeviceTagClaim
    from ..models import ForwardVirtualParentClaim

    candidate_ids = set(device_ids)
    if not candidate_ids:
        return {"released_device_ids": set(), "blocked_device_ids": set()}

    with ownership_write_lock():
        blocked_ids = set(
            ForwardDeviceTagClaim.objects.filter(device_id__in=candidate_ids)
            .exclude(sync=sync)
            .values_list("device_id", flat=True)
        )
        blocked_ids.update(
            ForwardDeviceIdentity.objects.filter(device_id__in=candidate_ids)
            .exclude(sync=sync)
            .values_list("device_id", flat=True)
        )
        blocked_ids.update(
            ForwardPreservedDeviceTagAssignment.objects.filter(
                device_id__in=candidate_ids
            ).values_list("device_id", flat=True)
        )
        parent_claims = list(
            ForwardVirtualParentClaim.objects.filter(
                Q(device_id__in=candidate_ids) | Q(parent_device_id__in=candidate_ids)
            ).values(
                "pk",
                "sync_id",
                "device_id",
                "parent_device_id",
                "virtual_context_id",
            )
        )
        for claim in parent_claims:
            involved = candidate_ids.intersection(
                {claim["device_id"], claim["parent_device_id"]}
            )
            if claim["sync_id"] != sync.pk:
                blocked_ids.update(involved)
        changed = True
        while changed:
            changed = False
            released_ids = candidate_ids - blocked_ids
            for claim in parent_claims:
                if claim["sync_id"] != sync.pk:
                    continue
                if (
                    claim["parent_device_id"] in released_ids
                    and claim["device_id"] not in released_ids
                ):
                    blocked_ids.add(claim["parent_device_id"])
                    changed = True

        released_ids = candidate_ids - blocked_ids
        ForwardDeviceTagClaim.objects.filter(
            sync=sync,
            device_id__in=released_ids,
        ).delete()
        ForwardDeviceIdentity.objects.filter(
            sync=sync,
            device_id__in=released_ids,
        ).delete()
        # A child can be pruned while its physical parent remains. A parent can
        # only be pruned when every linked child is in the same reviewed set.
        releasable_parent_claim_ids = {
            claim["pk"]
            for claim in parent_claims
            if claim["sync_id"] == sync.pk and claim["device_id"] in released_ids
        }
        cleanup_vdc_ids = {
            claim["virtual_context_id"]
            for claim in parent_claims
            if claim["pk"] in releasable_parent_claim_ids
            and claim["virtual_context_id"]
        }
        if releasable_parent_claim_ids:
            ForwardVirtualParentClaim.objects.filter(
                pk__in=releasable_parent_claim_ids
            ).delete()
        _cleanup_managed_virtual_contexts(cleanup_vdc_ids)
        return {
            "released_device_ids": released_ids,
            "blocked_device_ids": blocked_ids,
        }


def _materialize_virtual_parents(
    *,
    excluded_sync_ids=(),
    force_current=False,
    candidate_sync_id=None,
    candidate_generation=None,
):
    from dcim.models import Device

    from ..models import ForwardOwnershipReconciliation
    from ..models import ForwardVirtualParentClaim

    claims_by_child = defaultdict(set)
    claims = ForwardVirtualParentClaim.objects.exclude(sync_id__in=excluded_sync_ids)
    for child_id, parent_id in claims.values_list("device_id", "parent_device_id"):
        claims_by_child[child_id].add(parent_id)

    current = force_current or _domain_is_current(
        ForwardOwnershipReconciliation.Domain.VIRTUAL_PARENTS,
        excluded_sync_ids=excluded_sync_ids,
        candidate_sync_id=candidate_sync_id,
        candidate_generation=candidate_generation,
    )
    device_ids = set(claims_by_child)
    if current:
        for device in Device.objects.only("pk", "custom_field_data"):
            if _object_pk(
                (device.custom_field_data or {}).get("forward_parent_device")
            ) not in (
                None,
                "",
            ):
                device_ids.add(device.pk)

    linked = 0
    cleared = 0
    already = 0
    conflicts = 0
    for device in Device.objects.filter(pk__in=device_ids):
        parents = claims_by_child[device.pk]
        current_parent = _object_pk(
            (device.custom_field_data or {}).get("forward_parent_device")
        )
        if len(parents) > 1:
            conflicts += 1
            continue
        if parents:
            wanted = next(iter(parents))
            if current_parent == wanted:
                already += 1
                continue
            device.custom_field_data["forward_parent_device"] = wanted
            device.save()
            linked += 1
        elif current and current_parent not in (None, ""):
            device.custom_field_data["forward_parent_device"] = None
            device.save()
            cleared += 1
    return {
        "linked": linked,
        "cleared": cleared,
        "already": already,
        "conflicts": conflicts,
        "current": current,
    }


def reconcile_virtual_parent_claims(
    sync,
    desired,
    *,
    generation=None,
    snapshot_id=None,
):
    """Replace one sync's exact parent claims, then materialize their union."""
    from dcim.models import Device
    from dcim.models import VirtualDeviceContext

    from ..models import ForwardManagedVirtualContext
    from ..models import ForwardOwnershipReconciliation
    from ..models import ForwardVirtualParentClaim

    generation, snapshot_id = _generation_values(sync, generation, snapshot_id)
    with ownership_write_lock():
        existing = {
            claim.device_id: claim
            for claim in ForwardVirtualParentClaim.objects.filter(sync=sync)
        }
        devices = {
            device.pk: device
            for device in Device.objects.filter(
                pk__in=set(desired) | set(desired.values())
            )
        }
        cleanup_vdc_ids = set()
        claims_created = 0
        claims_updated = 0
        vdc_created = 0
        vdc_existing = 0
        for child_id, parent_id in desired.items():
            child = devices[child_id]
            virtual_context, created = VirtualDeviceContext.objects.get_or_create(
                device_id=parent_id,
                name=child.name,
                defaults={"status": "active"},
            )
            if created:
                ForwardManagedVirtualContext.objects.create(
                    virtual_context=virtual_context
                )
                vdc_created += 1
            else:
                vdc_existing += 1
            claim = existing.get(child_id)
            if claim is None:
                ForwardVirtualParentClaim.objects.create(
                    sync=sync,
                    device_id=child_id,
                    parent_device_id=parent_id,
                    virtual_context=virtual_context,
                    ingestion_id=generation,
                    snapshot_id=snapshot_id,
                )
                claims_created += 1
                continue
            if (
                claim.virtual_context_id
                and claim.virtual_context_id != virtual_context.pk
            ):
                cleanup_vdc_ids.add(claim.virtual_context_id)
            changed = (
                claim.parent_device_id != parent_id
                or claim.virtual_context_id != virtual_context.pk
                or claim.generation != generation
                or claim.snapshot_id != snapshot_id
            )
            if changed:
                claim.parent_device_id = parent_id
                claim.virtual_context = virtual_context
                claim.ingestion_id = generation
                claim.snapshot_id = snapshot_id
                claim.save(
                    update_fields=[
                        "parent_device",
                        "virtual_context",
                        "ingestion",
                        "snapshot_id",
                    ]
                )
                claims_updated += 1

        stale = list(
            ForwardVirtualParentClaim.objects.filter(sync=sync).exclude(
                device_id__in=desired
            )
        )
        cleanup_vdc_ids.update(
            claim.virtual_context_id for claim in stale if claim.virtual_context_id
        )
        stale_ids = [claim.pk for claim in stale]
        if stale_ids:
            ForwardVirtualParentClaim.objects.filter(pk__in=stale_ids).delete()

        materialized = _materialize_virtual_parents(
            candidate_sync_id=sync.pk,
            candidate_generation=generation,
        )
        cleanup = _cleanup_managed_virtual_contexts(cleanup_vdc_ids)
        if materialized["conflicts"]:
            ForwardOwnershipReconciliation.objects.update_or_create(
                sync=sync,
                domain=ForwardOwnershipReconciliation.Domain.VIRTUAL_PARENTS,
                defaults={
                    "ingestion_id": generation,
                    "snapshot_id": snapshot_id,
                    "status": ForwardOwnershipReconciliation.Status.FAILED,
                    "error_type": OwnershipConflictError.__name__,
                    "started_at": timezone.now(),
                    "completed_at": timezone.now(),
                },
            )
        else:
            _mark_reconciled(
                sync,
                ForwardOwnershipReconciliation.Domain.VIRTUAL_PARENTS,
                generation,
                snapshot_id,
            )
            if not materialized["current"] and _domain_is_current(
                ForwardOwnershipReconciliation.Domain.VIRTUAL_PARENTS
            ):
                materialized = _materialize_virtual_parents()
                cleanup = _cleanup_managed_virtual_contexts()
        return {
            "claims_created": claims_created,
            "claims_updated": claims_updated,
            "claims_released": len(stale_ids),
            "vdc_created": vdc_created,
            "vdc_existing": vdc_existing,
            **materialized,
            **cleanup,
        }


def release_sync_ownership(sync):
    """Release every ownership assertion before deleting a sync."""
    from ..models import ForwardDeviceIdentity
    from ..models import ForwardDeviceTagClaim
    from ..models import ForwardManagedDeviceTag
    from ..models import ForwardOwnershipReconciliation
    from ..models import ForwardVirtualParentClaim

    if not sync.pk:
        return
    with ownership_write_lock():
        vdc_ids = set(
            ForwardVirtualParentClaim.objects.filter(sync=sync)
            .exclude(virtual_context_id=None)
            .values_list("virtual_context_id", flat=True)
        )
        ForwardDeviceTagClaim.objects.filter(sync=sync).delete()
        ForwardDeviceIdentity.objects.filter(sync=sync).delete()
        ForwardVirtualParentClaim.objects.filter(sync=sync).delete()
        ForwardOwnershipReconciliation.objects.filter(sync=sync).delete()
        for managed_tag in ForwardManagedDeviceTag.objects.select_related("tag"):
            _materialize_managed_tag(
                managed_tag,
                excluded_sync_ids={sync.pk},
                force_current=True,
            )
        _materialize_virtual_parents(
            excluded_sync_ids={sync.pk},
            force_current=True,
        )
        _cleanup_managed_virtual_contexts(vdc_ids)


def release_source_ownership(source):
    """Release all sync claims before a source cascade is collected."""
    for sync in list(source.syncs.all()):
        release_sync_ownership(sync)


def ownership_finalization_summary(sync, *, generation=None):
    """Report whether every ownership domain required by this sync is current."""
    from dcim.models import Device

    from ..models import ForwardDeviceTagClaim
    from ..models import ForwardDeviceIdentity
    from ..models import ForwardManagedDeviceTag
    from ..models import ForwardManagedVirtualContext
    from ..models import ForwardOwnershipReconciliation
    from ..models import ForwardPreservedDeviceTagAssignment
    from ..models import ForwardVirtualParentClaim

    latest = latest_baseline_generation(sync)
    generation = int(generation or (latest or {}).get("generation") or 0)
    required = required_ownership_domains(sync)
    reconciled = {
        domain: (row_generation, status, error_type)
        for domain, row_generation, status, error_type in (
            ForwardOwnershipReconciliation.objects.filter(
                sync=sync,
                domain__in=required,
            ).values_list("domain", "ingestion_id", "status", "error_type")
        )
    }
    pending = [
        domain
        for domain in required
        if reconciled.get(domain, (None, None, ""))[:2]
        != (generation, ForwardOwnershipReconciliation.Status.COMPLETED)
    ]
    failed = [
        domain
        for domain in required
        if reconciled.get(domain, (None, None, ""))[:2]
        == (generation, ForwardOwnershipReconciliation.Status.FAILED)
    ]
    stale_claims = (
        ForwardDeviceTagClaim.objects.filter(sync=sync)
        .exclude(ingestion_id=generation)
        .count()
        + ForwardVirtualParentClaim.objects.filter(sync=sync)
        .exclude(ingestion_id=generation)
        .count()
    )
    provenance_sync_mismatches = sum(
        model.objects.filter(sync=sync).exclude(ingestion__sync_id=F("sync_id")).count()
        for model in (
            ForwardDeviceIdentity,
            ForwardDeviceTagClaim,
            ForwardVirtualParentClaim,
            ForwardOwnershipReconciliation,
        )
    )
    globally_stale_domains = [
        domain for domain in required if not _domain_is_current(domain)
    ]
    missing_assignments = 0
    extra_assignments = 0
    for managed_tag in ForwardManagedDeviceTag.objects.select_related("tag"):
        desired_ids = _desired_tag_device_ids(
            managed_tag.tag_id,
            managed_tag.claim_type,
        )
        preserved_ids = set(
            ForwardPreservedDeviceTagAssignment.objects.filter(
                tag_id=managed_tag.tag_id
            ).values_list("device_id", flat=True)
        )
        desired_ids.update(preserved_ids)
        assigned_ids = _tag_assignment_device_ids(managed_tag.tag)
        missing_assignments += len(desired_ids - assigned_ids)
        if _domain_is_current(
            _domain_for_claim_type(managed_tag.claim_type),
            tag_id=managed_tag.tag_id,
        ):
            extra_assignments += len(assigned_ids - desired_ids)

    claims_by_child = defaultdict(set)
    missing_virtual_contexts = 0
    virtual_context_parent_mismatches = 0
    for (
        child_id,
        parent_id,
        virtual_context_id,
        virtual_context_parent_id,
    ) in ForwardVirtualParentClaim.objects.values_list(
        "device_id",
        "parent_device_id",
        "virtual_context_id",
        "virtual_context__device_id",
    ):
        claims_by_child[child_id].add(parent_id)
        if virtual_context_id is None:
            missing_virtual_contexts += 1
        elif virtual_context_parent_id != parent_id:
            virtual_context_parent_mismatches += 1
    conflicting_children = sum(
        1 for parents in claims_by_child.values() if len(parents) > 1
    )
    parent_mismatches = 0
    devices = {
        device.pk: device
        for device in Device.objects.filter(pk__in=claims_by_child).only(
            "pk", "custom_field_data"
        )
    }
    for child_id, parents in claims_by_child.items():
        if len(parents) != 1:
            continue
        if _object_pk(
            (devices[child_id].custom_field_data or {}).get("forward_parent_device")
        ) != next(iter(parents)):
            parent_mismatches += 1
    unclaimed_parent_assignments = sum(
        1
        for device in Device.objects.only("pk", "custom_field_data")
        if device.pk not in claims_by_child
        and _object_pk((device.custom_field_data or {}).get("forward_parent_device"))
        not in (None, "")
    )
    orphan_managed_vdcs = ForwardManagedVirtualContext.objects.exclude(
        virtual_context_id__in=ForwardVirtualParentClaim.objects.exclude(
            virtual_context_id=None
        ).values("virtual_context_id")
    ).count()
    integrity_issue_count = (
        stale_claims
        + missing_assignments
        + extra_assignments
        + conflicting_children
        + parent_mismatches
        + missing_virtual_contexts
        + virtual_context_parent_mismatches
        + unclaimed_parent_assignments
        + orphan_managed_vdcs
        + len(globally_stale_domains)
        + provenance_sync_mismatches
    )
    return {
        "generation": generation or None,
        "required_domains": list(required),
        "pending_domains": pending,
        "globally_stale_domains": globally_stale_domains,
        "failed_domains": failed,
        "failure_types": sorted(
            {reconciled[domain][2] for domain in failed if reconciled[domain][2]}
        ),
        "stale_claims": stale_claims,
        "missing_assignments": missing_assignments,
        "extra_assignments": extra_assignments,
        "conflicting_parent_claims": conflicting_children,
        "parent_mismatches": parent_mismatches,
        "missing_virtual_contexts": missing_virtual_contexts,
        "virtual_context_parent_mismatches": virtual_context_parent_mismatches,
        "unclaimed_parent_assignments": unclaimed_parent_assignments,
        "orphan_managed_virtual_contexts": orphan_managed_vdcs,
        "provenance_sync_mismatches": provenance_sync_mismatches,
        "integrity_issue_count": integrity_issue_count,
        "complete": bool(generation) and not pending and integrity_issue_count == 0,
    }


def ownership_integrity_summary():
    """Return aggregate, non-sensitive provenance and generation evidence."""
    from dcim.models import Device

    from ..models import ForwardDeviceTagClaim
    from ..models import ForwardDeviceIdentity
    from ..models import ForwardManagedDeviceTag
    from ..models import ForwardManagedVirtualContext
    from ..models import ForwardOwnershipReconciliation
    from ..models import ForwardPreservedDeviceTagAssignment
    from ..models import ForwardSync
    from ..models import ForwardVirtualParentClaim

    provenance_sync_mismatches = sum(
        model.objects.exclude(ingestion__sync_id=F("sync_id")).count()
        for model in (
            ForwardDeviceIdentity,
            ForwardDeviceTagClaim,
            ForwardVirtualParentClaim,
            ForwardOwnershipReconciliation,
        )
    )

    missing_tag_assignments = 0
    unclaimed_managed_assignments = 0
    current_managed_tags = 0
    for managed_tag in ForwardManagedDeviceTag.objects.select_related("tag"):
        domain = _domain_for_claim_type(managed_tag.claim_type)
        desired_ids = _desired_tag_device_ids(
            managed_tag.tag_id,
            managed_tag.claim_type,
        )
        desired_ids.update(
            ForwardPreservedDeviceTagAssignment.objects.filter(
                tag_id=managed_tag.tag_id
            ).values_list("device_id", flat=True)
        )
        assigned_ids = _tag_assignment_device_ids(managed_tag.tag)
        missing_tag_assignments += len(desired_ids - assigned_ids)
        if _domain_is_current(domain, tag_id=managed_tag.tag_id):
            current_managed_tags += 1
            unclaimed_managed_assignments += len(assigned_ids - desired_ids)

    pending_reconciliations = 0
    missing_required_reconciliations = 0
    for sync in ForwardSync.objects.select_related("source"):
        latest = latest_baseline_generation(sync)
        for domain in required_ownership_domains(sync):
            reconciliation = ForwardOwnershipReconciliation.objects.filter(
                sync=sync,
                domain=domain,
            ).first()
            if reconciliation is None:
                missing_required_reconciliations += 1
            elif (
                latest is None
                or reconciliation.generation != latest["generation"]
                or reconciliation.status
                != ForwardOwnershipReconciliation.Status.COMPLETED
            ):
                pending_reconciliations += 1

    pending_managed_tag_domains = 0
    for managed_tag in ForwardManagedDeviceTag.objects.all():
        domain = _domain_for_claim_type(managed_tag.claim_type)
        relevant = _relevant_sync_ids(domain, tag_id=managed_tag.tag_id)
        if relevant and not _domain_is_current(domain, tag_id=managed_tag.tag_id):
            pending_managed_tag_domains += 1

    parent_domain_relevant = _relevant_sync_ids(
        ForwardOwnershipReconciliation.Domain.VIRTUAL_PARENTS
    )
    pending_virtual_parent_domain = int(
        bool(parent_domain_relevant)
        and not _domain_is_current(
            ForwardOwnershipReconciliation.Domain.VIRTUAL_PARENTS
        )
    )

    parent_conflicts = 0
    parent_mismatches = 0
    parent_claims_missing_virtual_context = 0
    virtual_context_parent_mismatches = 0
    claims_by_child = defaultdict(set)
    for (
        child_id,
        parent_id,
        virtual_context_id,
        virtual_context_parent_id,
    ) in ForwardVirtualParentClaim.objects.values_list(
        "device_id",
        "parent_device_id",
        "virtual_context_id",
        "virtual_context__device_id",
    ):
        claims_by_child[child_id].add(parent_id)
        if virtual_context_id is None:
            parent_claims_missing_virtual_context += 1
        elif virtual_context_parent_id != parent_id:
            virtual_context_parent_mismatches += 1
    devices = {
        device.pk: device
        for device in Device.objects.filter(pk__in=claims_by_child).only(
            "pk", "custom_field_data"
        )
    }
    for child_id, parents in claims_by_child.items():
        if len(parents) > 1:
            parent_conflicts += 1
            continue
        if _object_pk(
            (devices[child_id].custom_field_data or {}).get("forward_parent_device")
        ) != next(iter(parents)):
            parent_mismatches += 1
    unclaimed_parent_assignments = sum(
        1
        for device in Device.objects.only("pk", "custom_field_data")
        if device.pk not in claims_by_child
        and _object_pk((device.custom_field_data or {}).get("forward_parent_device"))
        not in (None, "")
    )

    orphan_managed_vdcs = ForwardManagedVirtualContext.objects.exclude(
        virtual_context_id__in=ForwardVirtualParentClaim.objects.exclude(
            virtual_context_id=None
        ).values("virtual_context_id")
    ).count()
    from netbox_branching.choices import BranchStatusChoices
    from netbox_branching.models import Branch

    open_branches = Branch.objects.exclude(
        status__in=[
            BranchStatusChoices.MERGED,
            BranchStatusChoices.ARCHIVED,
        ]
    ).count()
    pending_migration_branches = Branch.objects.filter(
        status=BranchStatusChoices.PENDING_MIGRATIONS
    ).count()
    reconciliation_status_counts = dict(
        Counter(ForwardOwnershipReconciliation.objects.values_list("status", flat=True))
    )
    reconciliation_failure_type_counts = dict(
        Counter(
            value
            for value in ForwardOwnershipReconciliation.objects.filter(
                status=ForwardOwnershipReconciliation.Status.FAILED
            ).values_list("error_type", flat=True)
            if value
        )
    )
    return {
        "managed_device_tags": ForwardManagedDeviceTag.objects.count(),
        "current_managed_device_tags": current_managed_tags,
        "device_tag_claims": ForwardDeviceTagClaim.objects.count(),
        "missing_tag_assignments": missing_tag_assignments,
        "unclaimed_managed_assignments": unclaimed_managed_assignments,
        "ownership_reconciliations": ForwardOwnershipReconciliation.objects.count(),
        "pending_reconciliations": pending_reconciliations,
        "missing_required_reconciliations": missing_required_reconciliations,
        "reconciliation_status_counts": reconciliation_status_counts,
        "reconciliation_failure_type_counts": reconciliation_failure_type_counts,
        "pending_managed_tag_domains": pending_managed_tag_domains,
        "pending_virtual_parent_domain": pending_virtual_parent_domain,
        "virtual_parent_claims": ForwardVirtualParentClaim.objects.count(),
        "parent_conflicts": parent_conflicts,
        "parent_mismatches": parent_mismatches,
        "parent_claims_missing_virtual_context": parent_claims_missing_virtual_context,
        "virtual_context_parent_mismatches": virtual_context_parent_mismatches,
        "unclaimed_parent_assignments": unclaimed_parent_assignments,
        "managed_virtual_contexts": ForwardManagedVirtualContext.objects.count(),
        "orphan_managed_virtual_contexts": orphan_managed_vdcs,
        "provenance_sync_mismatches": provenance_sync_mismatches,
        "open_branches": open_branches,
        "pending_migration_branches": pending_migration_branches,
    }
