"""Find ChangeDiff rows whose payload does not belong to their object type.

2.8.6 root-caused a customer's merge `KeyError` to `_sync_branch_change_diffs`
grouping a mixed-model batch by its first change's type, so a diff whose
`object_type` said IPAddress could carry a serialized Device in `original`.
The sink was fixed. Whether any such row survives in that deployment's
database was never checked: its plan recorded "confined to per-ingestion
branches, discarded on retry; no persistent damage is expected in main", and
an expectation is not a measurement.

This is the measurement. Read-only, computed on demand, persisted nowhere. A
row is flagged when its serialized payload carries field names the object
type's model does not have - a Device snapshot under an IPAddress diff has
`device_type` and `site`, which no IPAddress has. Payload VALUES are never
read or reported; only key names, which are schema identifiers.
"""

# Keys NetBox's serializer adds to every snapshot regardless of model.
_SERIALIZER_KEYS = frozenset(
    {
        "id",
        "custom_fields",
        "custom_field_data",
        "tags",
        "display",
        "url",
        "created",
        "last_updated",
    }
)


def foreign_payload_keys(model_class, payload):
    """Keys in ``payload`` that are not fields of ``model_class``."""
    if not isinstance(payload, dict) or model_class is None:
        return set()
    field_names = {field.name for field in model_class._meta.get_fields()}
    field_names |= {
        getattr(field, "attname", "")
        for field in model_class._meta.get_fields()
        if getattr(field, "attname", "")
    }
    return {
        key for key in payload if key not in field_names and key not in _SERIALIZER_KEYS
    }


def classify_change_diff(object_type, payloads):
    """Return the foreign key names per payload slot, or an empty dict when clean.

    ``payloads`` is ``{"original": ..., "modified": ..., "current": ...}``. A
    single foreign key can be a serializer quirk; two or more on one slot is a
    payload from another model.
    """
    model_class = object_type.model_class() if object_type is not None else None
    findings = {}
    for slot, payload in payloads.items():
        foreign = foreign_payload_keys(model_class, payload)
        if len(foreign) >= 2:
            findings[slot] = sorted(foreign)
    return findings


def audit_change_diffs(*, sample_limit=25):
    """Every ChangeDiff whose payload belongs to another model."""
    from netbox_branching.models import ChangeDiff

    scanned = 0
    flagged = []
    by_object_type = {}
    for diff in ChangeDiff.objects.select_related("object_type").iterator(
        chunk_size=500
    ):
        scanned += 1
        findings = classify_change_diff(
            diff.object_type,
            {
                "original": diff.original,
                "modified": diff.modified,
                "current": diff.current,
            },
        )
        if not findings:
            continue
        label = (
            diff.object_type.model_class()._meta.label_lower
            if diff.object_type
            else "?"
        )
        by_object_type[label] = by_object_type.get(label, 0) + 1
        if len(flagged) < max(int(sample_limit or 0), 0):
            flagged.append(
                {
                    "pk": diff.pk,
                    "branch": diff.branch_id,
                    "object_type": label,
                    "object_id": diff.object_id,
                    "action": diff.action,
                    "foreign_keys": findings,
                }
            )
    payload = {
        "scanned": scanned,
        "flagged_count": sum(by_object_type.values()),
        "flagged_by_object_type": dict(sorted(by_object_type.items())),
        "sample_limit": max(int(sample_limit or 0), 0),
        "sample": flagged,
    }
    if payload["flagged_count"]:
        payload["remediation"] = (
            "These rows carry a serialized payload from a different model than "
            "their object_type - the 2.8.6 mixed-model corruption. The sink was "
            "fixed in 2.8.6; a flagged row predates it or came from a branch "
            "that was never retried. Delete the flagged ChangeDiff rows (they are "
            "branch bookkeeping, not NetBox data) or discard their branch, then "
            "re-run this audit."
        )
    return payload
