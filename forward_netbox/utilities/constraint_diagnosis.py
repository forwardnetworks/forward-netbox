"""Name the row a constraint error is about, using reads only.

A bulk write that hits a constraint inside a branch cannot be retried row by
row the way `_isolate_bulk_objects` does on main: branch rows, ObjectChanges
and ChangeDiffs are one transaction, so re-driving them through per-row saves
would emit branch evidence on a second connection. Those paths therefore
re-raise, and the shard fails carrying only a constraint name - and
`safe_log_message` rebuilds the failure line without the database's own
`DETAIL`, so the offending row is unrecoverable from the ingestion issue, the
job and the exported support bundle alike.

A deployment lost a sync to `dcim_device_unique_name_site` and there was no
way, from anything the operator could reach or send us, to learn which device.
The Forward side had to be ruled out by pulling every device row for the
network and checking for duplicates by hand.

This closes that. It runs SELECTs only, so it is safe on the branch alias
inside the `except` block, and it records through the two conventions the
issue writers already consume:

* `exc.netbox_pk` renders `Affected NetBox row: pk N.` and is persisted.
* `exc.safe_diagnosis` merges into the value-free diagnosis.
* `exc.operator_detail` carries the values, for the GUI only - dropped from
  every download by `export_redaction.export_safe_payload`.
"""

from __future__ import annotations

from collections import Counter

from django.db.models import Q
from rq.timeouts import JobTimeoutException

# One SELECT per this many candidate keys. Large enough that a normal batch is
# a single query, small enough that a 10,000-row shard does not build one
# enormous OR tree.
_QUERY_CHUNK = 500
# How many conflicting rows to name. The first few identify the pattern; a
# thousand of them is a different report, and this one rides on a failure path.
_DEFAULT_LIMIT = 10


def _constraint_name(exc):
    """The database's own constraint name, or "" if the driver did not say."""
    seen = set()
    candidate = exc
    while candidate is not None and id(candidate) not in seen:
        seen.add(id(candidate))
        name = str(
            getattr(getattr(candidate, "diag", None), "constraint_name", "") or ""
        ).strip()
        if name:
            return name
        candidate = getattr(candidate, "__cause__", None)
    return ""


def _spec_from_expression(model, expression):
    """One `(field, attname, case_insensitive)` spec, or None if unreadable.

    NetBox writes its important uniqueness rules as EXPRESSIONS, not plain
    field tuples: `dcim_device_unique_name_site` is
    `UniqueConstraint(Lower("name"), "site", condition=Q(tenant__isnull=True))`.
    Reading only `.fields` finds an empty tuple there and concludes nothing can
    be resolved - on precisely the constraint that took a customer's sync down.
    """
    case_insensitive = False
    node = expression
    # `Lower(F("name"))` - the wrapper is what makes the rule case-insensitive,
    # and missing that means looking up `name=` exactly and finding nothing.
    if type(node).__name__ == "Lower":
        case_insensitive = True
        sources = getattr(node, "source_expressions", ()) or ()
        if len(sources) != 1:
            return None
        node = sources[0]
    name = getattr(node, "name", None)
    if not name:
        return None
    try:
        field = model._meta.get_field(name)
    except JobTimeoutException:
        raise
    except Exception:  # noqa: BLE001 - an expression over something else
        return None
    return (field.name, field.attname, case_insensitive)


def _constraint_specs(model, constraint_name, *, using):
    """`(specs, condition)` for a constraint, or `((), None)` when unresolved.

    Three tiers, and no guessing after them: a wrong field tuple produces a
    confident wrong answer about which row conflicted, which is worse than the
    silence it replaces.
    """
    if not constraint_name:
        return (), None

    for constraint in getattr(model._meta, "constraints", ()) or ():
        if str(getattr(constraint, "name", "") or "") != constraint_name:
            continue
        condition = getattr(constraint, "condition", None)
        # 1. A plain field tuple.
        fields = tuple(getattr(constraint, "fields", ()) or ())
        if fields:
            specs = []
            for name in fields:
                try:
                    field = model._meta.get_field(name)
                except JobTimeoutException:
                    raise
                except Exception:  # noqa: BLE001
                    return (), None
                specs.append((field.name, field.attname, False))
            return tuple(specs), condition
        # 2. Expressions, which is how the interesting ones are written.
        expressions = tuple(getattr(constraint, "expressions", ()) or ())
        if expressions:
            specs = []
            for expression in expressions:
                spec = _spec_from_expression(model, expression)
                if spec is None:
                    return (), None
                specs.append(spec)
            return tuple(specs), condition
        return (), None

    # 3. `unique_together` names are generated by the database, so the mapping
    #    lives there. Read it on `using` so a branch schema reports its own.
    try:
        from django.db import connections

        with connections[using].cursor() as cursor:
            constraints = connections[using].introspection.get_constraints(
                cursor, model._meta.db_table
            )
        columns = (constraints.get(constraint_name) or {}).get("columns") or []
        if columns:
            by_column = {field.column: field for field in model._meta.fields}
            if all(column in by_column for column in columns):
                return (
                    tuple(
                        (by_column[column].name, by_column[column].attname, False)
                        for column in columns
                    ),
                    None,
                )
    except JobTimeoutException:
        raise
    except Exception:  # noqa: BLE001 - introspection is best effort
        return (), None

    return (), None


def _key_for(obj, specs):
    """The object's key tuple, normalised the way the database compares it."""
    key = []
    for _field, attname, case_insensitive in specs:
        value = getattr(obj, attname, None)
        if case_insensitive and isinstance(value, str):
            value = value.casefold()
        key.append(value)
    return tuple(key)


def _lookup_for(key, specs):
    """A `Q` matching `key`, honouring case-insensitive members."""
    terms = {}
    for (field, attname, case_insensitive), value in zip(specs, key):
        terms[f"{field}__iexact" if case_insensitive else attname] = value
    return Q(**terms)


def _annotate(exc, model, *, create_objects, update_objects, using, limit):
    constraint_name = _constraint_name(exc)
    specs, condition = _constraint_specs(model, constraint_name, using=using)
    diagnosis = {"constraint_name": constraint_name} if constraint_name else {}

    if not specs:
        diagnosis["constraint_fields_resolved"] = False
        _merge(exc, diagnosis)
        return

    fields = [field for field, _attname, _ci in specs]
    diagnosis["constraint_fields_resolved"] = True
    diagnosis["constraint_fields"] = fields
    if any(case_insensitive for _f, _a, case_insensitive in specs):
        # Worth stating: it explains why two rows that look different to a
        # human collide, and why a plain equality lookup would find neither.
        diagnosis["constraint_case_insensitive"] = True

    objects = [*create_objects, *update_objects]
    counts = Counter()
    candidates = {}
    for obj in objects:
        key = _key_for(obj, specs)
        if None in key:
            # An unresolved FK cannot be what the database rejected on this
            # constraint; it would have failed a different one.
            continue
        counts[key] += 1
        candidates.setdefault(key, obj)

    # Two rows in the same statement need no query, and a query would not find
    # them: neither is in the table yet.
    duplicate_keys = [key for key, count in counts.items() if count > 1]

    conflicting_pks = []
    if candidates and not duplicate_keys:
        keys = list(candidates)
        for start_index in range(0, len(keys), _QUERY_CHUNK):
            lookup = Q()
            for key in keys[start_index : start_index + _QUERY_CHUNK]:
                lookup |= _lookup_for(key, specs)
            queryset = model.objects.using(using).filter(lookup)
            if condition is not None:
                # Only rows the constraint actually governs. A partial index
                # excludes the rest, so counting them would be a false report.
                queryset = queryset.filter(condition)
            for row in queryset.values_list("pk", *[s[1] for s in specs]):
                pk = row[0]
                # A row this write itself created is not a prior conflict.
                if pk in {
                    getattr(obj, "pk", None)
                    for obj in objects
                    if getattr(obj, "pk", None)
                }:
                    continue
                conflicting_pks.append(pk)
                if len(conflicting_pks) >= limit:
                    break
            if len(conflicting_pks) >= limit:
                break

    if duplicate_keys:
        diagnosis["conflict_kind"] = "in_batch_duplicate"
        diagnosis["conflict_count"] = len(duplicate_keys)
    elif conflicting_pks:
        diagnosis["conflict_kind"] = "existing_row"
        diagnosis["conflict_count"] = len(conflicting_pks)
        diagnosis["conflicting_pks"] = conflicting_pks
    else:
        # Resolved the constraint and matched nothing. Say so: it means the
        # collision is not the shape it appears to be, which is itself a lead.
        diagnosis["conflict_kind"] = "unidentified"

    _merge(exc, diagnosis)

    if conflicting_pks and getattr(exc, "netbox_pk", None) is None:
        exc.netbox_pk = conflicting_pks[0]

    # Tier 2: the values themselves, for the GUI. Dropped from every download.
    detail_keys = duplicate_keys or list(candidates)
    if detail_keys:
        existing = getattr(exc, "operator_detail", None)
        detail = dict(existing) if isinstance(existing, dict) else {}
        detail.setdefault(
            "conflicting_rows",
            [
                {
                    field: getattr(candidates[key], attname, None)
                    for field, attname, _ci in specs
                }
                for key in detail_keys[:limit]
                if key in candidates
            ],
        )
        exc.operator_detail = detail


def _merge(exc, diagnosis):
    existing = getattr(exc, "safe_diagnosis", None)
    merged = dict(existing) if isinstance(existing, dict) else {}
    merged.update(diagnosis)
    exc.safe_diagnosis = merged


def annotate_integrity_error(
    exc,
    model,
    *,
    create_objects=(),
    update_objects=(),
    using,
    limit=_DEFAULT_LIMIT,
):
    """Attach a constraint diagnosis to `exc`. Never raises, never writes.

    Called immediately before the bare `raise` in each bulk path, so the
    exception that propagates is the same object carrying more about itself.
    A failure to diagnose must never replace the failure being diagnosed.
    """
    try:
        _annotate(
            exc,
            model,
            create_objects=create_objects,
            update_objects=update_objects,
            using=using,
            limit=limit,
        )
    except JobTimeoutException:
        raise
    except Exception:  # noqa: BLE001 - diagnosis must not mask the real error
        return
