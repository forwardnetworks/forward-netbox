from django.db import IntegrityError

from ..exceptions import ForwardSearchError


def update_existing_or_create(
    runner, model, *, lookup, defaults, fallback_lookups=None, conflict_policy="strict"
):
    return coalesce_update_or_create(
        runner,
        model,
        coalesce_lookups=[lookup, *(fallback_lookups or [])],
        create_values={**lookup, **defaults},
        update_values=defaults,
        conflict_policy=conflict_policy,
    )


def coalesce_update_or_create(
    runner,
    model,
    *,
    coalesce_lookups,
    create_values,
    update_values=None,
    conflict_policy="strict",
):
    lookups = [lookup for lookup in (coalesce_lookups or []) if lookup]
    if not lookups:
        raise ValueError("At least one coalesce lookup must be provided.")

    update_values = create_values if update_values is None else update_values

    obj = None
    for lookup in lookups:
        obj = get_unique_or_raise(runner, model, lookup)
        if obj is not None:
            break

    if obj is None:
        try:
            obj = model(**create_values)
            obj.full_clean()
            obj.save()
            return obj, True
        except IntegrityError:
            if conflict_policy != "reuse_on_unique_conflict":
                raise
            for retry_lookup in lookups:
                obj = get_unique_or_raise(runner, model, retry_lookup)
                if obj is not None:
                    break
            if obj is None:
                raise

    update_fields = []
    for field, value in update_values.items():
        if getattr(obj, field) != value:
            setattr(obj, field, value)
            update_fields.append(field)
    if update_fields:
        obj.full_clean()
        obj.save(update_fields=update_fields)
    return obj, False


def get_unique_or_raise(runner, model, lookup):
    queryset = model.objects.filter(**lookup).order_by("pk")
    obj = queryset.first()
    if obj is None:
        return None
    if queryset.exclude(pk=obj.pk).exists():
        raise ForwardSearchError(
            f"Ambiguous coalesce lookup for `{model._meta.label_lower}` with {lookup}.",
            model_string=model._meta.label_lower,
            context=lookup,
        )
    return obj


def coalesce_lookup(row, *fields):
    return {
        field: row[field]
        for field in fields
        if field in row and row[field] not in ("", None)
    }


def coalesce_upsert(
    runner,
    model_string,
    model,
    *,
    coalesce_lookups,
    create_values,
    update_values=None,
):
    return coalesce_update_or_create(
        runner,
        model,
        coalesce_lookups=coalesce_lookups,
        create_values=create_values,
        update_values=update_values,
        conflict_policy=runner._conflict_policy(model_string),
    )


def coalesce_sets_for(runner, model_string, default_sets):
    configured = runner._model_coalesce_fields.get(model_string)
    if configured:
        return configured
    return [list(field_set) for field_set in default_sets]


def upsert_row(
    runner,
    model_string,
    model,
    *,
    row,
    coalesce_sets,
    create_values,
    update_values=None,
):
    lookups = [coalesce_lookup(row, *coalesce_set) for coalesce_set in coalesce_sets]
    return coalesce_upsert(
        runner,
        model_string,
        model,
        coalesce_lookups=lookups,
        create_values=create_values,
        update_values=update_values,
    )


def upsert_row_from_defaults(
    runner, model_string, model, *, row, coalesce_sets, defaults
):
    return upsert_row(
        runner,
        model_string,
        model,
        row=row,
        coalesce_sets=coalesce_sets,
        create_values=defaults,
        update_values=defaults,
    )


def upsert_values_from_defaults(runner, model_string, model, *, values, coalesce_sets):
    lookups = [coalesce_lookup(values, *coalesce_set) for coalesce_set in coalesce_sets]
    return coalesce_upsert(
        runner,
        model_string,
        model,
        coalesce_lookups=lookups,
        create_values=values,
        update_values=values,
    )


def delete_by_coalesce(runner, model, lookups):
    lookups = [lookup for lookup in lookups or [] if lookup]
    if not lookups:
        return False
    for lookup in lookups:
        obj = get_unique_or_raise(runner, model, lookup)
        if obj is not None:
            obj.delete()
            return True
    return False


def optional_model(app_label, model_name, model_string):
    from django.apps import apps

    from ..exceptions import ForwardQueryError

    try:
        return apps.get_model(app_label, model_name)
    except LookupError as exc:
        raise ForwardQueryError(
            f"`{model_string}` sync requires the optional `{app_label}` NetBox "
            "plugin to be installed and migrated."
        ) from exc


def model_field_values(model, values):
    model_fields = {
        field.name
        for field in model._meta.fields
        if not getattr(field, "auto_created", False)
    }
    return {key: value for key, value in values.items() if key in model_fields}


def content_type_for(runner, model):
    from django.contrib.contenttypes.models import ContentType

    key = model._meta.label_lower
    if key not in runner._content_types:
        runner._content_types[key] = ContentType.objects.get_for_model(model)
    return runner._content_types[key]


def lookup_interface(device, interface_name):
    from dcim.models import Interface

    return Interface.objects.filter(device=device, name=interface_name).first()


def lookup_module_bay(device, module_bay_name):
    return device.modulebays.filter(name=module_bay_name).order_by("pk").first()
