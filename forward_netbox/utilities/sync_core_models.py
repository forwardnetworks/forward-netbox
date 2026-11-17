from dcim.models import DeviceRole
from dcim.models import DeviceType
from dcim.models import Manufacturer
from dcim.models import Platform
from dcim.models import Site


def _site_created_by_someone_else(runner, row):
    """True when NetBox's change log proves another actor created this site.

    The sync deletes a site it produced once Forward no longer returns it. A site
    it only matched (by slug) was created by someone else - an inventory import, a
    person - and is theirs to remove. The proof is a create record under a request
    that is not one of this sync's runs. No record (a fast baseline load writes
    none, and old records age out) is not proof, so the delete goes ahead as before.
    """
    from core.models import ObjectChange
    from core.models import ObjectType
    from django.db import DEFAULT_DB_ALIAS

    # Read main, not the active branch: deletes run inside the sync's branch, and
    # netbox_branching routes every change-log read to the branch's own table,
    # which holds only this branch's changes - the create record that proves who
    # made the site lives in main. A site that is not in main was created in this
    # branch, by this run, and is the sync's to remove.
    site = None
    for lookup in (
        runner._coalesce_lookup(row, "slug"),
        runner._coalesce_lookup(row, "name"),
    ):
        if lookup:
            site = Site.objects.using(DEFAULT_DB_ALIAS).filter(**lookup).first()
            if site is not None:
                break
    if site is None:
        return False
    created = (
        ObjectChange.objects.using(DEFAULT_DB_ALIAS)
        .filter(
            changed_object_type=ObjectType.objects.get_for_model(Site),
            changed_object_id=site.pk,
            action="create",
        )
        .order_by("time")
        .values_list("request_id", flat=True)
        .first()
    )
    if created is None:
        return False
    own = getattr(runner, "_site_delete_own_request_ids", None)
    if own is None:
        from ..models import ForwardIngestion

        own = {
            str(rid)
            for rid in ForwardIngestion.objects.using(DEFAULT_DB_ALIAS)
            .filter(sync=runner.sync)
            .exclude(change_request_id=None)
            .values_list("change_request_id", flat=True)
        }
        runner._site_delete_own_request_ids = own
    return str(created) not in own


def _objects_referencing_site(site):
    """Rows that point at ``site`` through any non-M2M relation, as model labels.

    NetBox cascades a site delete into objects that merely carry it as a cached
    scope (prefixes and clusters keep a `_site` foreign key with CASCADE), so a
    delete that NetBox would allow can still take another system's prefixes with
    it. A site is only the sync's to delete once nothing points at it - the same
    rule Prune orphans applies (`scope_reconciliation._occupied_site_ids`).
    """
    from ..models import ForwardChangePolicyRule

    referencing = []
    for relation in Site._meta.related_objects:
        if relation.many_to_many:
            continue
        # The base manager: a default manager that filters would hide references.
        rows = relation.related_model._base_manager.filter(
            **{relation.field.attname: site.pk}
        )
        sample = list(rows[:5])
        if sample:
            referencing.extend(sample)
    # Relations declared with `related_name="+"` are not in `related_objects`,
    # and a change-policy rule scoped to the site would be cascade-deleted.
    referencing.extend(ForwardChangePolicyRule._base_manager.filter(site=site)[:5])
    return referencing


def delete_dcim_site(runner, row):
    from django.db.models.deletion import ProtectedError

    from .sync_primitives import protected_delete_skip

    for lookup in (
        runner._coalesce_lookup(row, "slug"),
        runner._coalesce_lookup(row, "name"),
    ):
        if not lookup:
            continue
        site = Site.objects.filter(**lookup).first()
        if site is None:
            continue
        referencing = _objects_referencing_site(site)
        if referencing:
            raise protected_delete_skip(
                Site,
                ProtectedError(
                    "still referenced (the delete would cascade or be refused)",
                    set(referencing),
                ),
                obj=site,
                context=lookup,
            )
        break
    if _site_created_by_someone_else(runner, row):
        runner._record_aggregated_skip_warning(
            model_string="dcim.site",
            reason="site-created-by-someone-else",
            warning_message=(
                f"Kept site `{row.get('slug') or row.get('name')}`: this sync no "
                "longer returns it, but NetBox's change log shows another actor "
                "created it, so it is theirs to remove."
            ),
        )
        return False
    return runner._delete_by_coalesce(
        Site,
        [
            runner._coalesce_lookup(row, "slug"),
            runner._coalesce_lookup(row, "name"),
        ],
    )


def delete_dcim_manufacturer(runner, row):
    return runner._delete_by_coalesce(
        Manufacturer,
        [
            runner._coalesce_lookup(row, "slug"),
            runner._coalesce_lookup(row, "name"),
        ],
    )


def delete_dcim_devicerole(runner, row):
    return runner._delete_by_coalesce(
        DeviceRole,
        [
            runner._coalesce_lookup(row, "slug"),
            runner._coalesce_lookup(row, "name"),
        ],
    )


def delete_dcim_platform(runner, row):
    return runner._delete_by_coalesce(
        Platform,
        [
            runner._coalesce_lookup(row, "slug"),
            runner._coalesce_lookup(row, "name"),
        ],
    )


def delete_dcim_devicetype(runner, row):
    manufacturer = None
    if row.get("manufacturer_slug"):
        manufacturer = runner._get_unique_or_raise(
            Manufacturer, {"slug": row["manufacturer_slug"]}
        )
    if manufacturer is None and row.get("manufacturer"):
        manufacturer = runner._get_unique_or_raise(
            Manufacturer, {"name": row["manufacturer"]}
        )
    if manufacturer is None:
        return False

    return runner._delete_by_coalesce(
        DeviceType,
        [
            (
                {"manufacturer": manufacturer, "slug": row["slug"]}
                if row.get("slug")
                else {}
            ),
            (
                {"manufacturer": manufacturer, "model": row["model"]}
                if row.get("model")
                else {}
            ),
        ],
    )


def apply_dcim_site(runner, row):
    runner._ensure_site(row)


def apply_dcim_manufacturer(runner, row):
    runner._ensure_manufacturer(row)


def _leaf_outcome(runner, obj):
    """One flat row, one object: created, changed, or unchanged."""
    if obj is None:
        return "creates"
    return "updates" if runner.last_upsert_would_change else "unchanged"


def apply_dcim_platform(runner, row, *, preview=False):
    if preview:
        # The preview runner's `_ensure_platform` is a bare lookup (the device
        # classification must never create a platform), so classify through
        # the plan and the runner's read-only upsert instead.
        from dcim.models import Platform

        lookups, create_values, update_values = runner._platform_upsert_plan(
            row, manufacturer_authoritative=True
        )
        platform, _ = runner._coalesce_upsert(
            "dcim.platform",
            Platform,
            coalesce_lookups=lookups,
            create_values=create_values,
            update_values=update_values,
        )
        return _leaf_outcome(runner, platform)
    return runner._ensure_platform(row, manufacturer_authoritative=True)


def apply_dcim_devicerole(runner, row, *, preview=False):
    role = runner._ensure_role(row)
    if preview:
        return _leaf_outcome(runner, role)
    return role


def apply_dcim_devicetype(runner, row):
    runner._ensure_device_type(row)
