from django.db import migrations
from django.utils.text import slugify


# This data migration governs public control-plane rows and sync configuration.
fake_on_branch = True
RESERVED_STATUS_TAG_SLUGS = {
    "forward-backfilled",
    "forward-out-of-scope",
}


def _configured_include_tags(parameters):
    include_tags = parameters.get("device_tag_include_tags") or []
    if not include_tags and parameters.get("device_tag_include"):
        include_tags = [parameters.get("device_tag_include")]
    return [str(tag).strip() for tag in include_tags if str(tag).strip()]


def initialize_ownership_control_plane(apps, schema_editor):
    ForwardSource = apps.get_model("forward_netbox", "ForwardSource")
    ForwardSync = apps.get_model("forward_netbox", "ForwardSync")
    ManagedTag = apps.get_model("forward_netbox", "ForwardManagedDeviceTag")
    Tag = apps.get_model("extras", "Tag")

    changed_syncs = []
    for sync in ForwardSync.objects.all().only("pk", "parameters").iterator():
        parameters = dict(sync.parameters or {})
        if "auto_prune_orphans" not in parameters:
            continue
        parameters.pop("auto_prune_orphans", None)
        sync.parameters = parameters
        changed_syncs.append(sync)
    if changed_syncs:
        ForwardSync.objects.bulk_update(changed_syncs, ["parameters"], batch_size=2000)

    managed = []
    for slug, claim_type in (
        ("forward-backfilled", "backfilled"),
        ("forward-out-of-scope", "out_of_scope"),
    ):
        tag = Tag.objects.filter(slug=slug).first()
        if tag is not None:
            managed.append(ManagedTag(tag_id=tag.pk, claim_type=claim_type))

    for source in ForwardSource.objects.all().only("parameters").iterator():
        parameters = source.parameters or {}
        if not parameters.get("apply_device_scope_tags"):
            continue
        for name in _configured_include_tags(parameters):
            slug = slugify(name) or slugify(name.replace(".", "-"))
            if not slug or slug in RESERVED_STATUS_TAG_SLUGS:
                raise RuntimeError(
                    f"Forward source {source.pk} has managed scope tag `{name}` "
                    f"with invalid or reserved slug `{slug}`. Correct the source "
                    "configuration before upgrading."
                )
            tag = Tag.objects.filter(slug=slug).first() if slug else None
            if tag is None:
                tag = Tag.objects.filter(name=name).first()
            if tag is not None:
                managed.append(ManagedTag(tag_id=tag.pk, claim_type="scope"))

    ManagedTag.objects.bulk_create(managed, ignore_conflicts=True, batch_size=2000)


class Migration(migrations.Migration):
    dependencies = [
        ("forward_netbox", "0034_source_ownership_provenance"),
    ]

    operations = [
        migrations.RunPython(
            initialize_ownership_control_plane,
            reverse_code=migrations.RunPython.noop,
        ),
    ]
