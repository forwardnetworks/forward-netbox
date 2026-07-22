from django.db import migrations
from django.db import models


fake_on_branch = True
STATUS_CLAIM_BY_SLUG = {
    "forward-backfilled": "backfilled",
    "forward-out-of-scope": "out_of_scope",
}


def normalize_managed_tag_domains(apps, schema_editor):
    ManagedTag = apps.get_model("forward_netbox", "ForwardManagedDeviceTag")
    Tag = apps.get_model("extras", "Tag")

    tag_ids = set(ManagedTag.objects.values_list("tag_id", flat=True))
    slugs = dict(Tag.objects.filter(pk__in=tag_ids).values_list("pk", "slug"))
    for tag_id in tag_ids:
        rows = ManagedTag.objects.filter(tag_id=tag_id).order_by("pk")
        expected_claim_type = STATUS_CLAIM_BY_SLUG.get(slugs.get(tag_id), "scope")
        keeper = rows.filter(claim_type=expected_claim_type).first()
        if keeper is None:
            rows.delete()
            continue
        rows.exclude(pk=keeper.pk).delete()


class Migration(migrations.Migration):
    dependencies = [
        ("forward_netbox", "0037_merge_identity_and_assignment_provenance"),
    ]

    operations = [
        migrations.RunPython(
            normalize_managed_tag_domains,
            reverse_code=migrations.RunPython.noop,
        ),
        migrations.RemoveConstraint(
            model_name="forwardmanageddevicetag",
            name="forward_managed_device_tag_identity",
        ),
        migrations.AddConstraint(
            model_name="forwardmanageddevicetag",
            constraint=models.UniqueConstraint(
                fields=("tag",),
                name="forward_managed_device_tag_identity",
            ),
        ),
    ]
