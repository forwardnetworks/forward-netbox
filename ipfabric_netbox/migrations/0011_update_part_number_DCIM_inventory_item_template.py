"""
NIM-18852.
Update to truncate name and description fields because they can
exceed DB field limits.
"""
from django.db import migrations

# Keep the original template here rather than loading it from transform_map.json
# so our revert won’t break if that template ever changes.
OLD_TEMPLATE = (
    "{% if object.name is not none %}"
    "{{ object.name }}"
    "{% elif object.dscr is not none %}"
    "{{ object.dscr}}"
    "{% else %}Default Name{% endif %}"
)

NEW_TEMPLATE = (
    "{% if object.name is not none %}"
    "{{ object.name | string | truncate(64, True) }}"
    "{% elif object.dscr is not none %}"
    "{{ object.dscr | string | truncate(64, True) }}"
    "{% else %}Default Name{% endif %}"
)


def apply_truncate_template(apps, schema_editor):
    """Replace old template with truncated version on all matching transform fields."""
    TransformField = apps.get_model("ipfabric_netbox", "IPFabricTransformField")
    TransformField.objects.filter(
        template=OLD_TEMPLATE,
        source_field="name",
        target_field="name",
    ).update(template=NEW_TEMPLATE)


def revert_truncate_template(apps, schema_editor):
    """Revert truncated template back to the original exact template."""
    TransformField = apps.get_model("ipfabric_netbox", "IPFabricTransformField")
    TransformField.objects.filter(
        template=NEW_TEMPLATE,
        source_field="name",
        target_field="name",
    ).update(template=OLD_TEMPLATE)


class Migration(migrations.Migration):
    dependencies = [
        ("ipfabric_netbox", "0010_remove_uuid_from_get_or_create"),
    ]

    operations = [
        migrations.RunPython(
            apply_truncate_template,
            revert_truncate_template,
        ),
    ]
