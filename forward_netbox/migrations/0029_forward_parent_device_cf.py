from django.db import migrations

# Object custom field on dcim.device pointing at dcim.device. Populated by the
# vsys/vdom parent linkage (utilities.vsys_parent) so a virtual context
# (Palo vsys / Fortinet vdom) is associated with its physical chassis.
PARENT_DEVICE_CF = "forward_parent_device"


def create_parent_device_cf(apps, schema_editor):
    CustomField = apps.get_model("extras", "CustomField")
    ContentType = apps.get_model("contenttypes", "ContentType")
    device_ct = ContentType.objects.get(app_label="dcim", model="device")
    cf, _ = CustomField.objects.get_or_create(
        name=PARENT_DEVICE_CF,
        defaults={
            "type": "object",
            "label": "Parent Device",
            "description": (
                "Physical chassis for a virtual context (Palo Alto vsys / "
                "Fortinet vdom), set by the Forward sync."
            ),
            "related_object_type": device_ct,
            "required": False,
            "search_weight": 1000,
        },
    )
    # Idempotent: ensure the related type + assignment even if the row pre-existed.
    if cf.related_object_type_id != device_ct.id:
        cf.related_object_type = device_ct
        cf.save(update_fields=["related_object_type"])
    cf.object_types.set([device_ct])


def remove_parent_device_cf(apps, schema_editor):
    CustomField = apps.get_model("extras", "CustomField")
    CustomField.objects.filter(name=PARENT_DEVICE_CF).delete()


class Migration(migrations.Migration):
    dependencies = [
        ("forward_netbox", "0028_remove_forwardexecutionstep_run_and_more"),
        ("extras", "0001_initial"),
        ("dcim", "0001_initial"),
        ("contenttypes", "0001_initial"),
    ]

    operations = [
        migrations.RunPython(create_parent_device_cf, remove_parent_device_cf),
    ]
