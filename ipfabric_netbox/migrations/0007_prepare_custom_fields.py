from typing import TYPE_CHECKING

from django.db import migrations
from extras.choices import CustomFieldTypeChoices
from extras.choices import CustomFieldUIEditableChoices
from extras.choices import CustomFieldUIVisibleChoices
from extras.choices import CustomLinkButtonClassChoices


if TYPE_CHECKING:
    from django.apps import apps
    from django.db.backends.base.schema import BaseDatabaseSchemaEditor


def create_custom_field(
    apps: "apps",
    field_name: str,
    label: str,
    models: list,
    object_type=None,
    cf_type: str | None = "type_text",
):
    """Create a single custom field and link it to required models."""
    ObjectType = apps.get_model("core", "ObjectType")

    defaults = {
        "label": label,
        "related_object_type": (
            ObjectType.objects.get_for_model(object_type) if object_type else None
        ),
        "ui_visible": getattr(CustomFieldUIVisibleChoices, "ALWAYS"),
        "ui_editable": getattr(CustomFieldUIEditableChoices, "NO"),
    }

    custom_field, _ = apps.get_model("extras", "CustomField").objects.update_or_create(
        type=getattr(CustomFieldTypeChoices, cf_type.upper()),
        name=field_name,
        defaults=defaults,
    )

    for model in models:
        custom_field.object_types.add(ObjectType.objects.get_for_model(model))


def prepare_custom_fields(apps: "apps", schema_editor: "BaseDatabaseSchemaEditor"):
    """Forward migration to prepare ipfabric_netbox custom fields and links."""
    Device = apps.get_model("dcim", "Device")
    Site = apps.get_model("dcim", "Site")

    create_custom_field(
        apps,
        "ipfabric_source",
        "IP Fabric Source",
        [Device, Site],
        cf_type="type_object",
        object_type=apps.get_model("ipfabric_netbox", "IPFabricSource"),
    )
    create_custom_field(
        apps,
        "ipfabric_branch",
        "IP Fabric Last Sync",
        [Device, Site],
        cf_type="type_object",
        object_type=apps.get_model("ipfabric_netbox", "IPFabricBranch"),
    )
    cl, _ = apps.get_model("extras", "CustomLink").objects.update_or_create(
        defaults={
            "link_text": "{% if object.custom_field_data.ipfabric_source is defined %}{% set SOURCE_ID = object.custom_field_data.ipfabric_source %}{% if SOURCE_ID %}IP Fabric{% endif %}{% endif %}",
            "link_url": '{% if object.custom_field_data.ipfabric_source is defined %}{% set SOURCE_ID = object.custom_field_data.ipfabric_source %}{% if SOURCE_ID %}{% set BASE_URL = object.custom_fields.filter(related_object_type__model="ipfabricsource").first().related_object_type.model_class().objects.get(pk=SOURCE_ID).url %}{{ BASE_URL }}/inventory/devices?options={"filters":{"sn": ["like","{{ object.serial }}"]}}{% endif %}{%endif%}',
            "new_window": True,
            "button_class": CustomLinkButtonClassChoices.BLUE,
        },
        name="ipfabric",
    )
    cl.object_types.add(
        apps.get_model("core", "ObjectType").objects.get_for_model(Device)
    )


def cleanup_custom_fields(apps: "apps", schema_editor: "BaseDatabaseSchemaEditor"):
    """Reverse migration to prepare ipfabric_netbox custom fields and links."""
    for custom_field_name in ["ipfabric_source", "ipfabric_branch"]:
        custom_field = apps.get_model("extras", "CustomField").objects.get(
            name=custom_field_name
        )
        for model in custom_field.object_types.all()[:]:
            custom_field.object_types.remove(model)
        custom_field.delete()


class Migration(migrations.Migration):
    dependencies = [
        ("dcim", "0191_module_bay_rebuild"),
        ("extras", "0121_customfield_related_object_filter"),
        (
            "ipfabric_netbox",
            "0006_alter_ipfabrictransformmap_target_model",
        ),
    ]
    operations = [
        migrations.RunPython(prepare_custom_fields, cleanup_custom_fields),
    ]
