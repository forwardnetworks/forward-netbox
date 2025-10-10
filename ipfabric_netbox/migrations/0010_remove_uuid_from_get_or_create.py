from typing import TYPE_CHECKING

from django.db import migrations

from ipfabric_netbox.utilities.transform_map import get_transform_map

if TYPE_CHECKING:
    from django.apps import apps
    from django.db.backends.base.schema import BaseDatabaseSchemaEditor


items_to_change = {
    ("ipaddress", "ipam", "ipaddress"): {"fields": [], "relationships": ["vrf"]},
    ("device", "dcim", "platform"): {"fields": [], "relationships": ["manufacturer"]},
    ("device", "dcim", "device"): {
        "fields": [],
        "relationships": ["platform", "site", "device_type", "role"],
    },
    ("device", "dcim", "devicetype"): {"fields": [], "relationships": ["manufacturer"]},
    ("interface", "dcim", "interface"): {"fields": [], "relationships": ["device"]},
    ("part_number", "dcim", "inventoryitem"): {
        "fields": ["part_id"],
        "relationships": ["device", "manufacturer"],
    },
    ("vlan", "ipam", "vlan"): {"fields": ["name"], "relationships": ["site"]},
    ("prefix", "ipam", "prefix"): {"fields": ["scope_id"], "relationships": ["vrf"]},
    ("virtualchassis", "dcim", "virtualchassis"): {
        "fields": [],
        "relationships": ["master"],
    },
}


def add_templates(apps: "apps", schema_editor: "BaseDatabaseSchemaEditor"):
    transform_map_data = get_transform_map()

    for item in transform_map_data:
        transform_map_id = (
            item["data"]["source_model"],
            item["data"]["target_model"]["app_label"],
            item["data"]["target_model"]["model"],
        )
        if transform_map_id not in items_to_change:
            continue
        source_model, app_label, target_model = transform_map_id
        transform_map, _ = apps.get_model(
            "ipfabric_netbox", "IPFabricTransformMap"
        ).objects.get_or_create(
            source_model=source_model,
            target_model=apps.get_model("contenttypes", "ContentType").objects.get(
                app_label=app_label,
                model=target_model,
            ),
            defaults={"name": item["data"]["name"]},
        )

        change_data = items_to_change[transform_map_id]

        for field_map in item["field_maps"]:
            f_target_field = field_map.pop("target_field", None)
            if f_target_field not in change_data["fields"]:
                continue
            field, _ = apps.get_model(
                "ipfabric_netbox", "IPFabricTransformField"
            ).objects.get_or_create(
                target_field=f_target_field,
                transform_map=transform_map,
                defaults=field_map,
            )
            field.template = field_map["template"]
            field.save()

        for relationship_map in item["relationship_maps"]:
            rel_target_field = relationship_map.pop("target_field", None)
            if rel_target_field not in change_data["relationships"]:
                continue
            relationship_map["source_model"] = apps.get_model(
                "contenttypes", "ContentType"
            ).objects.get(**relationship_map.pop("source_model"))
            relationship, _ = apps.get_model(
                "ipfabric_netbox", "IPFabricRelationshipField"
            ).objects.get_or_create(
                target_field=rel_target_field,
                transform_map=transform_map,
                defaults=relationship_map,
            )
            relationship.template = relationship_map["template"]
            relationship.save()


def return_templates(apps: "apps", schema_editor: "BaseDatabaseSchemaEditor"):
    """It would be way too complex to revert this migration and there is no need to do so.
    The original code works the same with or without the templates."""
    pass


class Migration(migrations.Migration):
    dependencies = [
        ("ipfabric_netbox", "0009_transformmap_changes_for_netbox_v4_2"),
    ]

    operations = [
        migrations.RunPython(add_templates, return_templates),
    ]
