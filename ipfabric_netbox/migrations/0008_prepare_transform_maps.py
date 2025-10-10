from typing import TYPE_CHECKING

from django.db import migrations

from ipfabric_netbox.utilities.transform_map import build_transform_maps
from ipfabric_netbox.utilities.transform_map import get_transform_map


if TYPE_CHECKING:
    from django.apps import apps
    from django.db.backends.base.schema import BaseDatabaseSchemaEditor


def prepare_transform_maps(apps: "apps", schema_editor: "BaseDatabaseSchemaEditor"):
    """Create transform maps if they do not exist yet.
    They used to be created during plugin.ready() so they might be present on older DBs.
    """
    if apps.get_model("ipfabric_netbox", "IPFabricTransformMap").objects.count() == 0:
        build_transform_maps(data=get_transform_map())


def cleanup_transform_maps(apps: "apps", schema_editor: "BaseDatabaseSchemaEditor"):
    """Delete all transform maps."""
    # IPFabricTransformField and IPFabricRelationshipField are deleted by CASCADE
    apps.get_model("ipfabric_netbox", "IPFabricTransformMap").objects.all().delete()


class Migration(migrations.Migration):
    # Depend on all models that are used in transform maps
    dependencies = [
        ("core", "0012_job_object_type_optional"),
        ("dcim", "0191_module_bay_rebuild"),
        ("extras", "0121_customfield_related_object_filter"),
        ("ipam", "0070_vlangroup_vlan_id_ranges"),
        (
            "ipfabric_netbox",
            "0007_prepare_custom_fields",
        ),
    ]
    operations = [
        migrations.RunPython(prepare_transform_maps, cleanup_transform_maps),
    ]
