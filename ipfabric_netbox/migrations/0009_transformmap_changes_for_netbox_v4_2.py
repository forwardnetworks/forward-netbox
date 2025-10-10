import contextlib
import re
from typing import TYPE_CHECKING

import django.db.models.deletion
from django.db import migrations
from django.db import models

from ipfabric_netbox.utilities.transform_map import build_transform_maps
from ipfabric_netbox.utilities.transform_map import get_transform_map


if TYPE_CHECKING:
    from django.apps import apps
    from django.db.backends.base.schema import BaseDatabaseSchemaEditor
    from ipfabric_netbox.models import IPFabricTransformField, IPFabricTransformMap


def get_current_transform_map(
    apps: "apps",
    target_model: str,
    app_label: str = "dcim",
    source_model: str = "interface",
) -> "IPFabricTransformMap":
    return apps.get_model("ipfabric_netbox", "IPFabricTransformMap").objects.get(
        source_model=source_model,
        target_model=apps.get_model("contenttypes", "ContentType").objects.get(
            app_label=app_label,
            model=target_model,
        ),
    )


def get_current_map_field(apps: "apps", target_model: str) -> "IPFabricTransformField":
    """Finds the current TransformMapField for MAC address so we keep the template.
    We need to do this because some customer might have altered it."""
    return apps.get_model("ipfabric_netbox", "IPFabricTransformField").objects.get(
        source_field="mac",
        target_field="mac_address",
        transform_map=get_current_transform_map(apps, target_model),
    )


def create_transform_map(apps: "apps", schema_editor: "BaseDatabaseSchemaEditor"):
    """Create transform map structure for MAC Address (new in NetBox v4.2).
    Keeps current template for MAC Address if it exists.

    Also fixes interface.duplex not being populated."""

    with contextlib.suppress(
        apps.get_model("ipfabric_netbox", "IPFabricTransformMap").DoesNotExist
    ):
        # The map will already be present if we're using newer transform map.
        # This means we don't need to run the migration since it got created with 0008_prepare_transform_maps migration
        get_current_transform_map(apps, "macaddress")
        return

    # region - MAC Address
    current_mac_field = get_current_map_field(apps, "interface")
    transform_map_data = get_transform_map()

    mac_address_transform_map = None
    # Get MAC Address transform map defined in transform_map.json
    for i, transform_map in enumerate(transform_map_data[:]):
        if transform_map["data"]["name"] == "MAC Address Transform Map":
            mac_address_transform_map = [transform_map]
    # Replace the template only the current field is there
    if current_mac_field:
        for field_map in mac_address_transform_map[0]["field_maps"]:
            if field_map["source_field"] == "mac":
                field_map["template"] = current_mac_field.template

    build_transform_maps(mac_address_transform_map)
    current_mac_field.delete()
    # endregion

    # region - duplex
    # Create duplex transform field for Interface. Ignore if it already exists
    IPFabricTransformField = apps.get_model("ipfabric_netbox", "IPFabricTransformField")
    field_data = {
        "source_field": "duplex",
        "target_field": "duplex",
        "transform_map": get_current_transform_map(apps, "interface"),
    }
    try:
        IPFabricTransformField.objects.get(**field_data)
    except IPFabricTransformField.DoesNotExist:
        IPFabricTransformField(**field_data).save()
    # endregion

    # region - Prefix site
    # Prefix.site has changed to Prefix.scope relation (to allow site groups etc.)
    prefix_transform_map = get_current_transform_map(
        apps, source_model="prefix", app_label="ipam", target_model="prefix"
    )
    current_site_relationship = prefix_transform_map.relationship_maps.get(
        target_field="site"
    )
    IPFabricTransformField(
        source_field="siteName",
        target_field="scope_id",
        coalesce=True,
        template="{% if object.siteName is defined %}"
        + current_site_relationship.template
        + "{% else %}None{% endif %}",
        transform_map=prefix_transform_map,
    ).save()
    current_site_relationship.template = '{% if object.siteName is defined %}{{ contenttypes.ContentType.objects.get(app_label="dcim", model="site").pk }}{% else %}None{% endif %}'
    current_site_relationship.source_model = apps.get_model(
        "contenttypes", "ContentType"
    ).objects.get(app_label="contenttypes", model="contenttype")
    current_site_relationship.target_field = "scope_type"
    current_site_relationship.save()
    # endregion


def delete_transform_map(apps: "apps", schema_editor: "BaseDatabaseSchemaEditor"):
    current_mac_field = get_current_map_field(apps, "macaddress")

    # region - MAC Address
    apps.get_model("ipfabric_netbox", "IPFabricTransformField")(
        coalesce=False,
        source_field="mac",
        target_field="mac_address",
        template=current_mac_field.template,
        transform_map=get_current_transform_map(apps, "interface"),
    ).save()

    # Use CASCADE to delete the transform fields and relationships
    current_mac_field.transform_map.delete()
    # endregion

    # region - duplex
    # Delete new duplex field
    apps.get_model("ipfabric_netbox", "IPFabricTransformField").objects.get(
        source_field="duplex",
        target_field="duplex",
        transform_map=get_current_transform_map(apps, "interface"),
    ).delete()
    # endregion

    # region - Prefix site
    prefix_transform_map = get_current_transform_map(
        apps, source_model="prefix", app_label="ipam", target_model="prefix"
    )
    current_site_relationship = prefix_transform_map.relationship_maps.get(
        target_field="scope_type"
    )
    current_site_field = prefix_transform_map.field_maps.get(source_field="siteName")
    match = re.search(r"is defined %}(.*){% else %}None", current_site_field.template)
    current_site_field.delete()
    if match:
        current_site_relationship.template = match.group(1)
    else:
        current_site_relationship.template = "{% set SLUG = object.siteName | slugify %}{{ dcim.Site.objects.filter(slug=SLUG).first().pk }}"
    current_site_relationship.source_model = apps.get_model(
        "contenttypes", "ContentType"
    ).objects.get(app_label="dcim", model="site")
    current_site_relationship.target_field = "site"
    current_site_relationship.save()
    # endregion


def set_macaddress_sync_param(apps: "apps", schema_editor: "BaseDatabaseSchemaEditor"):
    IPFabricSync = apps.get_model("ipfabric_netbox", "IPFabricSync")
    for sync in IPFabricSync.objects.all():
        if sync.parameters.get("interface"):
            sync.parameters["macaddress"] = True
        else:
            sync.parameters["macaddress"] = False
        sync.save()


def remove_macaddress_sync_param(
    apps: "apps", schema_editor: "BaseDatabaseSchemaEditor"
):
    IPFabricSync = apps.get_model("ipfabric_netbox", "IPFabricSync")
    for sync in IPFabricSync.objects.all():
        sync.parameters.pop("macaddress", None)
        sync.save()


class Migration(migrations.Migration):
    dependencies = [
        ("contenttypes", "0002_remove_content_type_name"),
        ("ipfabric_netbox", "0008_prepare_transform_maps"),
    ]

    operations = [
        migrations.AlterField(
            model_name="ipfabrictransformmap",
            name="target_model",
            field=models.ForeignKey(
                limit_choices_to=models.Q(
                    models.Q(
                        models.Q(("app_label", "dcim"), ("model", "site")),
                        models.Q(("app_label", "dcim"), ("model", "manufacturer")),
                        models.Q(("app_label", "dcim"), ("model", "platform")),
                        models.Q(("app_label", "dcim"), ("model", "devicerole")),
                        models.Q(("app_label", "dcim"), ("model", "devicetype")),
                        models.Q(("app_label", "dcim"), ("model", "device")),
                        models.Q(("app_label", "dcim"), ("model", "virtualchassis")),
                        models.Q(("app_label", "dcim"), ("model", "interface")),
                        models.Q(("app_label", "dcim"), ("model", "macaddress")),
                        models.Q(("app_label", "ipam"), ("model", "vlan")),
                        models.Q(("app_label", "ipam"), ("model", "vrf")),
                        models.Q(("app_label", "ipam"), ("model", "prefix")),
                        models.Q(("app_label", "ipam"), ("model", "ipaddress")),
                        models.Q(
                            ("app_label", "contenttypes"), ("model", "contenttype")
                        ),
                        models.Q(("app_label", "tenancy"), ("model", "tenant")),
                        models.Q(("app_label", "dcim"), ("model", "inventoryitem")),
                        _connector="OR",
                    )
                ),
                on_delete=django.db.models.deletion.PROTECT,
                related_name="+",
                to="contenttypes.contenttype",
            ),
        ),
        migrations.RunPython(set_macaddress_sync_param, remove_macaddress_sync_param),
        migrations.RunPython(create_transform_map, delete_transform_map),
    ]
