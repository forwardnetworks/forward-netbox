from django.db import migrations
from django.db import models


DEFAULT_COALESCE_FIELDS = {
    "dcim.site": [["slug"], ["name"]],
    "dcim.manufacturer": [["slug"], ["name"]],
    "dcim.devicerole": [["slug"], ["name"]],
    "dcim.platform": [["slug"], ["name"]],
    "dcim.devicetype": [["manufacturer_slug", "slug"], ["manufacturer_slug", "model"]],
    "dcim.device": [["name"]],
    "dcim.virtualchassis": [["name"]],
    "dcim.interface": [["device", "name"]],
    "dcim.macaddress": [["mac_address"]],
    "ipam.vlan": [["site", "vid"]],
    "ipam.vrf": [["rd"], ["name"]],
    "ipam.prefix": [["prefix", "vrf"]],
    "ipam.ipaddress": [["address", "vrf"]],
    "dcim.inventoryitem": [["device", "name", "part_id", "serial"]],
}


def backfill_coalesce_fields(apps, schema_editor):
    ForwardNQEMap = apps.get_model("forward_netbox", "ForwardNQEMap")
    for nqe_map in ForwardNQEMap.objects.select_related("netbox_model").all():
        model_string = f"{nqe_map.netbox_model.app_label}.{nqe_map.netbox_model.model}"
        nqe_map.coalesce_fields = DEFAULT_COALESCE_FIELDS.get(model_string, [["name"]])
        nqe_map.save(update_fields=["coalesce_fields"])


def noop(apps, schema_editor):
    pass


class Migration(migrations.Migration):
    dependencies = [
        ("forward_netbox", "0001_initial"),
    ]

    operations = [
        migrations.AddField(
            model_name="forwardnqemap",
            name="coalesce_fields",
            field=models.JSONField(blank=True, default=list),
        ),
        migrations.RunPython(backfill_coalesce_fields, noop),
    ]
