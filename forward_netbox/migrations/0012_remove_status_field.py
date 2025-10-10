"""
NIM-18527.
Remove status field from TransformMap.
"""
from django.db import migrations


class Migration(migrations.Migration):
    dependencies = [
        ("forward_netbox", "0011_update_part_number_DCIM_inventory_item_template"),
    ]

    operations = [
        migrations.RemoveField(
            model_name="forwardtransformmap",
            name="status",
        ),
    ]
