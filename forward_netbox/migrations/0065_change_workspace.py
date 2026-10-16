from django.db import migrations
from django.db import models


class Migration(migrations.Migration):
    dependencies = [
        ("forward_netbox", "0064_change_comments"),
    ]

    operations = [
        migrations.AddField(
            model_name="forwardchange",
            name="workspace_network_id",
            field=models.CharField(blank=True, default="", max_length=100),
        ),
        migrations.AddField(
            model_name="forwardchange",
            name="workspace_origin",
            field=models.CharField(blank=True, default="", max_length=16),
        ),
    ]
