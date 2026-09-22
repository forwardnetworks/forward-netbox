import django.db.models.deletion
from django.db import migrations
from django.db import models


class Migration(migrations.Migration):
    dependencies = [
        ("forward_netbox", "0056_aci_attachment_nqe_map_choices"),
    ]

    operations = [
        migrations.AddField(
            model_name="forwardnqemap",
            name="last_live_drift",
            field=models.JSONField(blank=True, default=dict),
        ),
        migrations.AddField(
            model_name="forwardnqemap",
            name="last_live_drift_at",
            field=models.DateTimeField(blank=True, null=True),
        ),
    ]
