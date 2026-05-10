from django.db import migrations
from django.db import models


class Migration(migrations.Migration):
    dependencies = [
        ("forward_netbox", "0011_forwardingestion_change_request_id"),
    ]

    operations = [
        migrations.AddField(
            model_name="forwardnqemap",
            name="query_repository",
            field=models.CharField(blank=True, default="", max_length=10),
        ),
        migrations.AddField(
            model_name="forwardnqemap",
            name="query_path",
            field=models.CharField(blank=True, default="", max_length=500),
        ),
    ]
