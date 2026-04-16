from django.db import migrations
from django.db import models


class Migration(migrations.Migration):

    dependencies = [
        ("forward_netbox", "0002_add_coalesce_fields"),
    ]

    operations = [
        migrations.AddField(
            model_name="forwardingestionissue",
            name="coalesce_fields",
            field=models.JSONField(blank=True, default=dict),
        ),
        migrations.AddField(
            model_name="forwardingestionissue",
            name="defaults",
            field=models.JSONField(blank=True, default=dict),
        ),
    ]
