from django.db import migrations
from django.db import models


class Migration(migrations.Migration):
    dependencies = [
        ("forward_netbox", "0010_forwardvalidationrun_override_applied_and_more"),
    ]

    operations = [
        migrations.AddField(
            model_name="forwardingestion",
            name="change_request_id",
            field=models.UUIDField(blank=True, db_index=True, null=True),
        ),
    ]
