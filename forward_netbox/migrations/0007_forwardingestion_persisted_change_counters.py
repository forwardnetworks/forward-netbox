from django.db import migrations
from django.db import models


class Migration(migrations.Migration):
    dependencies = [
        ("forward_netbox", "0006_alter_forwardnqemap_netbox_model"),
    ]

    operations = [
        migrations.AddField(
            model_name="forwardingestion",
            name="applied_change_count",
            field=models.PositiveIntegerField(default=0),
        ),
        migrations.AddField(
            model_name="forwardingestion",
            name="created_change_count",
            field=models.PositiveIntegerField(default=0),
        ),
        migrations.AddField(
            model_name="forwardingestion",
            name="deleted_change_count",
            field=models.PositiveIntegerField(default=0),
        ),
        migrations.AddField(
            model_name="forwardingestion",
            name="failed_change_count",
            field=models.PositiveIntegerField(default=0),
        ),
        migrations.AddField(
            model_name="forwardingestion",
            name="updated_change_count",
            field=models.PositiveIntegerField(default=0),
        ),
    ]
