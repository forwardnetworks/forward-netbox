from django.db import migrations
from django.db import models


class Migration(migrations.Migration):
    dependencies = [
        ("forward_netbox", "0015_execution_step_apply_engine"),
    ]

    operations = [
        migrations.AddField(
            model_name="forwardexecutionstep",
            name="fetched_row_count",
            field=models.PositiveIntegerField(default=0),
        ),
        migrations.AddField(
            model_name="forwardexecutionstep",
            name="query_runtime_ms",
            field=models.FloatField(blank=True, null=True),
        ),
    ]
