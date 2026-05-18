from django.db import migrations
from django.db import models


class Migration(migrations.Migration):
    dependencies = [
        ("forward_netbox", "0016_execution_step_query_metrics"),
    ]

    operations = [
        migrations.AddField(
            model_name="forwardexecutionstep",
            name="attempted_row_count",
            field=models.PositiveIntegerField(default=0),
        ),
        migrations.AddField(
            model_name="forwardexecutionstep",
            name="applied_row_count",
            field=models.PositiveIntegerField(default=0),
        ),
        migrations.AddField(
            model_name="forwardexecutionstep",
            name="skipped_row_count",
            field=models.PositiveIntegerField(default=0),
        ),
        migrations.AddField(
            model_name="forwardexecutionstep",
            name="failed_row_count",
            field=models.PositiveIntegerField(default=0),
        ),
    ]
