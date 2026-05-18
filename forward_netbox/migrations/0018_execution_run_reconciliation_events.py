from django.db import migrations
from django.db import models


class Migration(migrations.Migration):
    dependencies = [
        ("forward_netbox", "0017_execution_step_row_counters"),
    ]

    operations = [
        migrations.AddField(
            model_name="forwardexecutionrun",
            name="reconciliation_events",
            field=models.JSONField(blank=True, default=list),
        ),
    ]
