from django.db import migrations
from django.db import models


class Migration(migrations.Migration):

    dependencies = [
        ("forward_netbox", "0003_forwardingestionissue_context_fields"),
    ]

    operations = [
        migrations.AddField(
            model_name="forwardingestion",
            name="baseline_ready",
            field=models.BooleanField(default=False),
        ),
        migrations.AddField(
            model_name="forwardingestion",
            name="sync_mode",
            field=models.CharField(
                choices=[("full", "Full"), ("diff", "Diff"), ("hybrid", "Hybrid")],
                default="full",
                max_length=10,
            ),
        ),
    ]
