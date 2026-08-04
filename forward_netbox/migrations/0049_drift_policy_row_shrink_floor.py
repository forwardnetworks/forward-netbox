import django.core.validators
from django.db import migrations
from django.db import models


class Migration(migrations.Migration):
    dependencies = [
        ("forward_netbox", "0048_forwardingestion_skipped_change_count"),
    ]

    operations = [
        # Both fields carry their defaults onto existing policies on purpose.
        # The row-count floor replaces a check that used to run on every sync,
        # so an upgraded instance is protected without an operator action.
        migrations.AddField(
            model_name="forwarddriftpolicy",
            name="block_on_row_shrink",
            field=models.BooleanField(
                default=True,
                help_text=(
                    "Refuse a sync when a model returns far fewer rows than it "
                    "did in the last successful ingestion. The missing rows "
                    "would be reconciled as deletions."
                ),
            ),
        ),
        migrations.AddField(
            model_name="forwarddriftpolicy",
            name="max_row_shrink_percent",
            field=models.PositiveIntegerField(
                default=30,
                help_text=(
                    "How far a model's row count may fall below the last "
                    "successful ingestion before the sync is refused."
                ),
                validators=[
                    django.core.validators.MinValueValidator(0),
                    django.core.validators.MaxValueValidator(100),
                ],
            ),
        ),
    ]
