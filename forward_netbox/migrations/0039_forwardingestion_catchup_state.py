from django.db import migrations
from django.db import models


fake_on_branch = True


class Migration(migrations.Migration):
    dependencies = [
        ("forward_netbox", "0038_managed_tag_domain_guard"),
    ]

    operations = [
        migrations.AddField(
            model_name="forwardingestion",
            name="catchup_checked_at",
            field=models.DateTimeField(blank=True, null=True),
        ),
        migrations.AddField(
            model_name="forwardingestion",
            name="catchup_error_type",
            field=models.CharField(blank=True, default="", max_length=255),
        ),
        migrations.AddField(
            model_name="forwardingestion",
            name="catchup_reason",
            field=models.CharField(blank=True, default="", max_length=100),
        ),
        migrations.AddField(
            model_name="forwardingestion",
            name="catchup_status",
            field=models.CharField(
                choices=[
                    ("not_applicable", "Not applicable"),
                    ("pending", "Pending"),
                    ("queued", "Queued"),
                    ("current", "Current"),
                    ("failed", "Failed"),
                ],
                db_index=True,
                default="not_applicable",
                max_length=20,
            ),
        ),
        migrations.AddField(
            model_name="forwardingestion",
            name="catchup_target_snapshot_id",
            field=models.CharField(blank=True, default="", max_length=100),
        ),
    ]
