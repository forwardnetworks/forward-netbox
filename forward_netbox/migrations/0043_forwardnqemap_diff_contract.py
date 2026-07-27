from django.db import migrations
from django.db import models


fake_on_branch = True


class Migration(migrations.Migration):
    dependencies = [
        ("forward_netbox", "0042_device_generic_relation_guards"),
    ]

    operations = [
        migrations.AddField(
            model_name="forwardnqemap",
            name="diff_commit_id",
            field=models.CharField(
                blank=True,
                default="",
                editable=False,
                max_length=100,
            ),
        ),
        migrations.AddField(
            model_name="forwardnqemap",
            name="diff_source_sha256",
            field=models.CharField(
                blank=True,
                default="",
                editable=False,
                max_length=64,
            ),
        ),
        migrations.AddField(
            model_name="forwardnqemap",
            name="full_source_sha256",
            field=models.CharField(
                blank=True,
                default="",
                editable=False,
                max_length=64,
            ),
        ),
    ]
