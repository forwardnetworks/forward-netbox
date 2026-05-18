from django.db import migrations
from django.db import models


class Migration(migrations.Migration):
    dependencies = [
        ("forward_netbox", "0014_execution_step_fetch_scope"),
    ]

    operations = [
        migrations.AddField(
            model_name="forwardexecutionstep",
            name="apply_engine",
            field=models.CharField(
                blank=True,
                choices=[
                    ("adapter", "Adapter"),
                    ("bulk_orm", "Bulk ORM"),
                    ("turbobulk", "TurboBulk"),
                    ("parquet_bulk", "Parquet bulk"),
                ],
                default="adapter",
                max_length=30,
            ),
        ),
    ]
