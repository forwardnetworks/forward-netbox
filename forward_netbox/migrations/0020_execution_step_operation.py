from django.db import migrations
from django.db import models


class Migration(migrations.Migration):
    dependencies = [
        ("forward_netbox", "0019_forwardexecutionstep_query_parameters"),
    ]

    operations = [
        migrations.AddField(
            model_name="forwardexecutionstep",
            name="operation",
            field=models.CharField(blank=True, default="mixed", max_length=20),
        ),
    ]
