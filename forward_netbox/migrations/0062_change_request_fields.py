import django.core.validators
from django.db import migrations
from django.db import models


class Migration(migrations.Migration):
    dependencies = [
        ("forward_netbox", "0061_static_route_nqe_map_choice"),
    ]

    operations = [
        migrations.AddField(
            model_name="forwardchange",
            name="priority",
            field=models.PositiveSmallIntegerField(
                default=3,
                validators=[
                    django.core.validators.MinValueValidator(1),
                    django.core.validators.MaxValueValidator(5),
                ],
            ),
        ),
        migrations.AddField(
            model_name="forwardchange",
            name="summary",
            field=models.TextField(blank=True, default=""),
        ),
    ]
