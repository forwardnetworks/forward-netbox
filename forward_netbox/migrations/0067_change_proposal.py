import django.db.models.deletion
from django.db import migrations
from django.db import models


class Migration(migrations.Migration):

    dependencies = [
        ("forward_netbox", "0066_change_validation"),
    ]

    operations = [
        migrations.AddField(
            model_name="forwardchange",
            name="predict_change_set_id",
            field=models.CharField(
                blank=True, db_default="", default="", max_length=100
            ),
        ),
        migrations.AddField(
            model_name="forwardchange",
            name="predict_snapshot_id",
            field=models.CharField(
                blank=True, db_default="", default="", max_length=100
            ),
        ),
        migrations.AddField(
            model_name="forwardchange",
            name="predict_digest",
            field=models.CharField(
                blank=True, db_default="", default="", max_length=64
            ),
        ),
        migrations.AddField(
            model_name="forwardchange",
            name="predict_started_at",
            field=models.DateTimeField(blank=True, null=True),
        ),
        migrations.CreateModel(
            name="ForwardChangeProposal",
            fields=[
                (
                    "id",
                    models.BigAutoField(
                        auto_created=True, primary_key=True, serialize=False
                    ),
                ),
                ("kind", models.CharField(default="cli", max_length=16)),
                ("target", models.CharField(max_length=255)),
                ("body", models.TextField(blank=True, default="")),
                (
                    "change",
                    models.ForeignKey(
                        on_delete=django.db.models.deletion.CASCADE,
                        related_name="proposals",
                        to="forward_netbox.forwardchange",
                    ),
                ),
            ],
            options={
                "verbose_name": "Forward Change Proposal",
                "verbose_name_plural": "Forward Change Proposals",
                "db_table": "forward_netbox_change_proposal",
                "ordering": ("target",),
            },
        ),
        migrations.AddConstraint(
            model_name="forwardchangeproposal",
            constraint=models.UniqueConstraint(
                fields=("change", "kind", "target"),
                name="forward_change_proposal_unique",
            ),
        ),
    ]
