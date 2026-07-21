from django.db import migrations
from django.db import models
import django.db.models.deletion
import django.utils.timezone


fake_on_branch = True


class Migration(migrations.Migration):
    dependencies = [
        ("forward_netbox", "0040_remove_redundant_nqe_settings"),
    ]

    operations = [
        migrations.CreateModel(
            name="ForwardWorkloadState",
            fields=[
                (
                    "id",
                    models.BigAutoField(
                        auto_created=True,
                        primary_key=True,
                        serialize=False,
                        verbose_name="ID",
                    ),
                ),
                ("model_string", models.CharField(max_length=100)),
                ("parameter_hash", models.CharField(max_length=64)),
                ("identity_contract_hash", models.CharField(max_length=64)),
                ("payload", models.BinaryField()),
                ("payload_checksum", models.CharField(max_length=64)),
                ("row_count", models.PositiveIntegerField(default=0)),
                (
                    "snapshot_id",
                    models.CharField(blank=True, default="", max_length=100),
                ),
                ("is_current", models.BooleanField(db_index=True, default=False)),
                (
                    "created",
                    models.DateTimeField(
                        default=django.utils.timezone.now,
                        editable=False,
                    ),
                ),
                (
                    "ingestion",
                    models.ForeignKey(
                        on_delete=django.db.models.deletion.CASCADE,
                        related_name="workload_states",
                        to="forward_netbox.forwardingestion",
                    ),
                ),
                (
                    "sync",
                    models.ForeignKey(
                        on_delete=django.db.models.deletion.CASCADE,
                        related_name="workload_states",
                        to="forward_netbox.forwardsync",
                    ),
                ),
            ],
            options={
                "verbose_name": "Forward Workload State",
                "verbose_name_plural": "Forward Workload States",
                "db_table": "forward_netbox_workload_state",
                "ordering": ("sync_id", "model_string", "-ingestion_id"),
            },
        ),
        migrations.AddConstraint(
            model_name="forwardworkloadstate",
            constraint=models.UniqueConstraint(
                fields=("ingestion", "model_string"),
                name="forward_workload_state_ingestion_model",
            ),
        ),
        migrations.AddConstraint(
            model_name="forwardworkloadstate",
            constraint=models.UniqueConstraint(
                condition=models.Q(("is_current", True)),
                fields=("sync", "model_string"),
                name="forward_workload_state_current_model",
            ),
        ),
    ]
