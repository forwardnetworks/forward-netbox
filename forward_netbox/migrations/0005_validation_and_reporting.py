import django.core.validators
import django.db.models.deletion
import django.utils.timezone
import netbox.models.deletion
from django.db import migrations
from django.db import models


class Migration(migrations.Migration):

    dependencies = [
        ("core", "0020_owner"),
        ("forward_netbox", "0004_forwardingestion_baseline_fields"),
    ]

    operations = [
        migrations.CreateModel(
            name="ForwardDriftPolicy",
            fields=[
                (
                    "id",
                    models.BigAutoField(
                        auto_created=True, primary_key=True, serialize=False
                    ),
                ),
                ("created", models.DateTimeField(auto_now_add=True, null=True)),
                ("last_updated", models.DateTimeField(auto_now=True, null=True)),
                ("name", models.CharField(max_length=100, unique=True)),
                ("enabled", models.BooleanField(default=True)),
                (
                    "baseline_mode",
                    models.CharField(
                        choices=[
                            ("latest_merged", "Latest merged ingestion"),
                            ("none", "No baseline"),
                        ],
                        default="latest_merged",
                        max_length=30,
                    ),
                ),
                ("require_processed_snapshot", models.BooleanField(default=True)),
                ("block_on_query_errors", models.BooleanField(default=True)),
                ("block_on_zero_rows", models.BooleanField(default=False)),
                (
                    "max_deleted_objects",
                    models.PositiveIntegerField(blank=True, null=True),
                ),
                (
                    "max_deleted_percent",
                    models.PositiveIntegerField(
                        blank=True,
                        null=True,
                        validators=[
                            django.core.validators.MinValueValidator(0),
                            django.core.validators.MaxValueValidator(100),
                        ],
                    ),
                ),
            ],
            options={
                "verbose_name": "Forward Drift Policy",
                "verbose_name_plural": "Forward Drift Policies",
                "db_table": "forward_netbox_drift_policy",
                "ordering": ("name",),
            },
            bases=(netbox.models.deletion.DeleteMixin, models.Model),
        ),
        migrations.CreateModel(
            name="ForwardValidationRun",
            fields=[
                (
                    "id",
                    models.BigAutoField(
                        auto_created=True, primary_key=True, serialize=False
                    ),
                ),
                (
                    "status",
                    models.CharField(
                        choices=[
                            ("queued", "Queued"),
                            ("running", "Running"),
                            ("passed", "Passed"),
                            ("blocked", "Blocked"),
                            ("failed", "Failed"),
                        ],
                        default="queued",
                        max_length=20,
                    ),
                ),
                ("allowed", models.BooleanField(default=False)),
                (
                    "snapshot_selector",
                    models.CharField(blank=True, default="", max_length=100),
                ),
                (
                    "snapshot_id",
                    models.CharField(blank=True, default="", max_length=100),
                ),
                (
                    "baseline_snapshot_id",
                    models.CharField(blank=True, default="", max_length=100),
                ),
                ("snapshot_info", models.JSONField(blank=True, default=dict)),
                ("snapshot_metrics", models.JSONField(blank=True, default=dict)),
                ("model_results", models.JSONField(blank=True, default=list)),
                ("drift_summary", models.JSONField(blank=True, default=dict)),
                ("blocking_reasons", models.JSONField(blank=True, default=list)),
                (
                    "created",
                    models.DateTimeField(
                        default=django.utils.timezone.now, editable=False
                    ),
                ),
                ("started", models.DateTimeField(blank=True, null=True)),
                ("completed", models.DateTimeField(blank=True, null=True)),
                (
                    "job",
                    models.ForeignKey(
                        blank=True,
                        null=True,
                        on_delete=django.db.models.deletion.SET_NULL,
                        to="core.job",
                    ),
                ),
                (
                    "policy",
                    models.ForeignKey(
                        blank=True,
                        null=True,
                        on_delete=django.db.models.deletion.SET_NULL,
                        related_name="validation_runs",
                        to="forward_netbox.forwarddriftpolicy",
                    ),
                ),
                (
                    "sync",
                    models.ForeignKey(
                        on_delete=django.db.models.deletion.CASCADE,
                        related_name="validation_runs",
                        to="forward_netbox.forwardsync",
                    ),
                ),
            ],
            options={
                "verbose_name": "Forward Validation Run",
                "verbose_name_plural": "Forward Validation Runs",
                "db_table": "forward_netbox_validation_run",
                "ordering": ("-pk",),
            },
        ),
        migrations.AddField(
            model_name="forwardingestion",
            name="model_results",
            field=models.JSONField(blank=True, default=list),
        ),
        migrations.AddField(
            model_name="forwardingestion",
            name="validation_run",
            field=models.ForeignKey(
                blank=True,
                null=True,
                on_delete=django.db.models.deletion.SET_NULL,
                related_name="ingestions",
                to="forward_netbox.forwardvalidationrun",
            ),
        ),
        migrations.AddField(
            model_name="forwardsync",
            name="drift_policy",
            field=models.ForeignKey(
                blank=True,
                null=True,
                on_delete=django.db.models.deletion.SET_NULL,
                related_name="syncs",
                to="forward_netbox.forwarddriftpolicy",
            ),
        ),
    ]
