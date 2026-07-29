from typing import ClassVar

import django.db.models.deletion
import django.utils.timezone
from django.db import migrations
from django.db import models

fake_on_branch = True


class Migration(migrations.Migration):
    dependencies: ClassVar[list] = [
        ("forward_netbox", "0044_contributor_baseline_cache"),
    ]

    operations: ClassVar[list] = [
        migrations.CreateModel(
            name="ForwardMergeAttempt",
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
                ("attempt_number", models.PositiveIntegerField()),
                (
                    "status",
                    models.CharField(
                        choices=[
                            ("running", "Running"),
                            ("applied", "Branch changes applied"),
                            ("completed", "Completed"),
                            ("failed", "Failed"),
                            ("interrupted", "Interrupted"),
                        ],
                        db_index=True,
                        default="running",
                        max_length=20,
                    ),
                ),
                ("phase", models.CharField(default="preparing", max_length=32)),
                ("total_changes", models.PositiveBigIntegerField(default=0)),
                ("merged_changes", models.PositiveBigIntegerField(default=0)),
                ("failed_changes", models.PositiveBigIntegerField(default=0)),
                (
                    "current_model",
                    models.CharField(blank=True, default="", max_length=100),
                ),
                ("model_progress", models.JSONField(blank=True, default=dict)),
                ("checkpoint_sequence", models.PositiveBigIntegerField(default=0)),
                (
                    "started_at",
                    models.DateTimeField(
                        default=django.utils.timezone.now,
                        editable=False,
                    ),
                ),
                (
                    "heartbeat_at",
                    models.DateTimeField(
                        db_index=True,
                        default=django.utils.timezone.now,
                    ),
                ),
                ("finished_at", models.DateTimeField(blank=True, null=True)),
                (
                    "failure_kind",
                    models.CharField(blank=True, default="", max_length=32),
                ),
                (
                    "exception_type",
                    models.CharField(blank=True, default="", max_length=255),
                ),
                ("failure_summary", models.TextField(blank=True, default="")),
                ("traceback", models.TextField(blank=True, default="")),
                ("process_wait_status", models.IntegerField(blank=True, null=True)),
                ("process_exit_code", models.IntegerField(blank=True, null=True)),
                (
                    "process_signal",
                    models.PositiveSmallIntegerField(blank=True, null=True),
                ),
                (
                    "process_signal_name",
                    models.CharField(blank=True, default="", max_length=32),
                ),
                (
                    "ingestion",
                    models.ForeignKey(
                        on_delete=django.db.models.deletion.CASCADE,
                        related_name="merge_attempts",
                        to="forward_netbox.forwardingestion",
                    ),
                ),
                (
                    "job",
                    models.ForeignKey(
                        blank=True,
                        null=True,
                        on_delete=django.db.models.deletion.SET_NULL,
                        related_name="+",
                        to="core.job",
                    ),
                ),
            ],
            options={
                "verbose_name": "Forward Merge Attempt",
                "verbose_name_plural": "Forward Merge Attempts",
                "db_table": "forward_netbox_merge_attempt",
                "ordering": ("ingestion_id", "-attempt_number"),
            },
        ),
        migrations.AddConstraint(
            model_name="forwardmergeattempt",
            constraint=models.UniqueConstraint(
                fields=("ingestion", "attempt_number"),
                name="forward_merge_attempt_ingestion_number",
            ),
        ),
    ]
