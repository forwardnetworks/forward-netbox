import django.db.models.deletion
import django.utils.timezone
from django.db import migrations
from django.db import models


class Migration(migrations.Migration):
    dependencies = [
        ("core", "0020_owner"),
        ("netbox_branching", "0007_branch_merge_strategy"),
        ("forward_netbox", "0012_forwardnqemap_query_reference"),
    ]

    operations = [
        migrations.CreateModel(
            name="ForwardExecutionRun",
            fields=[
                (
                    "id",
                    models.BigAutoField(
                        auto_created=True, primary_key=True, serialize=False
                    ),
                ),
                ("backend", models.CharField(default="branching", max_length=30)),
                ("status", models.CharField(default="queued", max_length=30)),
                ("phase", models.CharField(blank=True, default="", max_length=50)),
                ("phase_message", models.TextField(blank=True, default="")),
                (
                    "snapshot_selector",
                    models.CharField(blank=True, default="", max_length=100),
                ),
                (
                    "snapshot_id",
                    models.CharField(blank=True, default="", max_length=100),
                ),
                (
                    "max_changes_per_branch",
                    models.PositiveIntegerField(default=10000),
                ),
                ("auto_merge", models.BooleanField(default=False)),
                ("total_steps", models.PositiveIntegerField(default=0)),
                ("next_step_index", models.PositiveIntegerField(default=1)),
                ("plan_preview", models.JSONField(blank=True, default=dict)),
                ("model_change_density", models.JSONField(blank=True, default=dict)),
                ("latest_heartbeat", models.DateTimeField(blank=True, null=True)),
                ("last_error", models.TextField(blank=True, default="")),
                ("baseline_ready", models.BooleanField(default=False)),
                (
                    "created",
                    models.DateTimeField(
                        default=django.utils.timezone.now, editable=False
                    ),
                ),
                ("updated", models.DateTimeField(auto_now=True)),
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
                    "source",
                    models.ForeignKey(
                        blank=True,
                        null=True,
                        on_delete=django.db.models.deletion.SET_NULL,
                        related_name="execution_runs",
                        to="forward_netbox.forwardsource",
                    ),
                ),
                (
                    "sync",
                    models.ForeignKey(
                        on_delete=django.db.models.deletion.CASCADE,
                        related_name="execution_runs",
                        to="forward_netbox.forwardsync",
                    ),
                ),
                (
                    "validation_run",
                    models.ForeignKey(
                        blank=True,
                        null=True,
                        on_delete=django.db.models.deletion.SET_NULL,
                        related_name="execution_runs",
                        to="forward_netbox.forwardvalidationrun",
                    ),
                ),
            ],
            options={
                "verbose_name": "Forward Execution Run",
                "verbose_name_plural": "Forward Execution Runs",
                "db_table": "forward_netbox_execution_run",
                "ordering": ("-pk",),
            },
        ),
        migrations.CreateModel(
            name="ForwardExecutionStep",
            fields=[
                (
                    "id",
                    models.BigAutoField(
                        auto_created=True, primary_key=True, serialize=False
                    ),
                ),
                ("index", models.PositiveIntegerField()),
                ("kind", models.CharField(default="stage", max_length=30)),
                ("status", models.CharField(default="pending", max_length=30)),
                (
                    "model_string",
                    models.CharField(blank=True, default="", max_length=100),
                ),
                ("label", models.CharField(blank=True, default="", max_length=250)),
                (
                    "query_name",
                    models.CharField(blank=True, default="", max_length=200),
                ),
                (
                    "execution_mode",
                    models.CharField(blank=True, default="", max_length=30),
                ),
                (
                    "execution_value",
                    models.CharField(blank=True, default="", max_length=600),
                ),
                ("commit_id", models.CharField(blank=True, default="", max_length=100)),
                ("sync_mode", models.CharField(blank=True, default="", max_length=20)),
                (
                    "baseline_snapshot_id",
                    models.CharField(blank=True, default="", max_length=100),
                ),
                ("estimated_changes", models.PositiveIntegerField(default=0)),
                ("actual_changes", models.PositiveIntegerField(default=0)),
                ("shard_keys", models.JSONField(blank=True, default=list)),
                ("fetch_mode", models.CharField(blank=True, default="", max_length=30)),
                (
                    "branch_name",
                    models.CharField(blank=True, default="", max_length=255),
                ),
                ("retry_count", models.PositiveIntegerField(default=0)),
                ("last_error", models.TextField(blank=True, default="")),
                ("heartbeat", models.DateTimeField(blank=True, null=True)),
                ("started", models.DateTimeField(blank=True, null=True)),
                ("completed", models.DateTimeField(blank=True, null=True)),
                (
                    "created",
                    models.DateTimeField(
                        default=django.utils.timezone.now, editable=False
                    ),
                ),
                ("updated", models.DateTimeField(auto_now=True)),
                (
                    "branch",
                    models.ForeignKey(
                        blank=True,
                        null=True,
                        on_delete=django.db.models.deletion.SET_NULL,
                        to="netbox_branching.branch",
                    ),
                ),
                (
                    "ingestion",
                    models.ForeignKey(
                        blank=True,
                        null=True,
                        on_delete=django.db.models.deletion.SET_NULL,
                        related_name="execution_steps",
                        to="forward_netbox.forwardingestion",
                    ),
                ),
                (
                    "job",
                    models.ForeignKey(
                        blank=True,
                        null=True,
                        on_delete=django.db.models.deletion.SET_NULL,
                        related_name="forward_execution_steps",
                        to="core.job",
                    ),
                ),
                (
                    "merge_job",
                    models.ForeignKey(
                        blank=True,
                        null=True,
                        on_delete=django.db.models.deletion.SET_NULL,
                        related_name="forward_execution_merge_steps",
                        to="core.job",
                    ),
                ),
                (
                    "run",
                    models.ForeignKey(
                        on_delete=django.db.models.deletion.CASCADE,
                        related_name="steps",
                        to="forward_netbox.forwardexecutionrun",
                    ),
                ),
            ],
            options={
                "verbose_name": "Forward Execution Step",
                "verbose_name_plural": "Forward Execution Steps",
                "db_table": "forward_netbox_execution_step",
                "ordering": ("run", "index", "kind"),
                "unique_together": {("run", "index", "kind")},
            },
        ),
    ]
