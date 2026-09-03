# Change control: seven tables for the Forward-backed change gate.
#
# Two things in here are deliberate and worth not "tidying" later.
#
# ForwardChange carries four advisory columns - predict_status,
# predict_reason, predict_pre_verdict, netbox_impact - while Forward's predict
# workflow is not live. They ship now so its arrival is additive: filling in
# the stub must not require a schema change. They are also quarantined from
# the verify gate by construction, because the gate reads
# ForwardChangeEvidence and nothing else. Making a prediction into a verdict
# would mean moving a column into that table, which is a visible act rather
# than a quiet drift.
#
# ForwardChangeEvidence is unique on (criterion, phase, snapshot_id) rather
# than (criterion, phase). Evidence belongs to a PINNED snapshot; re-running a
# criterion against a different snapshot is a new row, not an overwrite, so
# the before/after comparison a verdict rests on cannot be silently rebased.
import django.db.models.deletion
import django.utils.timezone
import netbox.models.deletion
import taggit.managers
import utilities.json
from django.conf import settings
from django.db import migrations
from django.db import models

import forward_netbox.models


class Migration(migrations.Migration):

    dependencies = [
        # `0001_initial`, not the migration makemigrations picked. These FKs
        # only need the dcim/extras models to EXIST, and naming a recent
        # migration pins a NetBox version the plugin does not actually require
        # - which `test_migration_dependencies` refuses for exactly that
        # reason.
        ("dcim", "0001_initial"),
        ("extras", "0001_initial"),
        ("forward_netbox", "0053_alter_forwardsource_owner"),
        ("netbox_branching", "0009_changediff_last_updated_auto_now"),
        ("users", "0016_default_ordering_indexes"),
        migrations.swappable_dependency(settings.AUTH_USER_MODEL),
    ]

    operations = [
        migrations.CreateModel(
            name="ForwardChange",
            fields=[
                (
                    "id",
                    models.BigAutoField(
                        auto_created=True, primary_key=True, serialize=False
                    ),
                ),
                ("created", models.DateTimeField(auto_now_add=True, null=True)),
                ("last_updated", models.DateTimeField(auto_now=True, null=True)),
                (
                    "custom_field_data",
                    models.JSONField(
                        blank=True,
                        default=dict,
                        encoder=utilities.json.CustomFieldJSONEncoder,
                    ),
                ),
                ("description", models.CharField(blank=True, max_length=200)),
                ("comments", models.TextField(blank=True)),
                ("ref", models.CharField(blank=True, default="", max_length=100)),
                ("title", models.CharField(max_length=200)),
                ("state", models.CharField(default="draft", max_length=32)),
                ("verdict", models.CharField(blank=True, default="", max_length=16)),
                (
                    "branch_name",
                    models.CharField(blank=True, default="", max_length=200),
                ),
                (
                    "branch_last_change_time",
                    models.DateTimeField(blank=True, null=True),
                ),
                (
                    "before_snapshot_id",
                    models.CharField(blank=True, default="", max_length=100),
                ),
                (
                    "after_snapshot_id",
                    models.CharField(blank=True, default="", max_length=100),
                ),
                ("window_start", models.DateTimeField(blank=True, null=True)),
                ("window_end", models.DateTimeField(blank=True, null=True)),
                ("applied_at", models.DateTimeField(blank=True, null=True)),
                (
                    "applied_ref",
                    models.CharField(blank=True, default="", max_length=200),
                ),
                (
                    "predict_status",
                    models.CharField(blank=True, default="", max_length=32),
                ),
                ("predict_reason", models.TextField(blank=True, default="")),
                (
                    "predict_pre_verdict",
                    models.CharField(blank=True, default="", max_length=32),
                ),
                ("netbox_impact", models.JSONField(blank=True, default=dict)),
                (
                    "applied_by",
                    models.ForeignKey(
                        blank=True,
                        null=True,
                        on_delete=django.db.models.deletion.SET_NULL,
                        related_name="+",
                        to=settings.AUTH_USER_MODEL,
                    ),
                ),
                (
                    "branch",
                    models.OneToOneField(
                        blank=True,
                        null=True,
                        on_delete=django.db.models.deletion.SET_NULL,
                        related_name="+",
                        to="netbox_branching.branch",
                    ),
                ),
                (
                    "owner",
                    models.ForeignKey(
                        blank=True,
                        null=True,
                        on_delete=django.db.models.deletion.PROTECT,
                        related_name="+",
                        to="users.owner",
                    ),
                ),
                (
                    "requester",
                    models.ForeignKey(
                        blank=True,
                        null=True,
                        on_delete=django.db.models.deletion.SET_NULL,
                        related_name="+",
                        to=settings.AUTH_USER_MODEL,
                    ),
                ),
                (
                    "source",
                    models.ForeignKey(
                        on_delete=django.db.models.deletion.PROTECT,
                        related_name="changes",
                        to="forward_netbox.forwardsource",
                    ),
                ),
                (
                    "tags",
                    taggit.managers.TaggableManager(
                        through="extras.TaggedItem", to="extras.Tag"
                    ),
                ),
            ],
            options={
                "verbose_name": "Forward Change",
                "verbose_name_plural": "Forward Changes",
                "db_table": "forward_netbox_change",
                "ordering": ("-created",),
            },
            bases=(
                forward_netbox.models.ForwardPluginModelDocsMixin,
                netbox.models.deletion.DeleteMixin,
                models.Model,
            ),
        ),
        migrations.CreateModel(
            name="ForwardChangeCriterion",
            fields=[
                (
                    "id",
                    models.BigAutoField(
                        auto_created=True, primary_key=True, serialize=False
                    ),
                ),
                ("created", models.DateTimeField(auto_now_add=True, null=True)),
                ("last_updated", models.DateTimeField(auto_now=True, null=True)),
                ("name", models.CharField(max_length=200)),
                ("family", models.CharField(default="acceptance", max_length=32)),
                ("expectation", models.CharField(default="no-rows", max_length=32)),
                ("blocking", models.BooleanField(default=True)),
                (
                    "query_path",
                    models.CharField(blank=True, default="", max_length=500),
                ),
                ("query_id", models.CharField(blank=True, default="", max_length=100)),
                ("commit_id", models.CharField(blank=True, default="", max_length=100)),
                (
                    "source_sha256",
                    models.CharField(blank=True, default="", max_length=64),
                ),
                (
                    "change",
                    models.ForeignKey(
                        on_delete=django.db.models.deletion.CASCADE,
                        related_name="criteria",
                        to="forward_netbox.forwardchange",
                    ),
                ),
            ],
            options={
                "verbose_name": "Forward Change Criterion",
                "verbose_name_plural": "Forward Change Criteria",
                "db_table": "forward_netbox_change_criterion",
                "ordering": ("change", "name"),
            },
            bases=(
                forward_netbox.models.ForwardPluginModelDocsMixin,
                netbox.models.deletion.DeleteMixin,
                models.Model,
            ),
        ),
        migrations.CreateModel(
            name="ForwardChangeDevice",
            fields=[
                (
                    "id",
                    models.BigAutoField(
                        auto_created=True, primary_key=True, serialize=False
                    ),
                ),
                (
                    "forward_device_key",
                    models.CharField(blank=True, default="", max_length=255),
                ),
                (
                    "change",
                    models.ForeignKey(
                        on_delete=django.db.models.deletion.CASCADE,
                        related_name="devices",
                        to="forward_netbox.forwardchange",
                    ),
                ),
                (
                    "device",
                    models.ForeignKey(
                        on_delete=django.db.models.deletion.CASCADE,
                        related_name="+",
                        to="dcim.device",
                    ),
                ),
            ],
            options={
                "verbose_name": "Forward Change Device",
                "verbose_name_plural": "Forward Change Devices",
                "db_table": "forward_netbox_change_device",
                "ordering": ("device__name",),
            },
        ),
        migrations.CreateModel(
            name="ForwardChangeEvidence",
            fields=[
                (
                    "id",
                    models.BigAutoField(
                        auto_created=True, primary_key=True, serialize=False
                    ),
                ),
                ("phase", models.CharField(max_length=16)),
                ("passed", models.BooleanField()),
                ("row_count", models.PositiveIntegerField(default=0)),
                ("snapshot_id", models.CharField(max_length=100)),
                ("commit_id", models.CharField(blank=True, default="", max_length=100)),
                (
                    "executed_at",
                    models.DateTimeField(default=django.utils.timezone.now),
                ),
                ("duration_ms", models.PositiveIntegerField(default=0)),
                ("shape", models.JSONField(blank=True, default=dict)),
                (
                    "criterion",
                    models.ForeignKey(
                        on_delete=django.db.models.deletion.CASCADE,
                        related_name="evidence",
                        to="forward_netbox.forwardchangecriterion",
                    ),
                ),
            ],
            options={
                "verbose_name": "Forward Change Evidence",
                "verbose_name_plural": "Forward Change Evidence",
                "db_table": "forward_netbox_change_evidence",
                "ordering": ("criterion", "phase"),
            },
        ),
        migrations.CreateModel(
            name="ForwardChangePolicy",
            fields=[
                (
                    "id",
                    models.BigAutoField(
                        auto_created=True, primary_key=True, serialize=False
                    ),
                ),
                ("created", models.DateTimeField(auto_now_add=True, null=True)),
                ("last_updated", models.DateTimeField(auto_now=True, null=True)),
                (
                    "custom_field_data",
                    models.JSONField(
                        blank=True,
                        default=dict,
                        encoder=utilities.json.CustomFieldJSONEncoder,
                    ),
                ),
                ("description", models.CharField(blank=True, max_length=200)),
                ("comments", models.TextField(blank=True)),
                ("name", models.CharField(max_length=100, unique=True)),
                ("enabled", models.BooleanField(default=True)),
                ("min_approvals", models.PositiveSmallIntegerField(default=1)),
                (
                    "owner",
                    models.ForeignKey(
                        blank=True,
                        null=True,
                        on_delete=django.db.models.deletion.PROTECT,
                        related_name="+",
                        to="users.owner",
                    ),
                ),
                (
                    "tags",
                    taggit.managers.TaggableManager(
                        through="extras.TaggedItem", to="extras.Tag"
                    ),
                ),
            ],
            options={
                "verbose_name": "Forward Change Policy",
                "verbose_name_plural": "Forward Change Policies",
                "db_table": "forward_netbox_change_policy",
                "ordering": ("name",),
            },
            bases=(
                forward_netbox.models.ForwardPluginModelDocsMixin,
                netbox.models.deletion.DeleteMixin,
                models.Model,
            ),
        ),
        migrations.CreateModel(
            name="ForwardChangePolicyRule",
            fields=[
                (
                    "id",
                    models.BigAutoField(
                        auto_created=True, primary_key=True, serialize=False
                    ),
                ),
                ("created", models.DateTimeField(auto_now_add=True, null=True)),
                ("last_updated", models.DateTimeField(auto_now=True, null=True)),
                ("tag_slug", models.CharField(blank=True, default="", max_length=100)),
                (
                    "device_role",
                    models.ForeignKey(
                        blank=True,
                        null=True,
                        on_delete=django.db.models.deletion.CASCADE,
                        related_name="+",
                        to="dcim.devicerole",
                    ),
                ),
                (
                    "policy",
                    models.ForeignKey(
                        on_delete=django.db.models.deletion.CASCADE,
                        related_name="rules",
                        to="forward_netbox.forwardchangepolicy",
                    ),
                ),
                (
                    "site",
                    models.ForeignKey(
                        blank=True,
                        null=True,
                        on_delete=django.db.models.deletion.CASCADE,
                        related_name="+",
                        to="dcim.site",
                    ),
                ),
            ],
            options={
                "verbose_name": "Forward Change Policy Rule",
                "verbose_name_plural": "Forward Change Policy Rules",
                "db_table": "forward_netbox_change_policy_rule",
                "ordering": ("policy", "pk"),
            },
            bases=(netbox.models.deletion.DeleteMixin, models.Model),
        ),
        migrations.CreateModel(
            name="ForwardChangeReview",
            fields=[
                (
                    "id",
                    models.BigAutoField(
                        auto_created=True, primary_key=True, serialize=False
                    ),
                ),
                ("created", models.DateTimeField(auto_now_add=True, null=True)),
                ("last_updated", models.DateTimeField(auto_now=True, null=True)),
                ("approved", models.BooleanField(default=False)),
                ("comment", models.TextField(blank=True, default="")),
                ("branch_change_time", models.DateTimeField(blank=True, null=True)),
                (
                    "baseline_snapshot_id",
                    models.CharField(blank=True, default="", max_length=100),
                ),
                (
                    "change",
                    models.ForeignKey(
                        on_delete=django.db.models.deletion.CASCADE,
                        related_name="reviews",
                        to="forward_netbox.forwardchange",
                    ),
                ),
                (
                    "reviewer",
                    models.ForeignKey(
                        blank=True,
                        null=True,
                        on_delete=django.db.models.deletion.SET_NULL,
                        related_name="+",
                        to=settings.AUTH_USER_MODEL,
                    ),
                ),
            ],
            options={
                "verbose_name": "Forward Change Review",
                "verbose_name_plural": "Forward Change Reviews",
                "db_table": "forward_netbox_change_review",
                "ordering": ("-created",),
            },
            bases=(
                forward_netbox.models.ForwardPluginModelDocsMixin,
                netbox.models.deletion.DeleteMixin,
                models.Model,
            ),
        ),
        migrations.AddConstraint(
            model_name="forwardchangecriterion",
            constraint=models.UniqueConstraint(
                fields=("change", "name"), name="forward_change_criterion_unique_name"
            ),
        ),
        migrations.AddConstraint(
            model_name="forwardchangedevice",
            constraint=models.UniqueConstraint(
                fields=("change", "device"), name="forward_change_device_unique"
            ),
        ),
        migrations.AddConstraint(
            model_name="forwardchangeevidence",
            constraint=models.UniqueConstraint(
                fields=("criterion", "phase", "snapshot_id"),
                name="forward_change_evidence_unique",
            ),
        ),
    ]
