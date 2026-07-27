import django.db.models.deletion
import django.utils.timezone
from django.db import migrations
from django.db import models

fake_on_branch = True


class Migration(migrations.Migration):
    dependencies = [
        ("forward_netbox", "0043_forwardnqemap_diff_contract"),
    ]

    operations = [
        migrations.CreateModel(
            name="ForwardContributorBaseline",
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
                ("snapshot_id", models.CharField(max_length=100)),
                ("network_fingerprint", models.CharField(max_length=64)),
                ("map_set_fingerprint", models.CharField(max_length=64)),
                ("scope_config_fingerprint", models.CharField(max_length=64)),
                ("scope_membership_fingerprint", models.CharField(max_length=64)),
                ("scope_payload_version", models.PositiveSmallIntegerField(default=1)),
                ("scope_payload", models.BinaryField(blank=True, default=b"")),
                ("scope_payload_checksum", models.CharField(max_length=64)),
                (
                    "status",
                    models.CharField(
                        choices=[
                            ("pending", "Pending"),
                            ("current", "Current"),
                            ("superseded", "Superseded"),
                            ("invalid", "Invalid"),
                        ],
                        db_index=True,
                        default="pending",
                        max_length=20,
                    ),
                ),
                ("is_current", models.BooleanField(db_index=True, default=False)),
                (
                    "created",
                    models.DateTimeField(
                        default=django.utils.timezone.now,
                        editable=False,
                    ),
                ),
                ("promoted_at", models.DateTimeField(blank=True, null=True)),
                (
                    "ingestion",
                    models.OneToOneField(
                        on_delete=django.db.models.deletion.PROTECT,
                        related_name="contributor_baseline",
                        to="forward_netbox.forwardingestion",
                    ),
                ),
                (
                    "parent_baseline",
                    models.ForeignKey(
                        blank=True,
                        null=True,
                        on_delete=django.db.models.deletion.SET_NULL,
                        related_name="child_baselines",
                        to="forward_netbox.forwardcontributorbaseline",
                    ),
                ),
                (
                    "sync",
                    models.ForeignKey(
                        on_delete=django.db.models.deletion.CASCADE,
                        related_name="contributor_baselines",
                        to="forward_netbox.forwardsync",
                    ),
                ),
            ],
            options={
                "verbose_name": "Forward Contributor Baseline",
                "verbose_name_plural": "Forward Contributor Baselines",
                "db_table": "forward_netbox_contributor_baseline",
                "ordering": ("sync_id", "-ingestion_id"),
            },
        ),
        migrations.CreateModel(
            name="ForwardContributorRelation",
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
                ("contract_key", models.CharField(max_length=255)),
                ("query_path", models.CharField(max_length=500)),
                ("query_id", models.CharField(max_length=100)),
                ("full_commit_id", models.CharField(max_length=100)),
                ("full_source_sha256", models.CharField(max_length=64)),
                ("diff_query_id", models.CharField(max_length=100)),
                ("diff_commit_id", models.CharField(max_length=100)),
                ("diff_source_sha256", models.CharField(max_length=64)),
                ("contract_fingerprint", models.CharField(max_length=64)),
                ("reducer_id", models.CharField(max_length=100)),
                ("reducer_version", models.PositiveIntegerField()),
                ("normalization_version", models.PositiveIntegerField()),
                ("identity_version", models.PositiveIntegerField()),
                ("provenance_identity_version", models.PositiveIntegerField(default=1)),
                ("payload_version", models.PositiveIntegerField(default=1)),
                ("row_count", models.PositiveIntegerField(default=0)),
                ("uncompressed_bytes", models.PositiveBigIntegerField(default=0)),
                ("compressed_bytes", models.PositiveBigIntegerField(default=0)),
                ("relation_checksum", models.CharField(max_length=64)),
                (
                    "baseline",
                    models.ForeignKey(
                        on_delete=django.db.models.deletion.CASCADE,
                        related_name="relations",
                        to="forward_netbox.forwardcontributorbaseline",
                    ),
                ),
                (
                    "query_map",
                    models.ForeignKey(
                        blank=True,
                        null=True,
                        on_delete=django.db.models.deletion.SET_NULL,
                        related_name="+",
                        to="forward_netbox.forwardnqemap",
                    ),
                ),
            ],
            options={
                "verbose_name": "Forward Contributor Relation",
                "verbose_name_plural": "Forward Contributor Relations",
                "db_table": "forward_netbox_contributor_relation",
                "ordering": ("baseline_id", "contract_key"),
            },
        ),
        migrations.CreateModel(
            name="ForwardContributorRelationChunk",
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
                ("sequence", models.PositiveIntegerField()),
                ("payload", models.BinaryField()),
                ("payload_checksum", models.CharField(max_length=64)),
                ("compressed_bytes", models.PositiveIntegerField()),
                (
                    "relation",
                    models.ForeignKey(
                        on_delete=django.db.models.deletion.CASCADE,
                        related_name="chunks",
                        to="forward_netbox.forwardcontributorrelation",
                    ),
                ),
            ],
            options={
                "verbose_name": "Forward Contributor Relation Chunk",
                "verbose_name_plural": "Forward Contributor Relation Chunks",
                "db_table": "forward_netbox_contributor_relation_chunk",
                "ordering": ("relation_id", "sequence"),
            },
        ),
        migrations.AddConstraint(
            model_name="forwardcontributorbaseline",
            constraint=models.UniqueConstraint(
                condition=models.Q(("is_current", True)),
                fields=("sync",),
                name="forward_contributor_baseline_current_sync",
            ),
        ),
        migrations.AddConstraint(
            model_name="forwardcontributorrelation",
            constraint=models.UniqueConstraint(
                fields=("baseline", "contract_key"),
                name="forward_contributor_relation_baseline_contract",
            ),
        ),
        migrations.AddConstraint(
            model_name="forwardcontributorrelationchunk",
            constraint=models.UniqueConstraint(
                fields=("relation", "sequence"),
                name="forward_contributor_chunk_relation_sequence",
            ),
        ),
    ]
