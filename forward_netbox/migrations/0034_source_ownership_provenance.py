import django.db.models.deletion
import django.utils.timezone
from django.db import migrations
from django.db import models


# Ownership is a main-schema control plane. Branch schemas must never receive
# independent claim rows or sequences.
fake_on_branch = True


class Migration(migrations.Migration):
    dependencies = [
        ("dcim", "0166_virtualdevicecontext"),
        ("extras", "0001_initial"),
        ("forward_netbox", "0033_alter_forwardnqemap_netbox_model"),
    ]

    operations = [
        migrations.CreateModel(
            name="ForwardManagedDeviceTag",
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
                (
                    "claim_type",
                    models.CharField(
                        choices=[
                            ("scope", "Managed scope"),
                            ("backfilled", "Backfilled status"),
                            ("out_of_scope", "Out-of-scope status"),
                        ],
                        max_length=32,
                    ),
                ),
                (
                    "tag",
                    models.ForeignKey(
                        on_delete=django.db.models.deletion.CASCADE,
                        related_name="+",
                        to="extras.tag",
                    ),
                ),
            ],
            options={
                "verbose_name": "Forward Managed Device Tag",
                "verbose_name_plural": "Forward Managed Device Tags",
                "db_table": "forward_netbox_managed_device_tag",
                "ordering": ("tag__name", "claim_type"),
            },
        ),
        migrations.CreateModel(
            name="ForwardManagedVirtualContext",
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
                (
                    "virtual_context",
                    models.OneToOneField(
                        on_delete=django.db.models.deletion.CASCADE,
                        related_name="+",
                        to="dcim.virtualdevicecontext",
                    ),
                ),
            ],
            options={
                "verbose_name": "Forward Managed Virtual Context",
                "verbose_name_plural": "Forward Managed Virtual Contexts",
                "db_table": "forward_netbox_managed_virtual_context",
                "ordering": (
                    "virtual_context__device__name",
                    "virtual_context__name",
                ),
            },
        ),
        migrations.CreateModel(
            name="ForwardOwnershipReconciliation",
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
                (
                    "domain",
                    models.CharField(
                        choices=[
                            ("scope_tags", "Managed scope tags"),
                            ("status_tags", "Scope status tags"),
                            ("virtual_parents", "Virtual parents"),
                        ],
                        max_length=32,
                    ),
                ),
                (
                    "ingestion",
                    models.ForeignKey(
                        db_column="generation",
                        on_delete=django.db.models.deletion.PROTECT,
                        related_name="+",
                        to="forward_netbox.forwardingestion",
                    ),
                ),
                (
                    "snapshot_id",
                    models.CharField(blank=True, default="", max_length=100),
                ),
                (
                    "status",
                    models.CharField(
                        choices=[
                            ("pending", "Pending"),
                            ("completed", "Completed"),
                            ("failed", "Failed"),
                        ],
                        default="pending",
                        max_length=16,
                    ),
                ),
                (
                    "error_type",
                    models.CharField(blank=True, default="", max_length=100),
                ),
                ("started_at", models.DateTimeField(default=django.utils.timezone.now)),
                ("completed_at", models.DateTimeField(blank=True, null=True)),
                (
                    "sync",
                    models.ForeignKey(
                        on_delete=django.db.models.deletion.PROTECT,
                        related_name="ownership_reconciliations",
                        to="forward_netbox.forwardsync",
                    ),
                ),
            ],
            options={
                "verbose_name": "Forward Ownership Reconciliation",
                "verbose_name_plural": "Forward Ownership Reconciliations",
                "db_table": "forward_netbox_ownership_reconciliation",
                "ordering": ("sync__name", "domain"),
            },
        ),
        migrations.CreateModel(
            name="ForwardDeviceTagClaim",
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
                (
                    "claim_type",
                    models.CharField(
                        choices=[
                            ("scope", "Managed scope"),
                            ("backfilled", "Backfilled status"),
                            ("out_of_scope", "Out-of-scope status"),
                        ],
                        max_length=32,
                    ),
                ),
                (
                    "ingestion",
                    models.ForeignKey(
                        db_column="generation",
                        on_delete=django.db.models.deletion.PROTECT,
                        related_name="+",
                        to="forward_netbox.forwardingestion",
                    ),
                ),
                (
                    "snapshot_id",
                    models.CharField(blank=True, default="", max_length=100),
                ),
                (
                    "device",
                    models.ForeignKey(
                        on_delete=django.db.models.deletion.CASCADE,
                        related_name="+",
                        to="dcim.device",
                    ),
                ),
                (
                    "sync",
                    models.ForeignKey(
                        on_delete=django.db.models.deletion.PROTECT,
                        related_name="device_tag_claims",
                        to="forward_netbox.forwardsync",
                    ),
                ),
                (
                    "tag",
                    models.ForeignKey(
                        on_delete=django.db.models.deletion.CASCADE,
                        related_name="+",
                        to="extras.tag",
                    ),
                ),
            ],
            options={
                "verbose_name": "Forward Device Tag Claim",
                "verbose_name_plural": "Forward Device Tag Claims",
                "db_table": "forward_netbox_device_tag_claim",
                "ordering": ("sync__name", "device__name", "tag__name", "claim_type"),
            },
        ),
        migrations.CreateModel(
            name="ForwardVirtualParentClaim",
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
                (
                    "ingestion",
                    models.ForeignKey(
                        db_column="generation",
                        on_delete=django.db.models.deletion.PROTECT,
                        related_name="+",
                        to="forward_netbox.forwardingestion",
                    ),
                ),
                (
                    "snapshot_id",
                    models.CharField(blank=True, default="", max_length=100),
                ),
                (
                    "device",
                    models.ForeignKey(
                        on_delete=django.db.models.deletion.CASCADE,
                        related_name="+",
                        to="dcim.device",
                    ),
                ),
                (
                    "parent_device",
                    models.ForeignKey(
                        on_delete=django.db.models.deletion.CASCADE,
                        related_name="+",
                        to="dcim.device",
                    ),
                ),
                (
                    "sync",
                    models.ForeignKey(
                        on_delete=django.db.models.deletion.PROTECT,
                        related_name="virtual_parent_claims",
                        to="forward_netbox.forwardsync",
                    ),
                ),
                (
                    "virtual_context",
                    models.ForeignKey(
                        blank=True,
                        null=True,
                        on_delete=django.db.models.deletion.SET_NULL,
                        related_name="+",
                        to="dcim.virtualdevicecontext",
                    ),
                ),
            ],
            options={
                "verbose_name": "Forward Virtual Parent Claim",
                "verbose_name_plural": "Forward Virtual Parent Claims",
                "db_table": "forward_netbox_virtual_parent_claim",
                "ordering": ("sync__name", "device__name"),
            },
        ),
        migrations.AddConstraint(
            model_name="forwardmanageddevicetag",
            constraint=models.UniqueConstraint(
                fields=("tag", "claim_type"),
                name="forward_managed_device_tag_identity",
            ),
        ),
        migrations.AddConstraint(
            model_name="forwardownershipreconciliation",
            constraint=models.UniqueConstraint(
                fields=("sync", "domain"),
                name="forward_ownership_reconciliation_identity",
            ),
        ),
        migrations.AddConstraint(
            model_name="forwarddevicetagclaim",
            constraint=models.UniqueConstraint(
                fields=("sync", "device", "tag", "claim_type"),
                name="forward_device_tag_claim_identity",
            ),
        ),
        migrations.AddConstraint(
            model_name="forwardvirtualparentclaim",
            constraint=models.UniqueConstraint(
                fields=("sync", "device"),
                name="forward_virtual_parent_claim_identity",
            ),
        ),
    ]
