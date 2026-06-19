import django.db.models.deletion
import netbox.models.deletion
from django.db import migrations
from django.db import models


class Migration(migrations.Migration):

    dependencies = [
        ("dcim", "0001_initial"),
        ("forward_netbox", "0024_forwarddeviceanalysis"),
    ]

    operations = [
        # The 1.6.0 read-only analysis table is empty in every deployment (it
        # shipped that release and is repopulated by the refresh job), so it is
        # recreated cleanly as a ChangeLoggedModel with a real device FK.
        migrations.DeleteModel(name="ForwardDeviceAnalysis"),
        migrations.CreateModel(
            name="ForwardDeviceAnalysis",
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
                ("created", models.DateTimeField(auto_now_add=True, null=True)),
                ("last_updated", models.DateTimeField(auto_now=True, null=True)),
                ("reachable", models.BooleanField(default=False)),
                ("blast_radius", models.PositiveIntegerField(default=0)),
                ("cve_count", models.PositiveIntegerField(default=0)),
                ("up_interfaces", models.PositiveIntegerField(default=0)),
                ("detail", models.CharField(blank=True, default="", max_length=255)),
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
                        on_delete=django.db.models.deletion.CASCADE,
                        related_name="device_analyses",
                        to="forward_netbox.forwardsync",
                    ),
                ),
            ],
            options={
                "verbose_name": "Forward Device Analysis",
                "verbose_name_plural": "Forward Device Analyses",
                "ordering": ("device__name",),
                "db_table": "forward_netbox_device_analysis",
            },
            bases=(netbox.models.deletion.DeleteMixin, models.Model),
        ),
        migrations.AddConstraint(
            model_name="forwarddeviceanalysis",
            constraint=models.UniqueConstraint(
                fields=("sync", "device"),
                name="forward_device_analysis_sync_device",
            ),
        ),
    ]
