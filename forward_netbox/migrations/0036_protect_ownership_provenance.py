import django.db.models.deletion
from django.db import migrations
from django.db import models


# Ownership is a main-schema control plane. Branch schemas must never receive
# independent claim constraints or foreign keys.
fake_on_branch = True


class Migration(migrations.Migration):
    dependencies = [
        ("forward_netbox", "0035_initialize_ownership_control_plane"),
    ]

    operations = [
        migrations.AlterField(
            model_name="forwardmanageddevicetag",
            name="tag",
            field=models.ForeignKey(
                on_delete=django.db.models.deletion.PROTECT,
                related_name="+",
                to="extras.tag",
            ),
        ),
        migrations.AlterField(
            model_name="forwarddevicetagclaim",
            name="device",
            field=models.ForeignKey(
                on_delete=django.db.models.deletion.PROTECT,
                related_name="+",
                to="dcim.device",
            ),
        ),
        migrations.AlterField(
            model_name="forwarddevicetagclaim",
            name="tag",
            field=models.ForeignKey(
                on_delete=django.db.models.deletion.PROTECT,
                related_name="+",
                to="extras.tag",
            ),
        ),
        migrations.AlterField(
            model_name="forwardmanagedvirtualcontext",
            name="virtual_context",
            field=models.OneToOneField(
                on_delete=django.db.models.deletion.PROTECT,
                related_name="+",
                to="dcim.virtualdevicecontext",
            ),
        ),
        migrations.AlterField(
            model_name="forwardvirtualparentclaim",
            name="device",
            field=models.ForeignKey(
                on_delete=django.db.models.deletion.PROTECT,
                related_name="+",
                to="dcim.device",
            ),
        ),
        migrations.AlterField(
            model_name="forwardvirtualparentclaim",
            name="parent_device",
            field=models.ForeignKey(
                on_delete=django.db.models.deletion.PROTECT,
                related_name="+",
                to="dcim.device",
            ),
        ),
        migrations.AlterField(
            model_name="forwardvirtualparentclaim",
            name="virtual_context",
            field=models.ForeignKey(
                blank=True,
                null=True,
                on_delete=django.db.models.deletion.PROTECT,
                related_name="+",
                to="dcim.virtualdevicecontext",
            ),
        ),
    ]
