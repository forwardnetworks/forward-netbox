from django.db import migrations, models


def add_network_id_column(apps, schema_editor):
    schema_editor.execute(
        "ALTER TABLE forward_netbox_forwardsource ADD COLUMN IF NOT EXISTS network_id varchar(100);"
    )


def drop_network_id_column(apps, schema_editor):
    schema_editor.execute(
        "ALTER TABLE forward_netbox_forwardsource DROP COLUMN IF EXISTS network_id;"
    )


class Migration(migrations.Migration):

    dependencies = [
        ("forward_netbox", "0003_forwardnqequery"),
    ]

    operations = [
        migrations.RunPython(add_network_id_column, drop_network_id_column),
        migrations.AlterField(
            model_name="forwardsource",
            name="network_id",
            field=models.CharField(
                max_length=100,
                blank=True,
                null=True,
                verbose_name="Network ID",
                help_text="Optional Forward Networks network identifier used for API scoping.",
            ),
        ),
    ]
