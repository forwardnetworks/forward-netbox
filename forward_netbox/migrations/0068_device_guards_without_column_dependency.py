from django.db import migrations

fake_on_branch = True


def rebuild(apps, schema_editor):
    from forward_netbox.utilities.device_guard_triggers import (
        rebuild_column_bound_guards,
    )

    rebuild_column_bound_guards(using=schema_editor.connection.alias)


class Migration(migrations.Migration):
    """Rewrite the device guard triggers so they no longer pin their columns.

    NetBox 4.7.4 widens `extras_taggeditem.object_id`, which a trigger declared
    `UPDATE OF ... object_id` blocks. See `utilities.device_guard_triggers`.
    """

    dependencies = [
        ("forward_netbox", "0067_change_proposal"),
    ]

    operations = [
        migrations.RunPython(rebuild, migrations.RunPython.noop),
    ]
