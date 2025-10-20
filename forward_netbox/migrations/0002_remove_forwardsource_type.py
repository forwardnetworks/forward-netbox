from django.db import migrations


class Migration(migrations.Migration):

    dependencies = [
        ("forward_netbox", "0001_initial"),
    ]

    operations = [
        migrations.RemoveField(
            model_name="forwardsource",
            name="type",
        ),
    ]
