from django.db import migrations
from django.db import models

# The 2.9.x lane's `0057_nqe_map_last_live_drift`, adopted by name.
#
# The one 2.9.x migration with no equivalent on this lane: 2.9.8 added the live
# query-drift result to each NQE map. A 2.9.x database already has these columns
# and records this name as applied, so it is skipped there; a 3.0.0 database has
# neither and gets them here.


class Migration(migrations.Migration):
    dependencies = [
        ("forward_netbox", "0056_aci_attachment_nqe_map_choices"),
    ]

    operations = [
        migrations.AddField(
            model_name="forwardnqemap",
            name="last_live_drift",
            field=models.JSONField(blank=True, default=dict),
        ),
        migrations.AddField(
            model_name="forwardnqemap",
            name="last_live_drift_at",
            field=models.DateTimeField(blank=True, null=True),
        ),
    ]
