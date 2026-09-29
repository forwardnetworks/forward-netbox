from django.db import migrations

# Join the 2.9.x lane's migration history to this one.
#
# Both chains branch from `0052`. A database from either lane has one of them
# applied and the other not; the other then runs - on a 2.9.x database this
# lane's `0053`-`0059`, on a 3.0.0 database the 2.9.x-named files, of which only
# `0057_nqe_map_last_live_drift` changes anything - and both end here. A 2.9.x
# migration added after this point needs the same treatment: a file of the same
# name on this lane, and this merge (or a later one) depending on it.


class Migration(migrations.Migration):
    dependencies = [
        ("forward_netbox", "0057_nqe_map_last_live_drift"),
        ("forward_netbox", "0059_aci_attachment_nqe_map_choices"),
    ]

    operations = []
