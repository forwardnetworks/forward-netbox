from django.db import migrations

# The 2.9.x lane's `0054_aci_tenant_policy_nqe_map_choices`, adopted by name.
#
# `main` and `maint/2.9.x` diverged after `0052`, each numbering its own
# migrations. A database migrated on 2.9.x records this name as applied; without
# a file of the same name here, upgrading it to 3.x would not recognise the
# history it already has. The change it made is made on this lane by
# `0057_aci_tenant_policy_nqe_map_choices`, so here it changes nothing - it only anchors the name.


class Migration(migrations.Migration):
    dependencies = [
        ("contenttypes", "0002_remove_content_type_name"),
        ("forward_netbox", "0053_routing_policy_nqe_map_choices"),
    ]

    operations = []
