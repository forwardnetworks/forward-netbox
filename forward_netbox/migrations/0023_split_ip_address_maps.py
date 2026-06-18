from django.db import migrations

# The unified "Forward IP Addresses" built-in map emitted both IPv4 and IPv6
# ipam.ipaddress rows, so operators could not disable IPv6 addresses
# independently. It is replaced by two built-in maps, "Forward IPv4 IP Addresses"
# and "Forward IPv6 IP Addresses", which post_migrate seeds. This migration
# removes the old built-in row on upgrade so addresses are not double-collected.
#
# Only the built_in=True row is deleted. On built-in maps any query_id /
# query_path is just the plugin's own published binding to a Forward org
# repository (set by publish_builtin_nqe_map_queries), not user-authored logic,
# so it is safe to remove. A user with custom IP-address logic uses a
# built_in=False map, which takes precedence and is left untouched here.
# Org-mode deployments should re-publish the two new built-in maps to their
# Forward org repository after upgrading.
OLD_MAP_NAME = "Forward IP Addresses"


def remove_unified_ip_address_map(apps, schema_editor):
    ForwardNQEMap = apps.get_model("forward_netbox", "ForwardNQEMap")
    ForwardNQEMap.objects.filter(built_in=True, name=OLD_MAP_NAME).delete()


def noop_reverse(apps, schema_editor):
    # The split maps are reseeded by post_migrate; the unified map is no longer
    # part of the registry, so there is nothing to restore.
    pass


class Migration(migrations.Migration):

    dependencies = [
        ("forward_netbox", "0022_alter_forwardnqemap_netbox_model"),
    ]

    operations = [
        migrations.RunPython(remove_unified_ip_address_map, noop_reverse),
    ]
