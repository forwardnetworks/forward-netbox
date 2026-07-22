from django.db import migrations


fake_on_branch = True

GUARD_FUNCTION = "forward_netbox_enforce_device_generic_relation"
GUARD_TRIGGER = "forward_netbox_device_gfk_guard"
GUARDS = (
    ("ipam_service", "parent_object_type_id", "parent_object_id"),
    ("tenancy_contactassignment", "object_type_id", "object_id"),
    ("extras_imageattachment", "object_type_id", "object_id"),
    ("extras_bookmark", "object_type_id", "object_id"),
    ("extras_journalentry", "assigned_object_type_id", "assigned_object_id"),
    ("extras_subscription", "object_type_id", "object_id"),
    ("extras_taggeditem", "content_type_id", "object_id"),
    (
        "netbox_routing_bgprouter",
        "assigned_object_type_id",
        "assigned_object_id",
    ),
)

CREATE_FUNCTION = f"""
CREATE FUNCTION {GUARD_FUNCTION}()
RETURNS trigger
LANGUAGE plpgsql
AS $$
DECLARE
    target_content_type_id bigint;
    target_object_id bigint;
    device_content_type_id bigint;
BEGIN
    target_content_type_id := (to_jsonb(NEW) ->> TG_ARGV[0])::bigint;
    target_object_id := (to_jsonb(NEW) ->> TG_ARGV[1])::bigint;

    SELECT id INTO device_content_type_id
    FROM django_content_type
    WHERE app_label = 'dcim' AND model = 'device';

    IF target_content_type_id = device_content_type_id
       AND NOT EXISTS (
           SELECT 1 FROM dcim_device WHERE id = target_object_id
       )
    THEN
        RAISE EXCEPTION 'Device generic relation targets missing Device %',
            target_object_id
            USING ERRCODE = 'foreign_key_violation';
    END IF;
    RETURN NEW;
END;
$$;
"""


def install_guards(apps, schema_editor):
    connection = schema_editor.connection
    existing_tables = set(connection.introspection.table_names())
    with connection.cursor() as cursor:
        for table_name, content_type_column, object_id_column in GUARDS:
            if table_name not in existing_tables:
                continue
            quoted_table = schema_editor.quote_name(table_name)
            quoted_trigger = schema_editor.quote_name(GUARD_TRIGGER)
            quoted_content_type_column = schema_editor.quote_name(content_type_column)
            quoted_object_id_column = schema_editor.quote_name(object_id_column)
            content_type_argument = content_type_column.replace("'", "''")
            object_id_argument = object_id_column.replace("'", "''")
            cursor.execute(
                f"""
                CREATE TRIGGER {quoted_trigger}
                BEFORE INSERT OR UPDATE OF
                    {quoted_content_type_column}, {quoted_object_id_column}
                ON {quoted_table}
                FOR EACH ROW
                EXECUTE FUNCTION {GUARD_FUNCTION}(
                    '{content_type_argument}',
                    '{object_id_argument}'
                )
                """
            )


def remove_guards(apps, schema_editor):
    connection = schema_editor.connection
    existing_tables = set(connection.introspection.table_names())
    with connection.cursor() as cursor:
        for table_name, _content_type_column, _object_id_column in GUARDS:
            if table_name not in existing_tables:
                continue
            cursor.execute(
                "DROP TRIGGER IF EXISTS "
                f"{schema_editor.quote_name(GUARD_TRIGGER)} ON "
                f"{schema_editor.quote_name(table_name)}"
            )


class Migration(migrations.Migration):
    dependencies = [
        ("forward_netbox", "0041_forwardworkloadstate"),
    ]

    operations = [
        migrations.RunSQL(
            CREATE_FUNCTION,
            reverse_sql=f"DROP FUNCTION IF EXISTS {GUARD_FUNCTION}()",
        ),
        migrations.RunPython(install_guards, remove_guards),
    ]
