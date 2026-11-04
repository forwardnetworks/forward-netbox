# The device generic-relation guard triggers, in a form that does not pin columns.
#
# Migration 0042 created each guard as `BEFORE INSERT OR UPDATE OF <type col>,
# <id col>`. A column list makes Postgres record a dependency on those columns,
# and NetBox 4.7.4's `extras.0145_taggeditem_object_id_bigint` widens
# `extras_taggeditem.object_id`, so on an existing install NetBox's own migrate
# failed: "cannot alter type of a column used in a trigger definition". A fresh
# database never hits it, because there NetBox's migration runs before ours
# creates the trigger.
#
# The guard now fires on every INSERT and UPDATE and returns at once when an
# update leaves both columns unchanged, which is what the column list did. It is
# rebuilt in every schema that carries it (netbox-branching copies triggers into
# branch schemas), from the catalogue rather than from a guessed search_path.
from django.db import connections

GUARD_FUNCTION = "forward_netbox_enforce_device_generic_relation"
GUARD_TRIGGER = "forward_netbox_device_gfk_guard"

GUARD_FUNCTION_SQL = f"""
CREATE OR REPLACE FUNCTION {GUARD_FUNCTION}()
RETURNS trigger
LANGUAGE plpgsql
AS $$
DECLARE
    target_content_type_id bigint;
    target_object_id bigint;
    device_content_type_id bigint;
BEGIN
    IF TG_OP = 'UPDATE'
       AND (to_jsonb(OLD) ->> TG_ARGV[0]) IS NOT DISTINCT FROM
           (to_jsonb(NEW) ->> TG_ARGV[0])
       AND (to_jsonb(OLD) ->> TG_ARGV[1]) IS NOT DISTINCT FROM
           (to_jsonb(NEW) ->> TG_ARGV[1])
    THEN
        RETURN NEW;
    END IF;

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


def create_trigger_sql(quote, schema_name, table_name, type_column, id_column):
    """The CREATE TRIGGER statement, with no column list."""
    target = (
        f"{quote(schema_name)}.{quote(table_name)}"
        if schema_name
        else quote(table_name)
    )
    type_argument = type_column.replace("'", "''")
    id_argument = id_column.replace("'", "''")
    return (
        f"CREATE TRIGGER {quote(GUARD_TRIGGER)} "
        f"BEFORE INSERT OR UPDATE ON {target} "
        f"FOR EACH ROW EXECUTE FUNCTION {GUARD_FUNCTION}("
        f"'{type_argument}', '{id_argument}')"
    )


def rebuild_column_bound_guards(using="default"):
    """Rewrite every guard trigger that still pins its columns. Idempotent.

    Returns how many triggers were rebuilt. A no-op where the guard function
    does not exist yet (a database before migration 0042).
    """
    database = connections[using]
    if database.vendor != "postgresql":
        return 0
    quote = database.ops.quote_name
    with database.cursor() as cursor:
        cursor.execute("SELECT to_regproc(%s) IS NOT NULL", [GUARD_FUNCTION])
        if not cursor.fetchone()[0]:
            return 0
        cursor.execute(GUARD_FUNCTION_SQL)
        cursor.execute(
            """
            SELECT n.nspname, c.relname,
                   encode(t.tgargs, 'escape')
            FROM pg_trigger t
            JOIN pg_class c ON c.oid = t.tgrelid
            JOIN pg_namespace n ON n.oid = c.relnamespace
            JOIN pg_proc p ON p.oid = t.tgfoid
            WHERE t.tgname = %s AND p.proname = %s AND NOT t.tgisinternal
              AND array_length(t.tgattr::int2[], 1) > 0
            """,
            [GUARD_TRIGGER, GUARD_FUNCTION],
        )
        bound = cursor.fetchall()
        for schema_name, table_name, raw_args in bound:
            # tgargs is the NUL-separated argument list: type column, id column.
            args = [part for part in (raw_args or "").split("\\000") if part]
            if len(args) != 2:
                continue
            cursor.execute(
                f"DROP TRIGGER {quote(GUARD_TRIGGER)} ON "
                f"{quote(schema_name)}.{quote(table_name)}"
            )
            cursor.execute(
                create_trigger_sql(quote, schema_name, table_name, args[0], args[1])
            )
    return len(bound)


def rebuild_before_migrate(sender, using="default", **kwargs):
    """`pre_migrate` hook: free the columns before ANY app's migrations run.

    The rewrite is also a migration (0068), but NetBox's `extras.0145` does not
    depend on it, so an upgrade could run NetBox's first and fail. Ordering that
    migration before NetBox's would make an install already on 4.7.4 refuse to
    migrate (an applied migration depending on an unapplied one), so the rewrite
    runs here, before the plan, whatever its order.
    """
    if getattr(sender, "label", None) != "forward_netbox":
        return
    try:
        rebuild_column_bound_guards(using=using)
    except Exception:  # noqa: BLE001 - never stop a migrate; 0068 retries
        import logging

        logging.getLogger("forward_netbox").warning(
            "Could not rebuild the device guard triggers before migrating; "
            "migration 0068 will."
        )
