from django.db import connection

_EMPTY_JSON_DEFAULTS = {
    "'[]'::json",
    "'[]'::jsonb",
}


def _has_empty_json_default(column_default):
    if column_default is None:
        return False
    return str(column_default).replace(" ", "") in _EMPTY_JSON_DEFAULTS


def ensure_core_job_compat_defaults():
    """Backfill legacy `core_job.notifications` defaults when needed."""

    with connection.cursor() as cursor:
        cursor.execute(
            """
            select column_default
            from information_schema.columns
            where table_schema = current_schema()
              and table_name = %s
              and column_name = %s
            """,
            ["core_job", "notifications"],
        )
        column = cursor.fetchone()
        if column is None:
            return
        can_run_schema_change = not getattr(connection, "in_atomic_block", False)
        if not _has_empty_json_default(column[0]) and can_run_schema_change:
            cursor.execute(
                "alter table core_job alter column notifications set default '[]'::jsonb"
            )
        cursor.execute("select 1 from core_job where notifications is null limit 1")
        if cursor.fetchone() is not None:
            cursor.execute(
                "update core_job set notifications = '[]'::jsonb where notifications is null"
            )
