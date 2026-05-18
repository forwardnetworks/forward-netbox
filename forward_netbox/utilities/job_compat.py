from django.db import connection


def ensure_core_job_compat_defaults():
    """Backfill legacy `core_job.notifications` defaults when needed."""

    with connection.cursor() as cursor:
        cursor.execute(
            """
            select 1
            from information_schema.columns
            where table_name = %s and column_name = %s
            """,
            ["core_job", "notifications"],
        )
        if cursor.fetchone() is None:
            return
        cursor.execute(
            "alter table core_job alter column notifications set default '[]'::jsonb"
        )
        cursor.execute(
            "update core_job set notifications = '[]'::jsonb where notifications is null"
        )
