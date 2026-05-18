from django.db import migrations
from django.db import models


class Migration(migrations.Migration):
    dependencies = [
        ("forward_netbox", "0013_execution_run_step"),
    ]

    operations = [
        migrations.SeparateDatabaseAndState(
            database_operations=[
                migrations.RunSQL(
                    sql="""
                    ALTER TABLE forward_netbox_execution_step
                    ADD COLUMN IF NOT EXISTS fetch_key_family varchar(50) NOT NULL DEFAULT '';
                    ALTER TABLE forward_netbox_execution_step
                    ALTER COLUMN fetch_key_family DROP DEFAULT;
                    """,
                    reverse_sql="""
                    ALTER TABLE forward_netbox_execution_step
                    DROP COLUMN IF EXISTS fetch_key_family;
                    """,
                ),
                migrations.RunSQL(
                    sql="""
                    ALTER TABLE forward_netbox_execution_step
                    ADD COLUMN IF NOT EXISTS fetch_parameters jsonb NOT NULL DEFAULT '{}'::jsonb;
                    ALTER TABLE forward_netbox_execution_step
                    ALTER COLUMN fetch_parameters DROP DEFAULT;
                    """,
                    reverse_sql="""
                    ALTER TABLE forward_netbox_execution_step
                    DROP COLUMN IF EXISTS fetch_parameters;
                    """,
                ),
                migrations.RunSQL(
                    sql="""
                    ALTER TABLE forward_netbox_execution_step
                    ADD COLUMN IF NOT EXISTS fetch_column_filters jsonb NOT NULL DEFAULT '[]'::jsonb;
                    ALTER TABLE forward_netbox_execution_step
                    ALTER COLUMN fetch_column_filters DROP DEFAULT;
                    """,
                    reverse_sql="""
                    ALTER TABLE forward_netbox_execution_step
                    DROP COLUMN IF EXISTS fetch_column_filters;
                    """,
                ),
            ],
            state_operations=[
                migrations.AddField(
                    model_name="forwardexecutionstep",
                    name="fetch_key_family",
                    field=models.CharField(blank=True, default="", max_length=50),
                ),
                migrations.AddField(
                    model_name="forwardexecutionstep",
                    name="fetch_parameters",
                    field=models.JSONField(blank=True, default=dict),
                ),
                migrations.AddField(
                    model_name="forwardexecutionstep",
                    name="fetch_column_filters",
                    field=models.JSONField(blank=True, default=list),
                ),
            ],
        ),
    ]
