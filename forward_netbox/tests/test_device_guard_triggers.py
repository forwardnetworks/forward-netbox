# NetBox 4.7.4's `extras.0145` widens `extras_taggeditem.object_id`. The plugin's
# guard trigger was declared `UPDATE OF content_type_id, object_id`, which pins
# those columns, so NetBox's own migrate failed on an existing install: "cannot
# alter type of a column used in a trigger definition". The guard is rebuilt
# without a column list, before any migration runs, and still refuses a tag that
# points at a missing device.
from django.db import connection
from django.db import DatabaseError
from django.db import transaction
from django.test import TransactionTestCase

from forward_netbox.utilities.device_guard_triggers import GUARD_FUNCTION
from forward_netbox.utilities.device_guard_triggers import GUARD_TRIGGER
from forward_netbox.utilities.device_guard_triggers import rebuild_column_bound_guards

TABLE = "extras_taggeditem"


def _column_count():
    with connection.cursor() as cursor:
        cursor.execute(
            "SELECT coalesce(array_length(tgattr::int2[], 1), 0) FROM pg_trigger "
            "WHERE tgname = %s AND tgrelid = %s::regclass",
            [GUARD_TRIGGER, TABLE],
        )
        row = cursor.fetchone()
    return None if row is None else row[0]


class RebuildColumnBoundGuardsTest(TransactionTestCase):
    def _install_old_style(self):
        with connection.cursor() as cursor:
            cursor.execute(f'DROP TRIGGER IF EXISTS "{GUARD_TRIGGER}" ON "{TABLE}"')
            cursor.execute(
                f'CREATE TRIGGER "{GUARD_TRIGGER}" BEFORE INSERT OR UPDATE OF '
                f'"content_type_id", "object_id" ON "{TABLE}" FOR EACH ROW '
                f"EXECUTE FUNCTION {GUARD_FUNCTION}('content_type_id', 'object_id')"
            )

    def test_the_old_trigger_blocks_netboxs_column_change_and_the_rebuild_frees_it(
        self,
    ):
        self._install_old_style()
        self.assertEqual(_column_count(), 2)
        with self.assertRaises(DatabaseError):
            with transaction.atomic(), connection.cursor() as cursor:
                cursor.execute(
                    f'ALTER TABLE "{TABLE}" ALTER COLUMN "object_id" TYPE bigint'
                )

        rebuilt = rebuild_column_bound_guards()

        self.assertGreaterEqual(rebuilt, 1)
        self.assertEqual(_column_count(), 0)
        with transaction.atomic(), connection.cursor() as cursor:
            cursor.execute(
                f'ALTER TABLE "{TABLE}" ALTER COLUMN "object_id" TYPE bigint'
            )

    def test_the_rebuilt_guard_still_refuses_a_tag_on_a_missing_device(self):
        self._install_old_style()
        rebuild_column_bound_guards()
        from django.contrib.contenttypes.models import ContentType
        from dcim.models import Device
        from extras.models import Tag

        tag = Tag.objects.create(name="guard-t", slug="guard-t")
        device_type = ContentType.objects.get_for_model(Device)
        with self.assertRaises(DatabaseError):
            with transaction.atomic(), connection.cursor() as cursor:
                cursor.execute(
                    f'INSERT INTO "{TABLE}" (object_id, content_type_id, tag_id) '
                    "VALUES (%s, %s, %s)",
                    [999999999, device_type.pk, tag.pk],
                )

    def test_running_it_again_changes_nothing(self):
        self._install_old_style()
        rebuild_column_bound_guards()

        self.assertEqual(rebuild_column_bound_guards(), 0)
        self.assertEqual(_column_count(), 0)
