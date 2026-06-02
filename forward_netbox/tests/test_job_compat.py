from unittest.mock import MagicMock
from unittest.mock import patch

from django.test import SimpleTestCase

from forward_netbox.utilities.job_compat import ensure_core_job_compat_defaults


class CoreJobCompatDefaultsTest(SimpleTestCase):
    def _run_with_fetches(self, fetches, *, in_atomic_block=False):
        cursor = MagicMock()
        cursor.fetchone.side_effect = fetches
        cursor_context = MagicMock()
        cursor_context.__enter__.return_value = cursor
        connection = MagicMock()
        connection.in_atomic_block = in_atomic_block
        connection.cursor.return_value = cursor_context
        with patch("forward_netbox.utilities.job_compat.connection", connection):
            ensure_core_job_compat_defaults()
        return [call.args[0] for call in cursor.execute.call_args_list]

    def test_returns_when_notifications_column_is_absent(self):
        queries = self._run_with_fetches([None])

        self.assertEqual(len(queries), 1)
        self.assertIn("information_schema.columns", queries[0])

    def test_skips_alter_when_default_already_exists(self):
        queries = self._run_with_fetches([("'[]'::jsonb",), None])

        self.assertFalse(any("alter table core_job" in query for query in queries))
        self.assertTrue(
            any(
                "from core_job where notifications is null" in query
                for query in queries
            )
        )

    def test_sets_default_and_backfills_null_notifications_when_needed(self):
        queries = self._run_with_fetches([(None,), (1,)])

        self.assertTrue(any("alter table core_job" in query for query in queries))
        self.assertTrue(any("update core_job" in query for query in queries))

    def test_skips_schema_change_inside_open_transaction(self):
        queries = self._run_with_fetches([(None,), None], in_atomic_block=True)

        self.assertFalse(any("alter table core_job" in query for query in queries))
