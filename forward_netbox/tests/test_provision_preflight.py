# A plugin in PLUGINS whose migrations were never applied leaves ContentTypes
# registered but tables missing; branch provisioning then dies mid-CREATE TABLE
# with an opaque ProgrammingError (field report: netbox_dlm_contract). The
# preflight must catch it before provisioning, with the remedy in the message.
from unittest.mock import Mock
from unittest.mock import patch

from core.exceptions import SyncError
from django.test import TestCase

from forward_netbox.utilities.branching import missing_branch_table_report
from forward_netbox.utilities.health import _database_tables_check
from forward_netbox.utilities.single_branch_executor import (
    ForwardSingleBranchExecutor,
)


class MissingBranchTableReportTest(TestCase):
    def test_healthy_database_reports_nothing(self):
        self.assertEqual(missing_branch_table_report(), {})

    def test_unmigrated_table_is_reported(self):
        real_tables = ["dcim_device", "missing_plugin_widget"]
        with patch(
            "netbox_branching.utilities.get_tables_to_replicate",
            return_value=real_tables,
        ):
            report = missing_branch_table_report()
        self.assertEqual(report, {"unknown": ["missing_plugin_widget"]})


class ExecutorTablePreflightTest(TestCase):
    def test_run_raises_actionable_sync_error_before_fetch(self):
        executor = object.__new__(ForwardSingleBranchExecutor)
        executor.logger = Mock()
        executor.sync = Mock()
        with (
            patch(
                "forward_netbox.utilities.single_branch_executor."
                "missing_branch_table_report",
                return_value={"netbox_dlm": ["netbox_dlm_contract"]},
            ),
        ):
            with self.assertRaises(SyncError) as ctx:
                executor.run()
        message = str(ctx.exception)
        self.assertIn("netbox_dlm", message)
        self.assertIn("netbox_dlm_contract", message)
        self.assertIn("migrate", message)


class DatabaseTablesHealthCheckTest(TestCase):
    def test_healthy_database_passes(self):
        check = _database_tables_check()
        self.assertIsNotNone(check)
        self.assertEqual(check["status"], "pass")

    def test_missing_tables_fail_with_remedy(self):
        with patch(
            "forward_netbox.utilities.health.missing_branch_table_report",
            return_value={"netbox_dlm": ["netbox_dlm_contract"]},
        ):
            check = _database_tables_check()
        self.assertEqual(check["status"], "fail")
        self.assertIn("netbox_dlm_contract", check["message"])
        self.assertIn("migrate", check["message"])

    def test_diagnostics_failure_returns_none_not_crash(self):
        with patch(
            "forward_netbox.utilities.health.missing_branch_table_report",
            side_effect=RuntimeError("introspection unavailable"),
        ):
            self.assertIsNone(_database_tables_check())
