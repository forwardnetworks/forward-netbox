from unittest.mock import Mock

from django.test import TestCase

from forward_netbox.models import ForwardSource
from forward_netbox.models import ForwardSync
from forward_netbox.utilities.sync import ForwardSyncRunner


class ModuleReadinessRollupTest(TestCase):
    """Rows without a module-bay identity produce one actionable summary."""

    def _runner(self):
        source = ForwardSource.objects.create(
            name=f"mr-src-{ForwardSource.objects.count()}",
            type="saas",
            url="https://fwd.app",
            status="ready",
            parameters={
                "username": "u",
                "password": "p",
                "verify": True,
                "network_id": "n",
            },
        )
        sync = ForwardSync.objects.create(
            name=f"mr-sync-{source.pk}",
            source=source,
            parameters={"snapshot_id": "x"},
        )
        return ForwardSyncRunner(sync=sync, ingestion=None, client=None, logger_=Mock())

    def _warnings(self, runner):
        return [c.args[0] for c in runner.logger.log_warning.call_args_list if c.args]

    def test_module_bay_skips_collapse_to_one_summary(self):
        runner = self._runner()
        for i in range(12):
            runner._record_aggregated_skip_warning(
                model_string="dcim.module",
                reason="missing-module-bay",
                warning_message="per-row text that must NOT be logged",
                sample=f"dev{i}/module 0",
            )
        # Nothing logged per row during the model apply.
        self.assertEqual(self._warnings(runner), [])

        runner._emit_aggregated_skip_warning_summaries("dcim.module")
        warns = self._warnings(runner)
        self.assertEqual(len(warns), 1)
        summary = warns[0]
        self.assertIn("Skipped 12 dcim.module row(s)", summary)
        self.assertIn("did not provide a module-bay name", summary)
        self.assertIn("dev0/module 0", summary)  # an example is shown
        self.assertIn("(+7 more)", summary)  # 12 total - 5 sampled = 7

    def test_few_skips_show_all_examples_no_more_suffix(self):
        runner = self._runner()
        for name in ("a", "b", "c"):
            runner._record_aggregated_skip_warning(
                model_string="dcim.module",
                reason="missing-module-bay",
                warning_message="x",
                sample=f"{name}/module 0",
            )
        runner._emit_aggregated_skip_warning_summaries("dcim.module")
        summary = self._warnings(runner)[0]
        self.assertIn("Skipped 3 dcim.module row(s)", summary)
        self.assertNotIn("more)", summary)

    def test_non_rollup_reason_keeps_per_row_then_suppressed(self):
        # Regression guard: ordinary skip reasons are unchanged by the rollup.
        runner = self._runner()
        for i in range(22):
            runner._record_aggregated_skip_warning(
                model_string="dcim.macaddress",
                reason="missing-interface",
                warning_message=f"skip {i}",
            )
        # First CONFLICT_WARNING_DETAIL_LIMIT (20) logged per row.
        self.assertEqual(len(self._warnings(runner)), 20)

        runner._emit_aggregated_skip_warning_summaries("dcim.macaddress")
        summary = self._warnings(runner)[-1]
        self.assertIn("Suppressed 2 additional", summary)
        self.assertIn("missing-interface", summary)
