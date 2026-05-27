import json
from io import StringIO
from pathlib import Path
from tempfile import TemporaryDirectory

from django.core.management import call_command
from django.core.management.base import CommandError
from django.test import TestCase
from django.utils import timezone

from forward_netbox.choices import ForwardExecutionRunStatusChoices
from forward_netbox.choices import ForwardExecutionStepKindChoices
from forward_netbox.choices import ForwardExecutionStepStatusChoices
from forward_netbox.models import ForwardExecutionRun
from forward_netbox.models import ForwardExecutionStep
from forward_netbox.models import ForwardSource
from forward_netbox.models import ForwardSync
from forward_netbox.utilities.scale_benchmark import scale_benchmark_report


class ScaleBenchmarkReportTest(TestCase):
    def test_report_passes_clean_support_bundle(self):
        report = scale_benchmark_report(
            {
                "run": {
                    "id": 1,
                    "backend": "branching",
                    "status": "completed",
                    "total_steps": 1,
                    "next_step_index": 2,
                    "baseline_ready": True,
                },
                "metrics": {
                    "step_count": 1,
                    "attempted_row_count": 10,
                    "applied_row_count": 10,
                    "failed_row_count": 0,
                    "fetch_modes": ["nqe_column_filter"],
                    "apply_engines": ["adapter"],
                    "pushdown_efficiency": {
                        "fallback_steps": 0,
                        "total_steps": 1,
                        "fallback_rate": 0.0,
                    },
                    "pushdown_runtime": {
                        "fallback_runtime_share": 0.0,
                        "full_fallback_runtime_share": 0.0,
                    },
                    "diff_utilization": {
                        "eligible_steps": 1,
                        "diff_steps": 1,
                        "diff_actual_ratio": 1.0,
                    },
                    "diff_baseline_transition": {
                        "status": "pass",
                        "code": "api_diff_active",
                        "action_code": "keep_query_identity_and_baseline",
                        "message": "All diff-capable stage steps used API diffs.",
                        "backend": "branching",
                        "diff_capable_step_count": 1,
                        "diff_step_count": 1,
                    },
                    "partition_retry_summary": {},
                    "throughput_smoothing": {
                        "wait_share": 0.0,
                        "wait_seconds": 0.0,
                        "total_observed_seconds": 10.0,
                    },
                },
                "steps": [{"index": 1, "status": "merged"}],
            }
        )

        self.assertEqual(report["status"], "pass")
        self.assertEqual(report["summary"]["failed_row_count"], 0)
        checks = {item["code"]: item for item in report["checks"]}
        self.assertEqual(
            checks["diff_baseline_transition"]["evidence"]["transition_code"],
            "api_diff_active",
        )

    def test_report_flags_fallback_and_row_failure_pressure(self):
        report = scale_benchmark_report(
            {
                "run": {"id": 2, "backend": "branching", "status": "completed"},
                "metrics": {
                    "step_count": 4,
                    "attempted_row_count": 100,
                    "failed_row_count": 2,
                    "pushdown_efficiency": {
                        "fallback_steps": 3,
                        "total_steps": 4,
                        "fallback_rate": 0.75,
                    },
                    "pushdown_runtime": {"fallback_runtime_share": 0.8},
                    "diff_utilization": {
                        "eligible_steps": 4,
                        "diff_steps": 1,
                        "diff_actual_ratio": 0.25,
                    },
                    "diff_baseline_transition": {
                        "status": "warn",
                        "code": "missing_or_ineligible_diff_baseline",
                        "action_code": "complete_baseline_then_use_newer_snapshot",
                        "message": "Diff-capable query identity exists, but no eligible prior baseline was available.",
                        "backend": "branching",
                        "diff_capable_step_count": 4,
                        "diff_step_count": 1,
                    },
                    "partition_retry_summary": {},
                    "throughput_smoothing": {"wait_share": 0.4},
                },
                "steps": [{"index": 1, "status": "merged"}],
            }
        )

        self.assertEqual(report["status"], "fail")
        check_statuses = {item["code"]: item["status"] for item in report["checks"]}
        self.assertEqual(check_statuses["pushdown_efficiency"], "fail")
        self.assertEqual(check_statuses["row_failures"], "fail")

    def test_report_flags_completed_run_with_non_terminal_steps(self):
        report = scale_benchmark_report(
            {
                "run": {
                    "id": 3,
                    "backend": "branching",
                    "status": "completed",
                    "total_steps": 3,
                    "next_step_index": 2,
                },
                "metrics": {"step_count": 3},
                "steps": [
                    {"index": 1, "status": "merged"},
                    {"index": 2, "status": "running"},
                    {"index": 3, "status": "pending"},
                ],
            }
        )

        checks = {item["code"]: item for item in report["checks"]}
        self.assertEqual(report["status"], "fail")
        self.assertEqual(checks["run_completion"]["status"], "fail")
        self.assertEqual(
            checks["run_completion"]["evidence"]["non_terminal_step_count"], 2
        )

    def test_report_warns_when_large_run_does_not_use_safe_bulk_orm(self):
        report = scale_benchmark_report(
            {
                "run": {
                    "id": 7,
                    "backend": "branching",
                    "status": "completed",
                    "total_steps": 20,
                    "next_step_index": 21,
                    "baseline_ready": True,
                },
                "metrics": {
                    "step_count": 20,
                    "attempted_row_count": 1000,
                    "applied_row_count": 1000,
                    "failed_row_count": 0,
                    "fetch_modes": ["nqe_column_filter"],
                    "apply_engines": ["adapter"],
                    "pushdown_efficiency": {
                        "fallback_steps": 0,
                        "total_steps": 20,
                        "fallback_rate": 0.0,
                    },
                    "pushdown_runtime": {
                        "fallback_runtime_share": 0.0,
                        "full_fallback_runtime_share": 0.0,
                    },
                    "diff_utilization": {},
                    "diff_baseline_transition": {},
                    "partition_retry_summary": {},
                    "throughput_smoothing": {
                        "wait_share": 0.0,
                        "wait_seconds": 0.0,
                        "total_observed_seconds": 10.0,
                    },
                },
                "steps": [
                    {"index": index, "status": "merged"} for index in range(1, 21)
                ],
            }
        )

        checks = {item["code"]: item for item in report["checks"]}
        self.assertEqual(checks["apply_engine_coverage"]["status"], "warn")
        self.assertEqual(report["status"], "warn")


class ForwardScaleBenchmarkCommandTest(TestCase):
    def setUp(self):
        self.source = ForwardSource.objects.create(
            name="scale-benchmark-source",
            type="saas",
            url="https://fwd.app",
            status="ready",
            parameters={
                "username": "user@example.com",
                "password": "secret",
                "verify": True,
                "network_id": "test-network",
            },
        )
        self.sync = ForwardSync.objects.create(
            name="scale-benchmark-sync",
            source=self.source,
            parameters={"snapshot_id": "latestProcessed"},
        )

    def _run_with_clean_step(self):
        now = timezone.now()
        run = ForwardExecutionRun.objects.create(
            sync=self.sync,
            source=self.source,
            backend="branching",
            status=ForwardExecutionRunStatusChoices.COMPLETED,
            snapshot_selector="latestProcessed",
            snapshot_id="synthetic-after",
            total_steps=1,
            next_step_index=2,
            baseline_ready=True,
            completed=now,
        )
        ForwardExecutionStep.objects.create(
            run=run,
            index=1,
            kind=ForwardExecutionStepKindChoices.STAGE,
            status=ForwardExecutionStepStatusChoices.MERGED,
            model_string="dcim.site",
            label="dcim.site benchmark shard",
            execution_mode="query_id",
            execution_value="synthetic-query",
            sync_mode="diff",
            operation="upsert",
            estimated_changes=10,
            actual_changes=10,
            fetched_row_count=10,
            query_runtime_ms=100.0,
            attempted_row_count=10,
            applied_row_count=10,
            failed_row_count=0,
            fetch_mode="nqe_column_filter",
            apply_engine="adapter",
            started=now,
            completed=now,
        )
        return run

    def test_command_reports_latest_sync_run(self):
        self._run_with_clean_step()

        stream = StringIO()
        call_command(
            "forward_scale_benchmark", "--sync-name", self.sync.name, stdout=stream
        )
        payload = json.loads(stream.getvalue())

        self.assertEqual(payload["status"], "pass")
        self.assertEqual(payload["run"]["backend"], "branching")
        self.assertEqual(payload["summary"]["step_count"], 1)

    def test_command_fail_on_warn_raises_for_incomplete_run(self):
        ForwardExecutionRun.objects.create(
            sync=self.sync,
            source=self.source,
            backend="branching",
            status=ForwardExecutionRunStatusChoices.RUNNING,
            snapshot_selector="latestProcessed",
            snapshot_id="synthetic-after",
            total_steps=1,
            next_step_index=1,
        )

        with self.assertRaises(CommandError):
            call_command(
                "forward_scale_benchmark",
                "--sync-name",
                self.sync.name,
                "--fail-on-warn",
                stdout=StringIO(),
            )

    def test_command_reconcile_reopens_completed_run_with_incomplete_steps(self):
        now = timezone.now()
        run = ForwardExecutionRun.objects.create(
            sync=self.sync,
            source=self.source,
            backend="branching",
            status=ForwardExecutionRunStatusChoices.COMPLETED,
            snapshot_selector="latestProcessed",
            snapshot_id="synthetic-after",
            total_steps=3,
            next_step_index=4,
            baseline_ready=True,
            completed=now,
        )
        ForwardExecutionStep.objects.create(
            run=run,
            index=1,
            kind=ForwardExecutionStepKindChoices.STAGE,
            status=ForwardExecutionStepStatusChoices.MERGED,
            model_string="dcim.site",
        )
        ForwardExecutionStep.objects.create(
            run=run,
            index=2,
            kind=ForwardExecutionStepKindChoices.STAGE,
            status=ForwardExecutionStepStatusChoices.PENDING,
            model_string="dcim.device",
        )
        ForwardExecutionStep.objects.create(
            run=run,
            index=3,
            kind=ForwardExecutionStepKindChoices.STAGE,
            status=ForwardExecutionStepStatusChoices.MERGED,
            model_string="dcim.interface",
        )

        stream = StringIO()
        call_command(
            "forward_scale_benchmark",
            "--run-id",
            str(run.pk),
            "--reconcile",
            stdout=stream,
        )

        payload = json.loads(stream.getvalue())
        run.refresh_from_db()
        self.assertEqual(payload["run"]["status"], "running")
        self.assertEqual(payload["run"]["next_step_index"], 2)
        self.assertEqual(run.status, ForwardExecutionRunStatusChoices.RUNNING)
        self.assertEqual(run.next_step_index, 2)
        self.assertFalse(run.baseline_ready)

    def test_command_rejects_sensitive_offline_support_bundle(self):
        bundle = {
            "run": {"id": 1, "backend": "branching", "status": "completed"},
            "metrics": {"step_count": 1},
            "steps": [{"fetch_parameters": {"".join(["network", "_id"]): "123456"}}],
        }
        with TemporaryDirectory() as tmp_dir:
            bundle_path = Path(tmp_dir) / "bundle.json"
            bundle_path.write_text(json.dumps(bundle), encoding="utf-8")

            with self.assertRaises(CommandError) as raised:
                call_command(
                    "forward_scale_benchmark",
                    "--input-json",
                    str(bundle_path),
                    stdout=StringIO(),
                )

        self.assertIn("sensitive content", str(raised.exception))

    def test_command_accepts_sanitized_offline_support_bundle(self):
        bundle = {
            "run": {"id": 1, "backend": "branching", "status": "completed"},
            "metrics": {
                "step_count": 1,
                "pushdown_efficiency": {
                    "fallback_steps": 0,
                    "total_steps": 1,
                    "fallback_rate": 0.0,
                },
            },
            "steps": [{"index": 1}],
        }
        with TemporaryDirectory() as tmp_dir:
            bundle_path = Path(tmp_dir) / "bundle.json"
            bundle_path.write_text(json.dumps(bundle), encoding="utf-8")
            stream = StringIO()

            call_command(
                "forward_scale_benchmark",
                "--input-json",
                str(bundle_path),
                stdout=stream,
            )

        payload = json.loads(stream.getvalue())
        self.assertEqual(payload["run"]["status"], "completed")

    def test_command_rejects_reconcile_for_offline_support_bundle(self):
        bundle = {
            "run": {"id": 1, "backend": "branching", "status": "completed"},
            "metrics": {"step_count": 1},
            "steps": [{"index": 1}],
        }
        with TemporaryDirectory() as tmp_dir:
            bundle_path = Path(tmp_dir) / "bundle.json"
            bundle_path.write_text(json.dumps(bundle), encoding="utf-8")

            with self.assertRaises(CommandError) as raised:
                call_command(
                    "forward_scale_benchmark",
                    "--input-json",
                    str(bundle_path),
                    "--reconcile",
                    stdout=StringIO(),
                )

        self.assertIn("--reconcile cannot be used", str(raised.exception))
