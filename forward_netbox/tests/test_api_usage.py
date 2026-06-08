from django.test import SimpleTestCase

from forward_netbox.choices import ForwardSourceDeploymentChoices
from forward_netbox.utilities.api_usage import evaluate_forward_api_usage


class ForwardApiUsageEvaluationTest(SimpleTestCase):
    def test_forward_saas_default_pacing_passes_with_headroom(self):
        evaluation = evaluate_forward_api_usage(
            {
                "api_requests_per_minute": 1800,
                "http_attempts": 12,
                "http_429_failures": 0,
                "nqe_query_calls": 2,
                "nqe_diff_calls": 1,
                "nqe_pages": 4,
                "throttle_sleep_seconds": 1.23456,
            },
            source_type=ForwardSourceDeploymentChoices.SAAS,
        )

        self.assertEqual(evaluation["status"], "passed")
        self.assertEqual(evaluation["failure_reasons"], [])
        self.assertEqual(evaluation["warnings"], [])
        self.assertEqual(
            evaluation["metrics"]["headroom_requests_per_minute"],
            200,
        )
        self.assertEqual(evaluation["metrics"]["nqe_calls"], 3)
        self.assertEqual(evaluation["metrics"]["throttle_sleep_seconds"], 1.235)

    def test_forward_saas_rate_above_hard_block_fails(self):
        evaluation = evaluate_forward_api_usage(
            {"api_requests_per_minute": 2001},
            source_type=ForwardSourceDeploymentChoices.SAAS,
        )

        self.assertEqual(evaluation["status"], "failed")
        self.assertEqual(
            evaluation["failure_reasons"],
            ["configured_requests_per_minute_exceeds_forward_saas_hard_block"],
        )

    def test_forward_saas_near_hard_block_warns(self):
        evaluation = evaluate_forward_api_usage(
            {"api_requests_per_minute": 1900},
            source_type=ForwardSourceDeploymentChoices.SAAS,
        )

        self.assertEqual(evaluation["status"], "warning")
        self.assertIn(
            "configured_requests_per_minute_near_forward_saas_hard_block",
            evaluation["warnings"],
        )

    def test_forward_saas_429_failures_warn(self):
        evaluation = evaluate_forward_api_usage(
            {"api_requests_per_minute": 1800, "http_429_failures": 1},
            source_type=ForwardSourceDeploymentChoices.SAAS,
        )

        self.assertEqual(evaluation["status"], "warning")
        self.assertEqual(evaluation["warnings"], ["forward_api_429_observed"])

    def test_short_observed_rate_sample_is_evidence_only(self):
        evaluation = evaluate_forward_api_usage(
            {
                "api_requests_per_minute": 1800,
                "http_attempts": 5,
                "usage_window_seconds": 1.0,
                "observed_http_attempts_per_minute": 240.0,
            },
            source_type=ForwardSourceDeploymentChoices.SAAS,
        )

        self.assertEqual(evaluation["status"], "passed")
        self.assertFalse(evaluation["metrics"]["observed_rate_sample_complete"])
        self.assertEqual(
            evaluation["metrics"]["observed_http_attempts_per_minute"],
            240.0,
        )

    def test_sustained_observed_rate_above_hard_block_fails(self):
        evaluation = evaluate_forward_api_usage(
            {
                "api_requests_per_minute": 1800,
                "http_attempts": 40,
                "usage_window_seconds": 60.0,
                "observed_http_attempts_per_minute": 2100.0,
            },
            source_type=ForwardSourceDeploymentChoices.SAAS,
        )

        self.assertEqual(evaluation["status"], "failed")
        self.assertIn(
            "observed_http_attempts_per_minute_exceeds_forward_saas_hard_block",
            evaluation["failure_reasons"],
        )
        self.assertTrue(evaluation["metrics"]["observed_rate_sample_complete"])

    def test_forward_saas_disabled_pacing_warns(self):
        evaluation = evaluate_forward_api_usage(
            {"api_requests_per_minute": 0},
            source_type=ForwardSourceDeploymentChoices.SAAS,
        )

        self.assertEqual(evaluation["status"], "warning")
        self.assertEqual(evaluation["warnings"], ["forward_saas_pacing_disabled"])

    def test_custom_source_does_not_apply_saas_rate_budget(self):
        evaluation = evaluate_forward_api_usage(
            {"api_requests_per_minute": 2500},
            source_type=ForwardSourceDeploymentChoices.CUSTOM,
        )

        self.assertEqual(evaluation["status"], "passed")
        self.assertEqual(evaluation["failure_reasons"], [])
        self.assertEqual(evaluation["warnings"], [])

    def test_unknown_source_uses_conservative_saas_default_budget(self):
        evaluation = evaluate_forward_api_usage({}, source_type=None)

        self.assertEqual(evaluation["status"], "passed")
        self.assertEqual(
            evaluation["metrics"]["configured_requests_per_minute"],
            1800,
        )

    def test_read_cache_metrics_are_reported(self):
        evaluation = evaluate_forward_api_usage(
            {
                "read_cache_hits": 3,
                "read_cache_misses": 1,
            },
            source_type=ForwardSourceDeploymentChoices.CUSTOM,
        )

        self.assertEqual(evaluation["metrics"]["read_cache_hits"], 3)
        self.assertEqual(evaluation["metrics"]["read_cache_misses"], 1)
        self.assertEqual(evaluation["metrics"]["read_cache_hit_rate"], 0.75)
