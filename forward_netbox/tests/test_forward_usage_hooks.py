"""Pin `UsageTrackingHooks`'s mapping from SDK request-hook events onto the
plugin's own `ApiUsageTracker` counters (forward-sdk migration plan, step 5
of 7). Not wired into a real client yet - these tests call the hooks
directly, the same way the SDK's `Transport` calls them.
"""

from unittest import TestCase

import httpx

from forward_netbox.utilities.forward_usage import ApiUsageTracker
from forward_netbox.utilities.forward_usage_hooks import UsageTrackingHooks


def _response(status_code):
    request = httpx.Request("GET", "https://fwd.example.invalid/api/networks")
    return httpx.Response(status_code, request=request)


class UsageTrackingHooksTest(TestCase):
    def setUp(self):
        self.usage = ApiUsageTracker(api_requests_per_minute=0)
        self.hooks = UsageTrackingHooks(self.usage)

    def test_on_request_records_one_http_attempt(self):
        self.hooks.on_request(None, None)
        self.hooks.on_request(None, None)

        summary = self.usage.summary()

        self.assertEqual(summary["http_attempts"], 2)

    def test_a_successful_response_records_a_success_and_its_status_class(self):
        self.hooks.on_response(None, _response(200), 0.1)

        summary = self.usage.summary()

        self.assertEqual(summary["http_successes"], 1)
        self.assertEqual(summary["http_failures"], 0)
        self.assertEqual(summary["http_status_classes"], {"2xx": 1})

    def test_a_transient_status_failure_is_classified_as_transient(self):
        self.hooks.on_response(None, _response(503), 0.1)

        summary = self.usage.summary()

        self.assertEqual(summary["http_failures"], 1)
        self.assertEqual(summary["http_status_failures"], 1)
        self.assertEqual(summary["http_transient_status_failures"], 1)
        self.assertEqual(summary["http_nontransient_status_failures"], 0)

    def test_a_nontransient_status_failure_is_classified_as_nontransient(self):
        self.hooks.on_response(None, _response(403), 0.1)

        summary = self.usage.summary()

        self.assertEqual(summary["http_failures"], 1)
        self.assertEqual(summary["http_nontransient_status_failures"], 1)
        self.assertEqual(summary["http_transient_status_failures"], 0)

    def test_a_429_response_also_records_the_dedicated_429_counter(self):
        self.hooks.on_response(None, _response(429), 0.1)

        summary = self.usage.summary()

        self.assertEqual(summary["http_429_failures"], 1)
        # 429 is transient (it's in TRANSIENT_FORWARD_HTTP_STATUS_CODES).
        self.assertEqual(summary["http_transient_status_failures"], 1)

    def test_on_retry_records_a_retry(self):
        self.hooks.on_retry(None, 0, 1.5, "HTTP 503")

        summary = self.usage.summary()

        self.assertEqual(summary["http_retries"], 1)

    def test_on_sleep_is_not_overridden_to_avoid_double_counting_the_throttle(self):
        # Throttle.throttle() already records throttle_sleep_seconds on this
        # same usage tracker; on_sleep firing for the same pacing event must
        # be a no-op here, or every sleep would be counted twice.
        self.hooks.on_sleep(2.5, "rate-limit")

        summary = self.usage.summary()

        self.assertEqual(summary["throttle_sleep_seconds"], 0.0)
