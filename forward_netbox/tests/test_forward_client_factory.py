"""Pin `get_client`'s config wiring (forward-sdk migration plan, step 5 of 7).

Not called from `ForwardClient.__init__` yet - that wiring is the next
commit in the sequence. Constructing an `SDKForwardClient` does no I/O
(`ForwardClient.__init__` only builds config objects and service handles),
so these tests exercise the real SDK client class directly rather than a
double.
"""

from unittest import TestCase
from unittest.mock import Mock
from unittest.mock import patch

from forward_sdk import ForwardClient as SDKForwardClient

from forward_netbox.utilities.forward_client_factory import _CrossProcessThrottleAdapter
from forward_netbox.utilities.forward_client_factory import get_client
from forward_netbox.utilities.forward_client_factory import USER_AGENT


class CrossProcessThrottleAdapterTest(TestCase):
    def test_acquire_delegates_to_the_wrapped_throttle_and_returns_seconds_slept(self):
        plugin_throttle = Mock()
        adapter = _CrossProcessThrottleAdapter(plugin_throttle)

        slept = adapter.acquire()

        plugin_throttle.throttle.assert_called_once_with()
        self.assertIsInstance(slept, float)
        self.assertGreaterEqual(slept, 0.0)


class GetClientTest(TestCase):
    def _client(self, **overrides):
        kwargs = {
            "base_url": "https://fwd.example.invalid",
            "username": "svc-account",
            "password": "s3cret",
            "verify": True,
            "timeout": 45.0,
            "retries": 2,
            "api_requests_per_minute": 0,
            "throttle": Mock(),
        }
        kwargs.update(overrides)
        with patch(
            "forward_netbox.utilities.forward_client_factory.resolve_proxies",
            return_value=None,
        ):
            return get_client(**kwargs)

    def test_returns_a_real_sdk_client(self):
        self.assertIsInstance(self._client(), SDKForwardClient)

    def test_credentials_and_tls_pass_through(self):
        client = self._client()
        self.assertEqual(client.config.username, "svc-account")
        self.assertEqual(client.config.password, "s3cret")
        self.assertTrue(client.config.verify)

    def test_retry_count_becomes_a_retry_policy_with_one_more_attempt(self):
        client = self._client(retries=2)
        self.assertEqual(client.config.retries.max_attempts, 3)

    def test_a_zero_requests_per_minute_does_not_reach_the_sdk_as_zero(self):
        # `coerce_api_requests_per_minute`'s "disabled" value is 0; the SDK's
        # own RateLimiter raises ValueError on a non-positive rate, so 0 must
        # become None here rather than being passed through literally. Moot
        # for actual pacing either way, since the custom throttle below takes
        # exclusive precedence over `rate_limit_rpm`, but the client must
        # still construct without raising.
        client = self._client(api_requests_per_minute=0)
        self.assertIsNone(client.config.rate_limit_rpm)

    def test_the_custom_throttle_is_installed_and_used_exclusively(self):
        plugin_throttle = Mock()
        client = self._client(throttle=plugin_throttle)

        installed = client._transport._throttle

        self.assertIsInstance(installed, _CrossProcessThrottleAdapter)
        installed.acquire()
        plugin_throttle.throttle.assert_called_once_with()

    def test_user_agent_matches_the_plugins_own(self):
        client = self._client()
        self.assertEqual(client.config.user_agent, USER_AGENT)

    def test_no_proxy_configured_leaves_the_client_unproxied(self):
        client = self._client()
        self.assertIsNone(client.config.proxy)

    def test_a_resolved_proxy_for_the_base_urls_scheme_is_used(self):
        with patch(
            "forward_netbox.utilities.forward_client_factory.resolve_proxies",
            return_value={"https": "http://proxy.example.invalid:3128"},
        ) as mock_resolve:
            client = get_client(
                base_url="https://fwd.example.invalid",
                username="svc-account",
                password="s3cret",
                verify=True,
                timeout=45.0,
                retries=2,
                api_requests_per_minute=0,
                throttle=Mock(),
            )

        self.assertEqual(client.config.proxy, "http://proxy.example.invalid:3128")
        mock_resolve.assert_called_once()
        self.assertEqual(
            mock_resolve.call_args.kwargs["url"], "https://fwd.example.invalid"
        )
