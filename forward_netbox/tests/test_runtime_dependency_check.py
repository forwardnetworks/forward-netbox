from unittest.mock import patch

from django.core.exceptions import ImproperlyConfigured
from django.test import SimpleTestCase

from forward_netbox import NetboxForwardConfig
from forward_netbox import _check_runtime_dependencies
from forward_netbox import _resolved_branching_version


class RuntimeDependencyCheckTest(SimpleTestCase):
    """Guards the netbox_branching startup dependency check.

    Regression: the check keyed off the distribution name ``netbox-branching``,
    but the PyPI distribution is ``netboxlabs-netbox-branching`` — so
    importlib.metadata.version() always raised PackageNotFoundError and the
    plugin logged a false "netbox_branching is not installed; syncs will fail"
    warning on EVERY boot, even when branching was installed and active.
    """

    def test_resolves_version_by_correct_distribution_name(self):
        # netbox-branching is installed as the netboxlabs-netbox-branching dist.
        self.assertIsNotNone(_resolved_branching_version())

    def test_plugin_config_version_matches_package_release(self):
        self.assertEqual(NetboxForwardConfig.version, "2.6.1")

    def test_exact_runtime_passes(self):
        _check_runtime_dependencies()

    def test_rejects_version_that_is_not_exact(self):
        with patch("forward_netbox._resolved_branching_version", return_value="1.0.4"):
            with self.assertRaises(ImproperlyConfigured):
                _check_runtime_dependencies()

        with patch("forward_netbox._resolved_branching_version", return_value="1.2.0"):
            with self.assertRaises(ImproperlyConfigured):
                _check_runtime_dependencies()

        with patch("forward_netbox._resolved_branching_version", return_value=None):
            with self.assertRaises(ImproperlyConfigured):
                _check_runtime_dependencies()

    def test_rejects_when_not_importable(self):
        real_import = (
            __builtins__["__import__"]
            if isinstance(__builtins__, dict)
            else __builtins__.__import__
        )

        def fake_import(name, *args, **kwargs):
            if name == "netbox_branching":
                raise ImportError("simulated missing plugin")
            return real_import(name, *args, **kwargs)

        with patch("builtins.__import__", side_effect=fake_import):
            with self.assertRaises(ImproperlyConfigured) as ctx:
                _check_runtime_dependencies()
        self.assertIn("must be enabled", str(ctx.exception))
