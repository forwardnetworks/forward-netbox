from unittest.mock import patch

from django.core.exceptions import ImproperlyConfigured
from django.test import SimpleTestCase

from forward_netbox import _check_runtime_dependencies
from forward_netbox import _resolved_branching_version
from forward_netbox import NetboxForwardConfig


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
        self.assertEqual(NetboxForwardConfig.version, "3.0.0")

    def test_exact_runtime_passes(self):
        _check_runtime_dependencies()

    def test_rejects_version_that_is_not_exact(self):
        # 1.2 is the supported series now, so the versions either side of it are
        # what must be refused. 1.1.x is the branching line for NetBox 4.6 and
        # cannot run on 4.7 at all; loading against it would be the worst kind
        # of failure, since the plugin would start and the merge internals it
        # reaches into would differ underneath.
        for version in ("1.0.4", "1.1.3", "1.3.0", None):
            with self.subTest(branching=version):
                with patch(
                    "forward_netbox._resolved_branching_version", return_value=version
                ):
                    with self.assertRaises(ImproperlyConfigured):
                        _check_runtime_dependencies()

    def test_accepts_the_supported_series_including_a_prerelease(self):
        # 3.0 ships against 1.2.0 final. The prerelease stays in this list on
        # purpose: `series_matches` accepts it by prefix, and anyone who
        # installed the beta before final shipped must not be locked out by a
        # version check that only ever saw release strings.
        for version in ("1.2.0b1", "1.2.0", "1.2.4"):
            with self.subTest(branching=version):
                with patch(
                    "forward_netbox._resolved_branching_version", return_value=version
                ):
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
