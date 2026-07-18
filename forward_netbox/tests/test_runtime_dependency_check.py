import logging
from unittest.mock import patch

from django.test import SimpleTestCase

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

    def _capture(self):
        logger = logging.getLogger("forward_netbox")
        with self.assertLogs(logger, level="INFO") as cm:
            _check_runtime_dependencies()
        return "\n".join(cm.output)

    def test_resolves_version_by_correct_distribution_name(self):
        # netbox-branching is installed as the netboxlabs-netbox-branching dist.
        self.assertIsNotNone(_resolved_branching_version())

    def test_no_false_not_installed_warning_when_installed(self):
        out = self._capture()
        self.assertNotIn("not installed", out)
        self.assertNotIn("syncs will fail", out)

    def test_warns_when_version_is_not_exact(self):
        with patch("forward_netbox._resolved_branching_version", return_value="1.0.4"):
            out = self._capture()
        self.assertIn("netbox_branching==1.1.1", out)
        self.assertIn("syncs will fail", out)

        with patch("forward_netbox._resolved_branching_version", return_value="1.2.0"):
            out = self._capture()
        self.assertIn("netbox_branching==1.1.1", out)
        self.assertIn("syncs will fail", out)

    def test_warns_when_not_importable(self):
        real_import = (
            __builtins__["__import__"]
            if isinstance(__builtins__, dict)
            else __builtins__.__import__
        )

        def fake_import(name, *args, **kwargs):
            if name == "netbox_branching":
                raise ImportError("simulated missing plugin")
            return real_import(name, *args, **kwargs)

        logger = logging.getLogger("forward_netbox")
        with patch("builtins.__import__", side_effect=fake_import):
            with self.assertLogs(logger, level="WARNING") as cm:
                _check_runtime_dependencies()
        out = "\n".join(cm.output)
        self.assertIn("not installed/enabled", out)
        self.assertIn("PLUGINS", out)
