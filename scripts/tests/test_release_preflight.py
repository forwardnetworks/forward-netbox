"""Tests for the fast release preflight.

These pin the two failures that cost six full gate runs during the 2.6.3
release: a version surface left at the previous release, and the UI harness
dependencies never installed.
"""

from __future__ import annotations

import json
import sys
import tempfile
import unittest
from pathlib import Path
from unittest import mock

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import check_release_preflight as preflight  # noqa: E402


class VersionSurfaceTest(unittest.TestCase):
    def test_all_surfaces_agree_in_the_real_tree(self):
        self.assertEqual(
            preflight.check_version_surfaces(), preflight.declared_version()
        )

    def test_rejects_a_surface_left_at_the_previous_release(self):
        surfaces = preflight.version_surfaces()
        stale = dict(surfaces)
        stale["forward_netbox/tests/test_runtime_dependency_check.py"] = "0.0.1"
        with mock.patch.object(preflight, "version_surfaces", return_value=stale):
            with self.assertRaisesRegex(preflight.PreflightError, "0.0.1"):
                preflight.check_version_surfaces()

    def test_names_every_drifted_surface(self):
        stale = {path: "0.0.1" for path in preflight.version_surfaces()}
        with mock.patch.object(preflight, "version_surfaces", return_value=stale):
            with self.assertRaises(preflight.PreflightError) as caught:
                preflight.check_version_surfaces()
        for path in stale:
            self.assertIn(path, str(caught.exception))

    def test_covers_the_fast_baseline_runtime_pin(self):
        # The pin gates the fast baseline engine; a stale value silently
        # reverts a first sync to the slow path, so it must stay checked.
        self.assertIn(
            "forward_netbox/utilities/fast_baseline.py",
            preflight.version_surfaces(),
        )

    def test_ignores_the_neighbouring_netbox_version_pins(self):
        # min_version/max_version sit beside the plugin version in __init__.
        self.assertEqual(
            preflight.version_surfaces()["forward_netbox/__init__.py"],
            preflight.declared_version(),
        )


class UiHarnessDependencyTest(unittest.TestCase):
    def test_passes_when_dependencies_are_installed(self):
        # Built rather than read from the working tree: CI runs the harness
        # tests before `npm install`, so asserting against the real
        # node_modules would make this test depend on the environment.
        with tempfile.TemporaryDirectory() as root:
            installed = Path(root)
            for name in json.loads(preflight._read(preflight.PACKAGE_JSON))[
                "devDependencies"
            ]:
                (installed / name).mkdir(parents=True)
            with mock.patch.object(preflight, "NODE_MODULES", installed):
                self.assertIn("playwright", preflight.check_ui_harness_dependencies())

    def test_rejects_missing_dependencies(self):
        with mock.patch.object(preflight, "NODE_MODULES", Path("/nonexistent")):
            with self.assertRaisesRegex(preflight.PreflightError, "npm install"):
                preflight.check_ui_harness_dependencies()

    def test_rejects_a_manifest_declaring_no_dependencies(self):
        with mock.patch.object(preflight, "_read", return_value=json.dumps({})):
            with self.assertRaisesRegex(preflight.PreflightError, "no UI harness"):
                preflight.check_ui_harness_dependencies()


if __name__ == "__main__":
    unittest.main()
