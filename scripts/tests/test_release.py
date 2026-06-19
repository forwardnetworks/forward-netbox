from __future__ import annotations

import importlib.util
import unittest
from pathlib import Path

_SPEC = importlib.util.spec_from_file_location(
    "release_tool", Path(__file__).resolve().parents[1] / "release.py"
)
release = importlib.util.module_from_spec(_SPEC)
_SPEC.loader.exec_module(release)


class BumpVersionTest(unittest.TestCase):
    def test_bumps_single_assignment(self):
        out = release.bump_version_text(
            'name = "x"\nversion = "1.5.10"\n', "1.5.10", "1.5.11", key="version"
        )
        self.assertIn('version = "1.5.11"', out)

    def test_raises_when_old_version_absent(self):
        with self.assertRaises(release.ReleaseError):
            release.bump_version_text(
                'version = "9.9.9"', "1.5.10", "1.5.11", key="version"
            )


class InsertReleaseRowTest(unittest.TestCase):
    TABLE = (
        "| Plugin Release | NetBox Version | Status |\n"
        "| --- | --- | --- |\n"
        "| `v1.5.10` | `4.5.9` and `4.6.2` validated | Current release; did a thing. |\n"
        "| `v1.5.9` | `4.5.9` and `4.6.2` validated | Superseded by `v1.5.10`; older. |\n"
    )

    def test_inserts_new_current_row_and_demotes_prior(self):
        out = release.insert_release_row(self.TABLE, "1.5.11", "new feature.")
        lines = out.splitlines()
        # New current row first, reusing the support cell.
        self.assertIn("| `v1.5.11` |", lines[2])
        self.assertIn("Current release; new feature.", lines[2])
        self.assertIn("`4.5.9` and `4.6.2` validated", lines[2])
        # Prior row demoted.
        self.assertIn("| `v1.5.10` |", lines[3])
        self.assertIn("Superseded by `v1.5.11`;", lines[3])
        # Only one "Current release;" remains.
        self.assertEqual(out.count("Current release;"), 1)

    def test_raises_without_current_row(self):
        with self.assertRaises(release.ReleaseError):
            release.insert_release_row("no current row here", "1.5.11", "x")


class SemverArgTest(unittest.TestCase):
    def test_semver_regex(self):
        self.assertIsNotNone(release.SEMVER_RE.match("1.5.11"))
        self.assertIsNone(release.SEMVER_RE.match("1.5"))
        self.assertIsNone(release.SEMVER_RE.match("v1.5.11"))


if __name__ == "__main__":
    unittest.main()
