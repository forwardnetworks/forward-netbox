from __future__ import annotations

import importlib.util
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch


_SPEC = importlib.util.spec_from_file_location(
    "reproducible_distribution",
    Path(__file__).resolve().parents[1] / "build_reproducible_distribution.py",
)
distribution = importlib.util.module_from_spec(_SPEC)
_SPEC.loader.exec_module(distribution)


class ReproducibleDistributionTest(unittest.TestCase):
    def test_copies_only_matching_independent_builds(self):
        with tempfile.TemporaryDirectory() as directory:
            output = Path(directory) / "dist"

            def build_once(build_dir, *, source_date_epoch):
                self.assertEqual(source_date_epoch, "1234")
                (build_dir / "package.whl").write_bytes(b"identical")
                return {"package.whl": "digest"}

            with patch.object(
                distribution, "_source_date_epoch", return_value="1234"
            ), patch.object(distribution, "_build_once", side_effect=build_once):
                result = distribution.build_reproducible_distribution(output)

            self.assertEqual(result, {"package.whl": "digest"})
            self.assertEqual((output / "package.whl").read_bytes(), b"identical")

    def test_rejects_digest_mismatch_without_replacing_output(self):
        with tempfile.TemporaryDirectory() as directory:
            output = Path(directory) / "dist"
            output.mkdir()
            (output / "existing.whl").write_bytes(b"keep")
            builds = iter(
                [
                    {"package.whl": "first"},
                    {"package.whl": "second"},
                ]
            )
            with patch.object(
                distribution, "_source_date_epoch", return_value="1234"
            ), patch.object(
                distribution,
                "_build_once",
                side_effect=lambda *_args, **_kwargs: next(builds),
            ), self.assertRaisesRegex(
                distribution.ReproducibleBuildError, "different SHA-256"
            ):
                distribution.build_reproducible_distribution(output)

            self.assertEqual((output / "existing.whl").read_bytes(), b"keep")


if __name__ == "__main__":
    unittest.main()
