from __future__ import annotations

import os
import stat
import tempfile
import unittest
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

from scripts.development_secrets import ensure_development_secrets
from scripts.development_secrets import SECRET_NAMES


class DevelopmentSecretsTest(unittest.TestCase):
    def test_creates_each_secret_once_with_restrictive_permissions(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            secret_dir = Path(temp_dir) / "secrets"
            first = ensure_development_secrets(secret_dir)
            first_values = {
                path.name: path.read_text(encoding="utf-8") for path in first
            }
            second = ensure_development_secrets(secret_dir)

            self.assertEqual(first, second)
            self.assertEqual(
                first_values,
                {path.name: path.read_text(encoding="utf-8") for path in second},
            )
            self.assertEqual({path.name for path in first}, set(SECRET_NAMES))
            self.assertEqual(stat.S_IMODE(secret_dir.stat().st_mode), 0o700)
            self.assertTrue(
                all(stat.S_IMODE(path.stat().st_mode) == 0o600 for path in first)
            )
            self.assertTrue(
                all(path.read_text(encoding="utf-8").strip() for path in first)
            )

    def test_supplied_values_support_existing_local_runtime_migration(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            secret_dir = Path(temp_dir) / "secrets"
            values = {name: f"existing-{name}" for name in SECRET_NAMES}

            paths = ensure_development_secrets(secret_dir, values=values)

            self.assertEqual(
                {path.name: path.read_text(encoding="utf-8").strip() for path in paths},
                values,
            )

    def test_rejects_symlink_and_permissive_existing_file(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            secret_dir = Path(temp_dir) / "secrets"
            secret_dir.mkdir()
            target = Path(temp_dir) / "target"
            target.write_text("not-accepted\n", encoding="utf-8")
            (secret_dir / SECRET_NAMES[0]).symlink_to(target)

            with self.assertRaisesRegex(ValueError, "regular file"):
                ensure_development_secrets(secret_dir)

        with tempfile.TemporaryDirectory() as temp_dir:
            secret_dir = Path(temp_dir) / "secrets"
            secret_dir.mkdir()
            path = secret_dir / SECRET_NAMES[0]
            path.write_text("not-accepted\n", encoding="utf-8")
            os.chmod(path, 0o644)

            with self.assertRaisesRegex(ValueError, "0600"):
                ensure_development_secrets(secret_dir)

    def test_rejects_unknown_or_empty_supplied_values(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            with self.assertRaisesRegex(ValueError, "Unknown"):
                ensure_development_secrets(
                    Path(temp_dir) / "secrets",
                    values={"unexpected": "value"},
                )

        with tempfile.TemporaryDirectory() as temp_dir:
            with self.assertRaisesRegex(ValueError, "must not be empty"):
                ensure_development_secrets(
                    Path(temp_dir) / "secrets",
                    values={SECRET_NAMES[0]: " "},
                )

    def test_parallel_first_run_is_atomic_and_converges(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            secret_dir = Path(temp_dir) / "secrets"
            with ThreadPoolExecutor(max_workers=8) as executor:
                results = list(
                    executor.map(
                        lambda _: ensure_development_secrets(secret_dir),
                        range(16),
                    )
                )

            expected = {path.name: path.read_bytes() for path in results[0]}
            self.assertTrue(
                all(
                    {path.name: path.read_bytes() for path in result} == expected
                    for result in results
                )
            )
            self.assertEqual(
                sorted(path.name for path in secret_dir.iterdir()),
                sorted(SECRET_NAMES),
            )
