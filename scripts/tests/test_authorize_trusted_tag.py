from __future__ import annotations

import os
import sys
import unittest
from contextlib import redirect_stdout
from io import StringIO
from unittest.mock import patch

import scripts.authorize_trusted_tag as trusted_tag


class TrustedTagAuthorizationTest(unittest.TestCase):
    commit = "a" * 40
    environment = {
        "GITHUB_REPOSITORY": trusted_tag.GITHUB_REPOSITORY,
        "GITHUB_REF": "refs/heads/main",
        "GITHUB_SHA": commit,
    }

    def test_authorizes_reviewed_bootstrap_anchor(self):
        evidence = {"pull_request": 61}
        with (
            patch.dict(os.environ, self.environment, clear=True),
            patch.object(trusted_tag, "_git_capture", return_value=self.commit),
            patch.object(
                trusted_tag,
                "verify_trusted_anchor_candidate",
                return_value=evidence,
            ) as verify,
        ):
            result = trusted_tag.authorize_trusted_tag(
                trusted_tag.TRUSTED_ANCHOR_TAG,
                self.commit,
                "brandonheller",
                "token",
            )

        verify.assert_called_once_with(self.commit, "brandonheller", "token")
        self.assertEqual(result["kind"], "anchor")
        self.assertEqual(result["target"], self.commit)

    def test_authorizes_reviewed_release_commit(self):
        evidence = {"production_commit": "b" * 40}
        with (
            patch.dict(os.environ, self.environment, clear=True),
            patch.object(trusted_tag, "_git_capture", return_value=self.commit),
            patch.object(trusted_tag, "_package_version", return_value="2.6.0"),
            patch.object(
                trusted_tag,
                "verify_release_commit_provenance",
                return_value=evidence,
            ) as verify,
        ):
            result = trusted_tag.authorize_trusted_tag(
                "v2.6.0",
                self.commit,
                "brandonheller",
                "token",
            )

        verify.assert_called_once_with(
            self.commit,
            "2.6.0",
            "brandonheller",
            "token",
        )
        self.assertEqual(result["kind"], "release")

    def test_rejects_non_main_dispatch(self):
        environment = {**self.environment, "GITHUB_REF": "refs/heads/feature"}
        with patch.dict(os.environ, environment, clear=True):
            with self.assertRaisesRegex(trusted_tag.TrustedTagError, "protected main"):
                trusted_tag.authorize_trusted_tag(
                    "v2.6.0",
                    self.commit,
                    "brandonheller",
                    "token",
                )

    def test_rejects_version_mismatch(self):
        with (
            patch.dict(os.environ, self.environment, clear=True),
            patch.object(trusted_tag, "_git_capture", return_value=self.commit),
            patch.object(trusted_tag, "_package_version", return_value="2.5.11"),
        ):
            with self.assertRaisesRegex(trusted_tag.TrustedTagError, "package version"):
                trusted_tag.authorize_trusted_tag(
                    "v2.6.0",
                    self.commit,
                    "brandonheller",
                    "token",
                )

    def test_rejects_abbreviated_or_non_hex_sha(self):
        with patch.dict(os.environ, self.environment, clear=True):
            for invalid in ("a" * 39, "G" * 40):
                with self.subTest(invalid=invalid):
                    with self.assertRaisesRegex(
                        trusted_tag.TrustedTagError,
                        "full lowercase",
                    ):
                        trusted_tag.authorize_trusted_tag(
                            "v2.6.0",
                            invalid,
                            "brandonheller",
                            "token",
                        )

    def test_main_does_not_log_authorization_evidence_or_token(self):
        secret = "secret-from-api-response"
        output = StringIO()
        argv = [
            "authorize_trusted_tag.py",
            "--tag",
            "v2.6.0",
            "--expected-sha",
            self.commit,
            "--reviewer",
            "brandonheller",
        ]
        with (
            patch.dict(os.environ, {"GH_TOKEN": secret}, clear=True),
            patch.object(sys, "argv", argv),
            patch.object(
                trusted_tag,
                "authorize_trusted_tag",
                return_value={"untrusted_evidence": secret},
            ),
            redirect_stdout(output),
        ):
            self.assertEqual(trusted_tag.main(), 0)

        self.assertEqual(output.getvalue(), "Trusted tag authorization passed.\n")
        self.assertNotIn(secret, output.getvalue())


if __name__ == "__main__":
    unittest.main()
