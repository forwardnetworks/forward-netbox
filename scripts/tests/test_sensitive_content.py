import os
import subprocess
import sys
from hashlib import sha256
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest import mock
from unittest import TestCase

from scripts.sensitive_content import BINARY_ALLOWLIST_FILE
from scripts.sensitive_content import ENV_PATTERN_VAR
from scripts.sensitive_content import format_finding
from scripts.sensitive_content import HISTORY_BASELINE_ENV_VAR
from scripts.sensitive_content import HISTORY_BASELINE_FILE
from scripts.sensitive_content import load_binary_allowlist
from scripts.sensitive_content import load_protected_history_baseline
from scripts.sensitive_content import load_sensitive_patterns
from scripts.sensitive_content import LOCAL_PATTERN_FILE
from scripts.sensitive_content import protected_history_range
from scripts.sensitive_content import require_environment_patterns
from scripts.sensitive_content import scan_commit_history
from scripts.sensitive_content import scan_git_tree
from scripts.sensitive_content import scan_name
from scripts.sensitive_content import scan_paths
from scripts.sensitive_content import scan_text


class SensitiveContentTest(TestCase):
    @staticmethod
    def _git(repo_root: Path, *args: str) -> str:
        return subprocess.run(
            ["git", *args],
            cwd=repo_root,
            check=True,
            capture_output=True,
            text=True,
        ).stdout.strip()

    def _init_repo(self, repo_root: Path) -> str:
        self._git(repo_root, "init", "-q")
        self._git(repo_root, "config", "user.name", "Scanner Test")
        self._git(repo_root, "config", "user.email", "scanner@example.invalid")
        (repo_root / BINARY_ALLOWLIST_FILE).write_text("", encoding="utf-8")
        (repo_root / "safe.txt").write_text("safe\n", encoding="utf-8")
        self._git(repo_root, "add", ".")
        self._git(repo_root, "commit", "-qm", "baseline")
        return self._git(repo_root, "rev-parse", "HEAD")

    def _commit_all(self, repo_root: Path, message: str) -> str:
        self._git(repo_root, "add", "-A")
        self._git(repo_root, "commit", "-qm", message)
        return self._git(repo_root, "rev-parse", "HEAD")

    def test_protected_history_baseline_is_full_hash_and_ancestor(self):
        baseline = load_protected_history_baseline(Path.cwd())

        self.assertRegex(baseline, r"^[0-9a-f]{40}$")
        self.assertEqual(protected_history_range(Path.cwd()), f"{baseline}..HEAD")

    def test_protected_history_baseline_rejects_missing_or_abbreviated_hash(self):
        with TemporaryDirectory() as temp_dir:
            repo_root = Path(temp_dir)
            with self.assertRaisesRegex(ValueError, "Missing"):
                load_protected_history_baseline(repo_root)
            (repo_root / HISTORY_BASELINE_FILE).write_text(
                "df85f2e\n", encoding="utf-8"
            )
            with self.assertRaisesRegex(ValueError, "full lowercase commit hash"):
                load_protected_history_baseline(repo_root)

    def test_protected_history_requires_external_baseline_match(self):
        baseline = load_protected_history_baseline(Path.cwd())
        with mock.patch.dict(
            os.environ,
            {HISTORY_BASELINE_ENV_VAR: "a" * 40},
        ):
            with self.assertRaisesRegex(ValueError, "externally approved"):
                protected_history_range(
                    Path.cwd(),
                    require_trusted_baseline=True,
                )

        with mock.patch.dict(
            os.environ,
            {HISTORY_BASELINE_ENV_VAR: baseline},
        ):
            self.assertEqual(
                protected_history_range(
                    Path.cwd(),
                    require_trusted_baseline=True,
                ),
                f"{baseline}..HEAD",
            )

    def test_builtin_patterns_flag_live_forward_identifiers(self):
        patterns = load_sensitive_patterns(Path.cwd())
        network_id = "".join(["54", "321"])
        snapshot_id = "".join(["98", "765"])
        org_id = "".join(["12", "345"])
        query_id = "Q_" + ("a" * 40)
        plus_alias = "".join(["operator", "+tenant", "@forwardnetworks.com"])

        findings = scan_text(
            "\n".join(
                [
                    f"network-id: {network_id}",
                    f"organization_id: {org_id}",
                    f"snapshot {snapshot_id}",
                    f"query-id: {query_id}",
                    plus_alias,
                ]
            ),
            source="sample.txt",
            patterns=patterns,
        )

        self.assertEqual(len(findings), 5)
        self.assertEqual(
            [finding.line_number for finding in findings],
            [1, 2, 3, 4, 5],
        )

    def test_builtin_patterns_ignore_generic_api_field_names(self):
        patterns = load_sensitive_patterns(Path.cwd())

        findings = scan_text(
            "\n".join(
                [
                    'req_params["networkId"] = network_id',
                    'req_params["snapshotId"] = snapshot_id',
                ]
            ),
            source="forward_api.py",
            patterns=patterns,
        )

        self.assertEqual(findings, [])

    def test_builtin_patterns_flag_quoted_identifier_values(self):
        patterns = load_sensitive_patterns(Path.cwd())
        network_id = "".join(["54", "321"])
        snapshot_id = "".join(["98", "765"])
        org_id = "".join(["12", "345"])

        findings = scan_text(
            "\n".join(
                [
                    f'"network_id": "{network_id}"',
                    f'"organization_id": "{org_id}"',
                    f"snapshot_id='{snapshot_id}'",
                ]
            ),
            source="test_fixture.py",
            patterns=patterns,
        )

        self.assertEqual(len(findings), 3)

    def test_builtin_patterns_flag_markdown_and_parenthesized_identifiers(self):
        patterns = load_sensitive_patterns(Path.cwd())
        network_id = "".join(["54", "321"])
        snapshot_id = "".join(["98", "765"])
        org_id = "".join(["12", "345"])

        findings = scan_text(
            "\n".join(
                [
                    f"Forward SaaS network `{network_id}`",
                    f"validation org network ({network_id})",
                    f"validation org ({org_id})",
                    f"processed snapshot `{snapshot_id}`",
                    f"snapshot [{snapshot_id}]",
                ]
            ),
            source="release-evidence.md",
            patterns=patterns,
        )

        self.assertEqual(len(findings), 5)

    def test_local_pattern_file_blocks_customer_name_literals_and_regexes(self):
        with TemporaryDirectory() as temp_dir:
            repo_root = Path(temp_dir)
            (repo_root / LOCAL_PATTERN_FILE).write_text(
                "\n".join(
                    [
                        "# local-only values",
                        "Acme Corp",
                        "re:tenant-[a-z]+",
                    ]
                ),
                encoding="utf-8",
            )

            patterns = load_sensitive_patterns(repo_root)
            findings = scan_text(
                "Acme Corp\nTenant-alpha\n",
                source="notes.txt",
                patterns=patterns,
            )

        self.assertEqual(len(findings), 2)
        self.assertTrue(
            findings[0].label.startswith("local literal")
            or findings[1].label.startswith("local literal")
        )
        self.assertTrue(
            findings[0].label.startswith("local regex")
            or findings[1].label.startswith("local regex")
        )

    def test_env_var_patterns_block_customer_names_without_a_local_file(self):
        # CI feeds customer identifiers via the secret-backed env var so they are
        # caught WITHOUT committing them to the repo (the gitignored local file is
        # invisible to CI — how a customer name once slipped through). This must
        # work even when no local pattern file exists.
        with TemporaryDirectory() as temp_dir:
            repo_root = Path(temp_dir)  # no LOCAL_PATTERN_FILE present
            with mock.patch.dict(
                "os.environ",
                {ENV_PATTERN_VAR: "\n".join(["Acme Corp", "re:tenant-[a-z]+"])},
            ):
                patterns = load_sensitive_patterns(repo_root)
            findings = scan_text(
                "acme corp\nTenant-beta\n",
                source="notes.txt",
                patterns=patterns,
            )

        self.assertEqual(len(findings), 2)
        self.assertTrue(all(f.label.startswith("env ") for f in findings))

    def test_release_requires_nonempty_environment_patterns(self):
        with mock.patch.dict(os.environ, {ENV_PATTERN_VAR: "# comments only"}):
            with self.assertRaisesRegex(ValueError, "at least one"):
                require_environment_patterns()

        with mock.patch.dict(os.environ, {ENV_PATTERN_VAR: "Example Customer"}):
            require_environment_patterns()

    def test_finding_output_never_includes_detected_value_or_line(self):
        patterns = load_sensitive_patterns(Path.cwd())
        private_value = "network " + "54321"
        finding = scan_text(
            f"prefix {private_value} suffix",
            source="sample.txt",
            patterns=patterns,
        )[0]

        rendered = format_finding(finding)

        self.assertNotIn(private_value, rendered)
        self.assertNotIn("prefix", rendered)
        self.assertEqual(
            rendered,
            "sample.txt:1: Forward network identifier",
        )

    def test_unreviewed_binary_fails_closed_and_exact_hash_is_allowed(self):
        with TemporaryDirectory() as temp_dir:
            repo_root = Path(temp_dir)
            binary_path = repo_root / "artifact.bin"
            binary_data = b"\x00private"
            binary_path.write_bytes(binary_data)
            (repo_root / BINARY_ALLOWLIST_FILE).write_text("", encoding="utf-8")
            patterns = load_sensitive_patterns(repo_root)

            findings = scan_paths(
                [binary_path],
                repo_root=repo_root,
                patterns=patterns,
                binary_allowlist={},
            )
            self.assertEqual(len(findings), 1)
            self.assertIn("unreviewed binary", findings[0].label)

            digest = sha256(binary_data).hexdigest()
            self.assertEqual(
                scan_paths(
                    [binary_path],
                    repo_root=repo_root,
                    patterns=patterns,
                    binary_allowlist={"artifact.bin": digest},
                ),
                [],
            )

    def test_history_scan_catches_content_added_then_deleted(self):
        with TemporaryDirectory() as temp_dir:
            repo_root = Path(temp_dir)
            baseline = self._init_repo(repo_root)
            private_value = "network " + "54321"
            secret_path = repo_root / "temporary.txt"
            secret_path.write_text(private_value + "\n", encoding="utf-8")
            self._commit_all(repo_root, "add temporary evidence")
            secret_path.unlink()
            self._commit_all(repo_root, "remove temporary evidence")

            findings = scan_commit_history(
                repo_root=repo_root,
                patterns=load_sensitive_patterns(repo_root),
                rev_args=[f"{baseline}..HEAD"],
            )

        self.assertTrue(
            any(
                finding.source.endswith(":temporary.txt")
                and finding.label == "Forward network identifier"
                for finding in findings
            )
        )

    def test_history_scan_catches_annotated_tag_messages(self):
        with TemporaryDirectory() as temp_dir:
            repo_root = Path(temp_dir)
            baseline = self._init_repo(repo_root)
            (repo_root / "safe.txt").write_text("still safe\n", encoding="utf-8")
            self._commit_all(repo_root, "safe change")
            private_value = "network " + "54321"
            self._git(
                repo_root,
                "tag",
                "-a",
                "v-test",
                baseline,
                "-m",
                private_value,
            )

            findings = scan_commit_history(
                repo_root=repo_root,
                patterns=load_sensitive_patterns(repo_root),
                rev_args=[f"{baseline}..HEAD"],
            )

        self.assertTrue(
            any(finding.source.startswith("tag-object:") for finding in findings)
        )

    def test_history_scan_recurses_through_nested_annotated_tags(self):
        with TemporaryDirectory() as temp_dir:
            repo_root = Path(temp_dir)
            baseline = self._init_repo(repo_root)
            private_value = "organization " + "54321"
            self._git(
                repo_root,
                "tag",
                "-a",
                "inner-tag",
                baseline,
                "-m",
                private_value,
            )
            self._git(
                repo_root,
                "tag",
                "-a",
                "outer-tag",
                "inner-tag",
                "-m",
                "safe outer message",
            )
            self._git(repo_root, "tag", "-d", "inner-tag")

            findings = scan_commit_history(
                repo_root=repo_root,
                patterns=load_sensitive_patterns(repo_root),
                rev_args=[f"{baseline}..HEAD"],
            )

        self.assertTrue(
            any(
                finding.label == "Forward organization identifier"
                and finding.source.startswith("tag-object:")
                for finding in findings
            )
        )

    def test_history_scan_catches_sensitive_blob_through_merge_graph(self):
        with TemporaryDirectory() as temp_dir:
            repo_root = Path(temp_dir)
            baseline = self._init_repo(repo_root)
            main_branch = self._git(repo_root, "branch", "--show-current")
            self._git(repo_root, "checkout", "-qb", "feature")
            private_value = "snapshot " + "98765"
            (repo_root / "merged.txt").write_text(
                private_value + "\n",
                encoding="utf-8",
            )
            self._commit_all(repo_root, "feature evidence")
            self._git(repo_root, "checkout", "-q", main_branch)
            (repo_root / "safe.txt").write_text("main change\n", encoding="utf-8")
            self._commit_all(repo_root, "main change")
            self._git(repo_root, "merge", "--no-ff", "-qm", "merge feature", "feature")

            findings = scan_commit_history(
                repo_root=repo_root,
                patterns=load_sensitive_patterns(repo_root),
                rev_args=[f"{baseline}..HEAD"],
            )

        self.assertTrue(
            any(
                finding.source.endswith(":merged.txt")
                and finding.label == "Forward snapshot identifier"
                for finding in findings
            )
        )

    def test_shallow_history_fails_when_baseline_commit_is_unavailable(self):
        with TemporaryDirectory() as temp_dir:
            source_root = Path(temp_dir) / "source"
            clone_root = Path(temp_dir) / "clone"
            source_root.mkdir()
            baseline = self._init_repo(source_root)
            (source_root / HISTORY_BASELINE_FILE).write_text(
                baseline + "\n",
                encoding="utf-8",
            )
            self._commit_all(source_root, "configure baseline")
            subprocess.run(
                [
                    "git",
                    "clone",
                    "-q",
                    "--depth",
                    "1",
                    source_root.as_uri(),
                    str(clone_root),
                ],
                check=True,
            )

            with self.assertRaisesRegex(ValueError, "full-history checkout"):
                protected_history_range(clone_root)

    def test_cli_fails_when_release_pattern_feed_is_missing(self):
        env = os.environ.copy()
        env.pop(ENV_PATTERN_VAR, None)

        result = subprocess.run(
            [
                sys.executable,
                "scripts/check_sensitive_content.py",
                "--git-files",
                "--require-env-patterns",
            ],
            cwd=Path.cwd(),
            env=env,
            capture_output=True,
            text=True,
        )

        self.assertEqual(result.returncode, 1)
        self.assertIn("must contain at least one", result.stdout)

    def test_stale_binary_allowlist_entry_is_rejected(self):
        with TemporaryDirectory() as temp_dir:
            repo_root = Path(temp_dir)
            self._init_repo(repo_root)
            data = b"\x00historical"
            artifact = repo_root / "historical.bin"
            artifact.write_bytes(data)
            self._commit_all(repo_root, "add binary")
            artifact.unlink()
            (repo_root / BINARY_ALLOWLIST_FILE).write_text(
                f"{sha256(data).hexdigest()} historical.bin\n",
                encoding="utf-8",
            )
            self._commit_all(repo_root, "delete binary")

            with self.assertRaisesRegex(ValueError, "without a current tracked file"):
                load_binary_allowlist(repo_root)

    def test_sensitive_path_and_ref_names_use_opaque_locations(self):
        with TemporaryDirectory() as temp_dir:
            repo_root = Path(temp_dir)
            baseline = self._init_repo(repo_root)
            private_path = "network-" + "54321.txt"
            path = repo_root / private_path
            path.write_text("safe content\n", encoding="utf-8")
            patterns = load_sensitive_patterns(repo_root)

            findings = scan_paths(
                [path],
                repo_root=repo_root,
                patterns=patterns,
                binary_allowlist={},
            )
            self._git(repo_root, "branch", private_path.removesuffix(".txt"))
            findings.extend(
                scan_commit_history(
                    repo_root=repo_root,
                    patterns=patterns,
                    rev_args=[f"{baseline}..HEAD"],
                )
            )

        self.assertGreaterEqual(len(findings), 2)
        for finding in findings:
            rendered = format_finding(finding)
            self.assertNotIn("54321", rendered)
            self.assertIn("sha256:", rendered)

    def test_candidate_tree_requires_external_baseline_and_base_allowlist(self):
        with TemporaryDirectory() as temp_dir:
            repo_root = Path(temp_dir)
            baseline = self._init_repo(repo_root)
            (repo_root / HISTORY_BASELINE_FILE).write_text(
                baseline + "\n", encoding="utf-8"
            )
            trusted_head = self._commit_all(repo_root, "trusted controls")
            (repo_root / HISTORY_BASELINE_FILE).write_text(
                ("a" * 40) + "\n", encoding="utf-8"
            )
            candidate_head = self._commit_all(repo_root, "change baseline")

            with mock.patch.dict(
                os.environ,
                {HISTORY_BASELINE_ENV_VAR: baseline},
            ):
                self._git(repo_root, "checkout", "-q", trusted_head)
                with self.assertRaisesRegex(ValueError, "external trust anchor"):
                    scan_git_tree(
                        repo_root=repo_root,
                        revision=candidate_head,
                        patterns=load_sensitive_patterns(repo_root),
                        require_trusted_controls=True,
                    )

    def test_explicit_ref_scan_redacts_control_characters(self):
        patterns = load_sensitive_patterns(Path.cwd())
        value = "network-" + "54321\nterminal"

        findings = scan_name(value, kind="ref", patterns=patterns)

        self.assertEqual(len(findings), 1)
        rendered = format_finding(findings[0])
        self.assertNotIn("54321", rendered)
        self.assertNotIn("\n", rendered)

    def test_binary_allowlist_rejects_path_traversal_and_duplicate_entries(self):
        digest = "a" * 64
        with TemporaryDirectory() as temp_dir:
            repo_root = Path(temp_dir)
            allowlist = repo_root / BINARY_ALLOWLIST_FILE
            allowlist.write_text(f"{digest} ../escape.bin\n", encoding="utf-8")
            with self.assertRaisesRegex(ValueError, "invalid or duplicate"):
                load_binary_allowlist(repo_root)

            allowlist.write_text(
                f"{digest} artifact.bin\n{digest} artifact.bin\n",
                encoding="utf-8",
            )
            with self.assertRaisesRegex(ValueError, "invalid or duplicate"):
                load_binary_allowlist(repo_root)
