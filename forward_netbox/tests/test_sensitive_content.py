from pathlib import Path
from tempfile import TemporaryDirectory
from unittest import TestCase

from forward_netbox.utilities.sensitive_content import load_sensitive_patterns
from forward_netbox.utilities.sensitive_content import LOCAL_PATTERN_FILE
from forward_netbox.utilities.sensitive_content import scan_text


class SensitiveContentTest(TestCase):
    def test_builtin_patterns_flag_live_forward_identifiers(self):
        patterns = load_sensitive_patterns(Path.cwd())
        network_id = "".join(["54", "321"])
        snapshot_id = "".join(["98", "765"])
        plus_alias = "".join(["operator", "+tenant", "@forwardnetworks.com"])

        findings = scan_text(
            "\n".join(
                [
                    f"network-id: {network_id}",
                    f"snapshot {snapshot_id}",
                    plus_alias,
                ]
            ),
            source="sample.txt",
            patterns=patterns,
        )

        self.assertEqual(len(findings), 3)
        self.assertEqual(
            [finding.line_number for finding in findings],
            [1, 2, 3],
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

        findings = scan_text(
            "\n".join(
                [
                    f'"network_id": "{network_id}"',
                    f"snapshot_id='{snapshot_id}'",
                ]
            ),
            source="test_fixture.py",
            patterns=patterns,
        )

        self.assertEqual(len(findings), 2)

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
