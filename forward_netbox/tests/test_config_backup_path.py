"""The config backup folder is operator input that becomes repository structure.

Every rule that keeps a mistake from becoming a path outside the folder, a file
git refuses to hold or an overlap with the `unmanaged` folder is pinned here,
next to the accepted forms.
"""

from django.core.exceptions import ValidationError
from django.test import SimpleTestCase

from forward_netbox.utilities.config_backup_path import config_backup_path_prefix
from forward_netbox.utilities.config_backup_path import (
    DEFAULT_CONFIG_BACKUP_PATH_PREFIX,
)
from forward_netbox.utilities.config_backup_path import device_config_path_template
from forward_netbox.utilities.config_backup_path import (
    normalize_config_backup_path_prefix,
)
from forward_netbox.utilities.config_backup_path import path_segments


class NormalizeTest(SimpleTestCase):
    def test_blank_and_missing_mean_the_default(self):
        for value in (None, "", "   "):
            self.assertEqual(
                normalize_config_backup_path_prefix(value),
                DEFAULT_CONFIG_BACKUP_PATH_PREFIX,
            )

    def test_a_plain_or_nested_relative_path_is_kept(self):
        self.assertEqual(normalize_config_backup_path_prefix("configs"), "configs")
        self.assertEqual(
            normalize_config_backup_path_prefix("net/configs"), "net/configs"
        )

    def test_whitespace_and_one_trailing_slash_are_tolerated(self):
        self.assertEqual(
            normalize_config_backup_path_prefix("  net/configs/ "), "net/configs"
        )

    def test_unsafe_paths_are_refused(self):
        unsafe = {
            "/etc": "relative",
            "/": "relative",
            "a//b": "empty segment",
            ".": "`.` or `..`",
            "..": "`.` or `..`",
            "a/../b": "`.` or `..`",
            "a\\b": "not `\\`",
            "a\x00b": "control characters",
            "a\nb": "control characters",
            ".git": "`.git`",
            "x/.GIT/y": "`.git`",
            "unmanaged": "unmanaged",
            "unmanaged/inner": "unmanaged",
            "a" * 101: "at most 100",
            "a/" * 130 + "b": "at most 255",
        }
        for value, fragment in unsafe.items():
            with self.subTest(value=value[:20]):
                with self.assertRaises(ValueError) as caught:
                    normalize_config_backup_path_prefix(value)
                self.assertIn(fragment, str(caught.exception))

    def test_only_text_is_accepted(self):
        with self.assertRaises(ValueError):
            normalize_config_backup_path_prefix(5)

    def test_unmanaged_deeper_in_the_path_is_fine(self):
        self.assertEqual(
            normalize_config_backup_path_prefix("net/unmanaged"), "net/unmanaged"
        )

    def test_parameters_without_the_key_use_the_default(self):
        self.assertEqual(config_backup_path_prefix({}), "configs")
        self.assertEqual(config_backup_path_prefix(None), "configs")
        self.assertEqual(
            config_backup_path_prefix({"config_backup_path_prefix": "a/b"}), "a/b"
        )

    def test_segments_are_bytes_for_the_git_tree(self):
        self.assertEqual(path_segments("net/configs"), [b"net", b"configs"])

    def test_the_validity_template_names_the_folder(self):
        self.assertEqual(
            device_config_path_template("net/configs"),
            "net/configs/{{device.name}}.cfg",
        )


class SourceValidatorTest(SimpleTestCase):
    """The source validator is the second of the three places a key lives."""

    def _validate(self, value):
        from forward_netbox.models import ForwardSource
        from forward_netbox.utilities.model_validation import clean_forward_source

        source = ForwardSource(
            name="v",
            type="saas",
            url="https://fwd.app",
            parameters={
                "username": "u@example.com",
                "password": "secret",
                "network_id": "net-1",
                "config_backup_path_prefix": value,
            },
        )
        clean_forward_source(source)

    def test_a_valid_folder_passes(self):
        self._validate("net/configs")

    def test_an_invalid_folder_fails_with_the_reason(self):
        with self.assertRaises(ValidationError) as caught:
            self._validate("../up")
        self.assertIn("config_backup_path_prefix", str(caught.exception))
