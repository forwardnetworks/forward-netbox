import tempfile
import unittest
from pathlib import Path
from unittest import mock

from scripts.build_netbox_device_type_aliases import build_alias_rows
from scripts.build_netbox_device_type_aliases import DATA_ROW_FIELDS
from scripts.build_netbox_device_type_aliases import slugify_netbox_model


class BuildNetBoxDeviceTypeAliasesTest(unittest.TestCase):
    def test_slugify_matches_forward_netbox_model_slug_semantics(self):
        self.assertEqual(slugify_netbox_model("ISR4331/K9"), "isr4331-slash-k9")
        self.assertEqual(slugify_netbox_model("WS-C4510R+E"), "ws-c4510r-plus-e")
        self.assertEqual(slugify_netbox_model("PA.5450"), "pa-dot-5450")

    def test_part_number_aliases_do_not_override_exact_model_aliases(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            vendor_dir = root / "device-types" / "Cisco"
            vendor_dir.mkdir(parents=True)
            (vendor_dir / "CBS220.yaml").write_text(
                "\n".join(
                    [
                        "manufacturer: Cisco",
                        "model: CBS220-24T-4G",
                        "slug: cisco-cbs220-24t-4g",
                        "part_number: CBS250-24T-4G",
                    ]
                ),
                encoding="utf-8",
            )
            (vendor_dir / "CBS250.yaml").write_text(
                "\n".join(
                    [
                        "manufacturer: Cisco",
                        "model: CBS250-24T-4G",
                        "slug: cisco-cbs250-24t-4g",
                        "part_number: CBS250-24T-4G",
                    ]
                ),
                encoding="utf-8",
            )

            with mock.patch(
                "scripts.build_netbox_device_type_aliases.git_commit",
                return_value="test-commit",
            ):
                rows, conflicts = build_alias_rows(root)

        match = next(
            row
            for row in rows
            if row["forward_manufacturer_slug"] == "cisco"
            and row["forward_model_slug"] == "cbs250-24t-4g"
        )
        self.assertEqual(match["netbox_slug"], "cisco-cbs250-24t-4g")
        self.assertEqual(match["match_source"], "model")
        self.assertEqual(match["record_type"], "device_type_alias")
        self.assertEqual(len(conflicts), 1)
        self.assertEqual(conflicts[0]["type"], "alias_conflict_skipped")

    def test_generated_rows_share_a_stable_data_file_schema(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            vendor_dir = root / "device-types" / "Cisco"
            vendor_dir.mkdir(parents=True)
            (vendor_dir / "ISR4331.yaml").write_text(
                "\n".join(
                    [
                        "manufacturer: Cisco",
                        "model: ISR4331/K9",
                        "slug: cisco-isr4331-k9",
                    ]
                ),
                encoding="utf-8",
            )

            with mock.patch(
                "scripts.build_netbox_device_type_aliases.git_commit",
                return_value="test-commit",
            ):
                rows, conflicts = build_alias_rows(root)

        self.assertEqual(conflicts, [])
        self.assertTrue(rows)
        self.assertTrue(all(set(row) == set(DATA_ROW_FIELDS) for row in rows))
        override = next(
            row
            for row in rows
            if row["record_type"] == "manufacturer_override"
            and row["forward_vendor"] == "Vendor.CISCO"
        )
        self.assertEqual(override["forward_vendor"], "Vendor.CISCO")
        self.assertEqual(override["manufacturer"], "Cisco")
        alias = next(row for row in rows if row["record_type"] == "device_type_alias")
        self.assertEqual(alias["forward_model_slug"], "isr4331-slash-k9")


if __name__ == "__main__":
    unittest.main()
