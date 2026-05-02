import unittest

from scripts.build_netbox_feature_tag_rules import build_feature_tag_rules
from scripts.build_netbox_feature_tag_rules import DATA_ROW_FIELDS


class BuildNetBoxFeatureTagRulesTest(unittest.TestCase):
    def test_default_rules_share_stable_schema(self):
        rows = build_feature_tag_rules()

        self.assertEqual(len(rows), 1)
        self.assertTrue(all(set(row) == set(DATA_ROW_FIELDS) for row in rows))
        rule = rows[0]
        self.assertEqual(rule["record_type"], "structured_feature_tag_rule")
        self.assertEqual(rule["feature"], "bgp")
        self.assertEqual(rule["tag"], "Prot_BGP")
        self.assertEqual(rule["tag_slug"], "prot-bgp")
        self.assertTrue(rule["enabled"])

    def test_can_emit_empty_rule_list(self):
        self.assertEqual(build_feature_tag_rules(include_defaults=False), [])


if __name__ == "__main__":
    unittest.main()
