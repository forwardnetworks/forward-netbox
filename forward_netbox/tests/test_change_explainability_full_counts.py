"""The change explainer's counts must cover every change, not the sample.

A run of ~96k changes reported a model mix drawn from the first 5,000 by pk, so
whichever model staged first looked like the whole run and the model holding the
rest was invisible. The field detail may stay sampled; the counts may not.
"""

from core.choices import ObjectChangeActionChoices
from core.models import ObjectType
from django.test import TestCase
from netbox_branching.models import Branch
from netbox_branching.models import ChangeDiff

from forward_netbox.models import ForwardIngestion
from forward_netbox.models import ForwardSource
from forward_netbox.models import ForwardSync
from forward_netbox.utilities.change_explainability import change_explainability_summary


class FullCountsTest(TestCase):
    def setUp(self):
        source = ForwardSource.objects.create(
            name="cx-source",
            type="saas",
            url="https://forward.example.com",
            status="ready",
            parameters={"network_id": "net-1"},
        )
        sync = ForwardSync.objects.create(name="cx-sync", source=source, parameters={})
        self.branch = Branch.objects.create(name="cx-branch", schema_id="cx_branch")
        self.ingestion = ForwardIngestion.objects.create(sync=sync, branch=self.branch)

    def _change(self, app_label, model, action, n):
        object_type = ObjectType.objects.get(app_label=app_label, model=model)
        for index in range(n):
            ChangeDiff.objects.create(
                branch=self.branch,
                object_type=object_type,
                object_id=index + 1,
                object_repr=f"{model}-{index}",
                action=action,
                original={},
                modified={},
                current={},
                conflicts=[],
            )

    def test_counts_cover_every_change_when_the_sample_is_smaller(self):
        # 5 prefix creates first (so a 3-row sample would see only prefixes),
        # then 4 vlan creates and 2 vlan updates.
        self._change("ipam", "prefix", ObjectChangeActionChoices.ACTION_CREATE, 5)
        self._change("ipam", "vlan", ObjectChangeActionChoices.ACTION_CREATE, 4)
        self._change("ipam", "vlan", ObjectChangeActionChoices.ACTION_UPDATE, 2)

        summary = change_explainability_summary(self.ingestion, max_changes=3)

        self.assertEqual(summary["total_change_count"], 11)
        self.assertEqual(summary["sampled_change_count"], 3)
        self.assertTrue(summary["truncated"])
        self.assertTrue(summary["counts_cover_all_changes"])
        self.assertEqual(summary["model_counts"], {"ipam.prefix": 5, "ipam.vlan": 6})
        self.assertEqual(
            summary["action_counts"],
            {
                ObjectChangeActionChoices.ACTION_CREATE: 9,
                ObjectChangeActionChoices.ACTION_UPDATE: 2,
            },
        )
        self.assertEqual(
            summary["model_action_counts"]["ipam.vlan"],
            {
                ObjectChangeActionChoices.ACTION_CREATE: 4,
                ObjectChangeActionChoices.ACTION_UPDATE: 2,
            },
        )

    def test_counts_add_up_to_the_total(self):
        self._change("ipam", "prefix", ObjectChangeActionChoices.ACTION_CREATE, 3)
        self._change("ipam", "vlan", ObjectChangeActionChoices.ACTION_UPDATE, 2)

        summary = change_explainability_summary(self.ingestion, max_changes=1)

        self.assertEqual(sum(summary["model_counts"].values()), 5)
        self.assertEqual(sum(summary["action_counts"].values()), 5)
        self.assertEqual(summary["total_change_count"], 5)
