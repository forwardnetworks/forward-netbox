from django.test import TestCase
from django.utils import timezone

from forward_netbox.choices import ForwardSyncStatusChoices
from forward_netbox.models import ForwardSource
from forward_netbox.models import ForwardSync
from forward_netbox.utilities.forward_api import LATEST_PROCESSED_SNAPSHOT
from forward_netbox.utilities.sync_state import get_display_parameters
from forward_netbox.utilities.sync_state import set_model_change_density
from forward_netbox.utilities.sync_state import set_model_change_density_profile


class ForwardSyncStateHelperTest(TestCase):
    def setUp(self):
        self.source = ForwardSource.objects.create(
            name="source-sync-state",
            type="saas",
            url="https://fwd.app",
            parameters={
                "username": "user@example.com",
                "password": "secret",
                "verify": True,
                "timeout": 1200,
                "network_id": "test-network",
            },
        )
        self.sync = ForwardSync.objects.create(
            name="sync-sync-state",
            source=self.source,
            parameters={
                "snapshot_id": LATEST_PROCESSED_SNAPSHOT,
                "dcim.device": True,
            },
        )

    def test_display_parameters_include_density_and_branch_hints(self):
        set_model_change_density(self.sync, {"dcim.device": 2.0})
        set_model_change_density_profile(
            self.sync,
            {
                "dcim.device": {
                    "density": 2.0,
                    "sample_count": 4,
                    "accepted_observations": 4,
                    "rejected_observations": 1,
                    "mean": 2.1,
                    "m2": 0.2,
                    "variance": 0.066666,
                    "stddev": 0.258198,
                    "last_updated_at": timezone.now().isoformat(),
                }
            },
        )

        params = get_display_parameters(
            self.sync,
            max_changes_per_staging_item_default=10000,
        )

        self.assertEqual(params["model_change_density"]["dcim.device"], 2.0)
        self.assertEqual(
            params["model_change_density_profile"]["model_count"],
            1,
        )
        self.assertIn("branch_budget_hints", params)
        self.assertEqual(params["branch_budget_hints"]["dcim.device"], 4666)
        self.assertEqual(
            params["branch_budget_density_policy"]["dcim.device"]["policy"],
            "medium_confidence_blended_density",
        )

    def test_ready_to_merge_is_not_ready_for_another_sync(self):
        self.sync.status = ForwardSyncStatusChoices.READY_TO_MERGE
        self.assertFalse(self.sync.ready_for_sync)
