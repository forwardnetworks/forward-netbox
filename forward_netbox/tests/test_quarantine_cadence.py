# The absence quarantine's defaults - three runs AND 72 hours - were a guess at
# one deployment's cadence, recorded as such in three release plans. Nothing
# showed an operator what they add up to on their own interval. This does.
from django.test import TestCase

from forward_netbox.models import ForwardSource
from forward_netbox.models import ForwardSync
from forward_netbox.utilities.health import sync_health_summary


class QuarantineCadenceTest(TestCase):
    def _sync(self, *, interval=None, **parameters):
        source = ForwardSource.objects.create(
            name=f"cadence-src-{interval}",
            type="saas",
            url="https://fwd.app",
            status="ready",
            parameters={"network_id": "net-1", **parameters},
        )
        return ForwardSync.objects.create(
            name=f"cadence-sync-{interval}", source=source, interval=interval
        )

    def test_an_hourly_sync_is_bound_by_the_hours_threshold(self):
        cadence = sync_health_summary(self._sync(interval=60))["quarantine_cadence"]
        self.assertEqual(cadence["binding"], "hours")
        self.assertEqual(cadence["effective_hours"], 72.0)
        self.assertIn("3 runs is 3.0 hours", cadence["message"])
        self.assertIn("hours threshold binds", cadence["message"])

    def test_a_weekly_sync_is_bound_by_the_runs_threshold(self):
        cadence = sync_health_summary(self._sync(interval=7 * 24 * 60))["quarantine_cadence"]
        self.assertEqual(cadence["binding"], "runs")
        self.assertEqual(cadence["effective_hours"], 504.0)
        self.assertIn("runs threshold binds", cadence["message"])

    def test_operator_thresholds_are_read_from_the_source(self):
        cadence = sync_health_summary(
            self._sync(
                interval=60,
                device_tag_prune_absence_runs=1,
                device_tag_prune_absence_hours=6,
            )
        )["quarantine_cadence"]
        self.assertEqual(cadence["required_runs"], 1)
        self.assertEqual(cadence["required_hours"], 6)
        self.assertEqual(cadence["effective_hours"], 6.0)

    def test_a_manual_sync_says_so_rather_than_guessing(self):
        cadence = sync_health_summary(self._sync(interval=None))["quarantine_cadence"]
        self.assertIsNone(cadence["effective_hours"])
        self.assertIsNone(cadence["binding"])
        self.assertIn("no recurrence interval", cadence["message"])
        self.assertEqual(cadence["status"], "info")
