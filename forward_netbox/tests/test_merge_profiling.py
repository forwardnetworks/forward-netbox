from django.test import SimpleTestCase

from forward_netbox.utilities.merge_profiling import MergeProfileRecorder
from forward_netbox.utilities.merge_profiling import profile_scope


class MergeProfileRecorderTest(SimpleTestCase):
    def test_nested_scopes_attribute_round_trips_to_active_owner(self):
        recorder = MergeProfileRecorder(sample_interval_seconds=0.1)

        def execute(_sql, _params, _many, _context):
            return "ok"

        with (
            recorder.activate(),
            profile_scope("fallback_orchestration", owner="ours", rows=1),
        ):
            recorder.execute(execute, "SELECT 1", None, False, {})
            with profile_scope(
                "objectchange_apply",
                model="dcim.site",
                owner="upstream_netbox_branching",
                rows=1,
            ):
                recorder.execute(
                    execute, "UPDATE dcim_site SET name = %s", [], False, {}
                )

        result = recorder.result()
        buckets = {(item["owner"], item["phase"]): item for item in result["buckets"]}

        self.assertEqual(result["statements"], 2)
        self.assertEqual(
            buckets[("ours", "fallback_orchestration")]["statements"],
            1,
        )
        self.assertEqual(
            buckets[("upstream_netbox_branching", "objectchange_apply")]["statements"],
            1,
        )
        self.assertEqual(
            buckets[("upstream_netbox_branching", "objectchange_apply")]["sql_verbs"],
            {"UPDATE": 1},
        )
