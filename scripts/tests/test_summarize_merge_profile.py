import importlib.util
import unittest
from collections import defaultdict
from pathlib import Path

SCRIPT = Path(__file__).resolve().parents[1] / "summarize_merge_profile.py"
SPEC = importlib.util.spec_from_file_location("summarize_merge_profile", SCRIPT)
MODULE = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(MODULE)


class MergeProfileSummaryTest(unittest.TestCase):
    def test_phase_cost_sums_model_buckets_before_round_statistics(self):
        round_result = {
            "volume": 10,
            "round": 1,
            "model_counts": {"dcim.device": 4, "dcim.interface": 6},
            "started_epoch": 1.0,
            "finished_epoch": 11.0,
            "wall_seconds": 10.0,
            "changes_per_second": 1.0,
            "statements_per_change": 1.2,
            "db_round_trips_per_change": 1.2,
            "db_execute_wall_fraction": 0.2,
            "python_cpu_seconds": 4.0,
            "python_cpu_utilization": 0.4,
            "peak_rss_mib": 100.0,
            "postgres": {"wal_bytes": 1000},
            "buckets": [
                {
                    "owner": "ours",
                    "phase": "bulk_application",
                    "model": "dcim.device",
                    "wall_seconds": 2.0,
                    "statements": 5,
                    "db_wall_seconds": 0.4,
                },
                {
                    "owner": "ours",
                    "phase": "bulk_application",
                    "model": "dcim.interface",
                    "wall_seconds": 3.0,
                    "statements": 7,
                    "db_wall_seconds": 0.6,
                },
                {
                    "owner": "unknown",
                    "phase": "unattributed",
                    "model": "",
                    "wall_seconds": 5.0,
                    "statements": 0,
                    "db_wall_seconds": 0.0,
                },
            ],
        }

        result = MODULE.summarize([round_result], defaultdict(list))
        phase = result["volumes"]["10"]["phases"]["ours:bulk_application"]

        self.assertEqual(phase["wall_seconds_per_change"]["median"], 0.5)
        self.assertEqual(phase["statements_per_change"]["median"], 1.2)
        self.assertEqual(phase["db_wall_seconds_per_change"]["median"], 0.1)


if __name__ == "__main__":
    unittest.main()
