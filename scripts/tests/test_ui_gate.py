# The browser UI journeys are part of the release gate. These checks keep that true:
# the journeys a release requires still exist, and `invoke ci` (the command a release
# authorization records) still runs the UI task, which fails when a required journey
# does not run and pass.
import re
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
JOURNEYS = ROOT / "scripts" / "ui" / "journeys"


def _required():
    lines = (ROOT / "scripts" / "ui" / "required-journeys.txt").read_text().splitlines()
    return [line.strip() for line in lines if line.strip() and not line.startswith("#")]


def _journey_ids():
    ids = []
    for path in sorted(JOURNEYS.glob("*.mjs")):
        match = re.search(r'^\s*id:\s*"([a-z0-9-]+)"', path.read_text(), re.MULTILINE)
        assert match, f"{path.name} declares no journey id"
        ids.append(match.group(1))
    return ids


class UiGateTest(unittest.TestCase):
    def test_every_required_journey_exists(self):
        missing = sorted(set(_required()) - set(_journey_ids()))

        self.assertEqual(missing, [], "a required UI journey has no file")

    def test_journey_ids_are_unique(self):
        ids = _journey_ids()

        self.assertEqual(len(ids), len(set(ids)))

    def test_a_site_rules_journey_is_required(self):
        # The 3.0.18 dropdown shipped broken because no journey opened a tag row.
        self.assertIn("site-rules-table", _required())
        self.assertIn("site-rules-save", _required())

    def test_the_runner_enforces_the_required_list(self):
        runner = (ROOT / "scripts" / "forward_ui_harness.mjs").read_text()

        self.assertIn("required-journeys.txt", runner)
        self.assertIn("required UI journeys did not run and pass", runner)

    def test_the_release_gate_runs_the_ui_task(self):
        tasks = (ROOT / "tasks.py").read_text()
        ci = tasks[tasks.index("@task(\n    pre=[\n        *_QUICK_GATE_LEGS") :]
        ci = ci[: ci.index("def ci(context)")]

        self.assertRegex(ci, r"\bui_test,")


if __name__ == "__main__":
    unittest.main()
