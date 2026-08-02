# The three post-release steps were each skipped at least once, because only one
# of them was automated and nothing failed at the time. These pin the piece that
# can be automated - opening the next `.dev0` - and the shape of the follow-up
# instruction for the piece that cannot, because the bridge commit's hash does
# not exist until its pull request lands.
from __future__ import annotations

import unittest
from unittest.mock import patch

import scripts.release as release


class NextPatchVersionTest(unittest.TestCase):
    def test_patch_component_increments(self):
        self.assertEqual(release._next_patch_version("2.7.0"), "2.7.1")

    def test_double_digit_patch_does_not_go_lexicographic(self):
        # "2.7.9" -> "2.7.10", not "2.7.91" or a string sort.
        self.assertEqual(release._next_patch_version("2.7.9"), "2.7.10")
        self.assertEqual(release._next_patch_version("2.9.99"), "2.9.100")


class StagePostReleaseTest(unittest.TestCase):
    def _run(self):
        calls = []
        with (
            patch.object(release, "run", side_effect=lambda cmd, **kw: calls.append(cmd)),
            patch.object(release, "stage_open_next") as open_next,
        ):
            release.stage_post_release("2.7.0", "v2.7.0")
        return calls, open_next

    def test_it_opens_the_next_dev_marker_through_the_existing_stage(self):
        # Not by editing files here: the four version surfaces must move
        # together, and the fast-baseline pin is the one that gets left behind.
        _calls, open_next = self._run()
        open_next.assert_called_once_with("2.7.1", write=True)

    def test_it_branches_from_origin_main_and_pushes(self):
        calls, _open_next = self._run()
        joined = [" ".join(call) for call in calls]
        self.assertIn("git checkout -B release/2.7.1-post-release origin/main", joined)
        self.assertTrue(
            any(call.startswith("git push") and "release/2.7.1-post-release" in call for call in joined),
            joined,
        )

    def test_it_runs_the_harness_gate_before_pushing(self):
        # A high-risk version-surface change without a plan file reds CI; catching
        # it locally avoids burning a round-trip during a release.
        calls, _open_next = self._run()
        joined = [" ".join(call) for call in calls]
        harness = next(i for i, c in enumerate(joined) if "check_harness.py" in c)
        push = next(i for i, c in enumerate(joined) if c.startswith("git push"))
        self.assertLess(harness, push)

    def test_it_prints_the_anchor_follow_up_naming_the_released_tag(self):
        with (
            patch.object(release, "run"),
            patch.object(release, "stage_open_next"),
            patch("builtins.print") as printed,
        ):
            release.stage_post_release("2.7.0", "v2.7.0")
        text = "\n".join(str(call.args[0]) for call in printed.call_args_list if call.args)
        self.assertIn("PRIOR_RELEASE_TAG", text)
        self.assertIn("PRIOR_POST_RELEASE_DOC_COMMIT", text)
        self.assertIn("v2.7.0..origin/main", text)


if __name__ == "__main__":
    unittest.main()
