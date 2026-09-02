# The three post-release steps were each skipped at least once, because only one
# of them was automated and nothing failed at the time. These pin the piece that
# can be automated - opening the next `.dev0` - and the shape of the follow-up
# instruction for the piece that cannot, because the bridge commit's hash does
# not exist until its pull request lands.
from __future__ import annotations

import inspect
import pathlib
import tempfile
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
    def _run(self, *, failing=None):
        calls = []

        def run(cmd, **kwargs):
            calls.append(cmd)
            if failing and failing in " ".join(cmd):
                raise release.ReleaseError("release command failed with exit code 1")

        with tempfile.TemporaryDirectory() as scratch:
            plan_path = pathlib.Path(scratch) / "post-release-2.7.0.md"
            with (
                patch.object(release, "run", side_effect=run),
                patch.object(release, "_bridge_plan_path", return_value=plan_path),
                patch.object(release, "_open_pull_request") as open_pull_request,
            ):
                try:
                    release.stage_post_release("2.7.0", "v2.7.0")
                except release.ReleaseError:
                    if not failing:
                        raise
            written = plan_path.read_text(encoding="utf-8")
        self.open_pull_request = open_pull_request
        return calls, written

    def test_it_opens_the_bridge_pull_request_itself(self):
        # The old version pushed and printed "merge that pull request" without
        # opening one, so an unattended run could never get past this step.
        self._run()
        self.open_pull_request.assert_called_once()
        self.assertEqual(
            self.open_pull_request.call_args.args[0], "docs/post-release-2.7.0"
        )

    def test_a_failed_stage_deletes_the_branch_it_made(self):
        # Leaving the branch behind is how v2.8.3's bridge slot was lost: the
        # next `git checkout -b` inherited a commit nobody meant to keep.
        calls, _written = self._run(failing="check_harness.py")
        joined = [" ".join(call) for call in calls]
        self.assertIn("git branch -D docs/post-release-2.7.0", joined)
        self.open_pull_request.assert_not_called()

    def test_it_writes_the_bridge_plan(self):
        _calls, written = self._run()
        self.assertIn("# Post-release bridge for 2.7.0", written)

    def test_it_branches_from_origin_main_and_pushes(self):
        calls, _written = self._run()
        joined = [" ".join(call) for call in calls]
        self.assertIn("git checkout -B docs/post-release-2.7.0 origin/main", joined)
        self.assertTrue(
            any(
                call.startswith("git push") and "docs/post-release-2.7.0" in call
                for call in joined
            ),
            joined,
        )

    def test_it_runs_the_harness_gate_before_pushing(self):
        # The gate was never the problem. It was correctly refusing a commit
        # that changed high-risk version surfaces with no plan file - which is
        # what the old .dev0 bump did on every release. A documentation-only
        # bridge satisfies it.
        calls, _written = self._run()
        joined = [" ".join(call) for call in calls]
        harness = next(i for i, c in enumerate(joined) if "check_harness.py" in c)
        push = next(i for i, c in enumerate(joined) if c.startswith("git push"))
        self.assertLess(harness, push)

    def test_it_prints_the_anchor_follow_up_naming_the_released_tag(self):
        with tempfile.TemporaryDirectory() as scratch:
            plan_path = pathlib.Path(scratch) / "post-release-2.7.0.md"
            with (
                patch.object(release, "run"),
                patch.object(release, "_bridge_plan_path", return_value=plan_path),
                patch.object(release, "_open_pull_request"),
                patch("builtins.print") as printed,
            ):
                release.stage_post_release("2.7.0", "v2.7.0")
        text = "\n".join(
            str(call.args[0]) for call in printed.call_args_list if call.args
        )
        self.assertIn("PRIOR_RELEASE_TAG", text)
        self.assertIn("PRIOR_POST_RELEASE_DOC_COMMIT", text)
        self.assertIn("release.py 2.7.0 --anchor", text)


class PostReleaseOpensTheBridgeTest(unittest.TestCase):
    """The stage must produce the commit a release actually needs next.

    It used to commit the next `.dev0` instead, and that was wrong three ways.
    It FAILED - the bump touches `pyproject.toml` and `fast_baseline.py`, both
    high-risk, and the harness requires a plan file in the same commit, which
    `stage_open_next` does not write. It STRANDED the operator on the branch it
    had created, with the bump committed and a clean working tree, so the next
    `git checkout -b` inherited it and for v2.8.3 that reached the bridge and
    disqualified it permanently. And it was the WRONG COMMIT: what follows a
    release is the documentation-only bridge, and that slot goes to whatever
    lands first.
    """

    def test_the_stage_no_longer_bumps_to_a_dev_marker(self):
        source = inspect.getsource(release.stage_post_release)
        body = source[source.index('"""', source.index('"""') + 3) + 3 :]
        self.assertNotIn(
            "stage_open_next(",
            body,
            "the post-release stage must not commit a .dev0; main carries the "
            "released version, and the bump used to poison the bridge slot",
        )

    def test_open_next_is_gone(self):
        # It documented a real incident and was the wrong remedy for it: a dev
        # marker on `main` offered source installs a version that was never
        # gated, tagged or published. The decision is recorded on the stage.
        self.assertFalse(hasattr(release, "stage_open_next"))

    def test_the_commit_carries_exactly_one_plan_file(self):
        source = inspect.getsource(release.stage_post_release)
        self.assertIn("plan_path", source)
        self.assertNotIn(
            '"add", "-A"',
            source,
            "a bridge commit must carry one file; `git add -A` is how four "
            "version surfaces reached the v2.8.3 bridge",
        )

    def test_the_generated_plan_satisfies_the_harness_headings(self):
        text = release._bridge_plan_text("9.9.9", "v9.9.9")
        for heading in (
            "## Goal",
            "## Constraints",
            "## Touched Surfaces",
            "## Approach",
            "## Validation",
            "## Rollback",
            "## Decision Log",
        ):
            self.assertIn(heading, text)

    def test_the_generated_plan_passes_the_verifiers_own_documentation_rule(self):
        """Cross-check against the verifier, not against a restated rule.

        The bridge is rejected unless every path in it is documentation. Asking
        `verify_release_provenance` directly means this cannot drift from the
        check that actually gates the release.
        """
        import importlib.util

        spec = importlib.util.spec_from_file_location(
            "bridge_rule", release.REPO_ROOT / "scripts/verify_release_provenance.py"
        )
        provenance = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(provenance)

        path = release._bridge_plan_path("9.9.9")
        relative = path.relative_to(release.REPO_ROOT).as_posix()
        self.assertTrue(
            provenance._is_documentation_path(relative),
            f"{relative} would disqualify the bridge it is generated for",
        )


class PostReleaseReturnsTheOperatorHomeTest(unittest.TestCase):
    """A stage that moves you must move you back, success or failure.

    The old version left the operator on a branch it created whenever its
    harness check failed - which was every release. The working tree was CLEAN,
    so `git status` showed nothing wrong, and the next `git checkout -b`
    silently inherited a commit they did not make.
    """

    def test_the_starting_branch_is_recorded_before_any_move(self):
        source = inspect.getsource(release.stage_post_release)
        self.assertIn("starting_branch", source)
        recorded = source.index("starting_branch")
        moved = source.index('"-B"')
        self.assertLess(
            recorded, moved, "record where the operator was before moving them"
        )

    def test_the_restore_runs_on_success_as_well_as_failure(self):
        source = inspect.getsource(release.stage_post_release)
        self.assertIn("finally:", source)
        restore = source[source.index("finally:") :]
        self.assertIn("checkout", restore)
        self.assertIn("starting_branch", restore)

    def test_a_failed_restore_cannot_replace_the_real_error(self):
        source = inspect.getsource(release.stage_post_release)
        restore = source[source.index("finally:") :]
        self.assertIn("check=False", restore)

    def test_the_whole_sequence_is_guarded(self):
        source = inspect.getsource(release.stage_post_release)
        guarded = source[source.index("    try:") : source.index("    finally:")]
        for fragment in ('"-B"', '"commit"', "check_harness.py", '"push"'):
            self.assertIn(fragment, guarded)


if __name__ == "__main__":
    unittest.main()
