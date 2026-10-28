import subprocess
import tempfile
import unittest
from pathlib import Path
from unittest import mock

from scripts import gated_tree
from scripts import pre_push_gate

PLAN = "docs/03_Plans/active/2026-10-26-release-3.0.9.md"
OTHER_PLAN = "docs/03_Plans/active/2026-10-27-site-network-maps-panel.md"


def entry(path, blob="a" * 40, mode="100644"):
    return f"{mode} blob {blob}\t{path}"


class DigestEntriesTest(unittest.TestCase):
    def test_the_release_plan_for_the_version_is_left_out(self):
        base = [entry("tasks.py"), entry(OTHER_PLAN)]
        with_plan = [*base, entry(PLAN, "b" * 40)]
        changed_plan = [*base, entry(PLAN, "c" * 40)]

        self.assertEqual(
            gated_tree.digest_entries(with_plan, "3.0.9"),
            gated_tree.digest_entries(changed_plan, "3.0.9"),
        )
        self.assertEqual(
            gated_tree.digest_entries(with_plan, "3.0.9"),
            gated_tree.digest_entries(base, "3.0.9"),
        )

    def test_any_other_file_changing_changes_the_digest(self):
        base = [entry("tasks.py"), entry(PLAN)]

        self.assertNotEqual(
            gated_tree.digest_entries(base, "3.0.9"),
            gated_tree.digest_entries(
                [entry("tasks.py", "d" * 40), entry(PLAN)], "3.0.9"
            ),
        )
        self.assertNotEqual(
            gated_tree.digest_entries(base, "3.0.9"),
            gated_tree.digest_entries(
                [entry("tasks.py", mode="100755"), entry(PLAN)], "3.0.9"
            ),
        )
        self.assertNotEqual(
            gated_tree.digest_entries(base, "3.0.9"),
            gated_tree.digest_entries([*base, entry("new.py")], "3.0.9"),
        )

    def test_a_plan_for_another_version_is_not_left_out(self):
        other = "docs/03_Plans/active/2026-10-01-release-3.0.8.md"
        self.assertNotEqual(
            gated_tree.digest_entries([entry(other, "a" * 40)], "3.0.9"),
            gated_tree.digest_entries([entry(other, "b" * 40)], "3.0.9"),
        )

    def test_the_order_of_entries_does_not_matter(self):
        one, two = entry("a.py"), entry("b.py")
        self.assertEqual(
            gated_tree.digest_entries([one, two], "3.0.9"),
            gated_tree.digest_entries([two, one], "3.0.9"),
        )


class RecordedDigestTest(unittest.TestCase):
    def test_reads_the_digest_line_the_cut_rendered(self):
        digest = "e" * 64
        text = (
            "## Release Authorization\n\n"
            "- Gated tree digest (every file except this plan): "
            f"`{digest}`\n"
        )
        self.assertEqual(gated_tree.recorded_digest(text), digest)

    def test_a_plan_without_the_line_records_none(self):
        self.assertIsNone(gated_tree.recorded_digest("## Release Authorization\n"))


class GitBackedTest(unittest.TestCase):
    """Real git repos, so the digest is proven against actual trees."""

    def setUp(self):
        self._scratch = tempfile.TemporaryDirectory()
        self.addCleanup(self._scratch.cleanup)
        self.repo = Path(self._scratch.name)
        self._git("init", "-q")
        self._git("config", "user.email", "t@example.com")
        self._git("config", "user.name", "T")
        (self.repo / "docs/03_Plans/active").mkdir(parents=True)
        (self.repo / "code.py").write_text("one\n")
        self.plan = self.repo / PLAN
        self.plan.write_text("# Release 3.0.9\n")
        self._git("add", "-A")
        self._git("commit", "-q", "-m", "cut")
        patcher = mock.patch.object(gated_tree, "REPO_ROOT", self.repo)
        patcher.start()
        self.addCleanup(patcher.stop)

    def _git(self, *args):
        return subprocess.run(
            ["git", *args], cwd=self.repo, check=True, capture_output=True, text=True
        ).stdout.strip()

    def _authorize(self, digest):
        self.plan.write_text(
            self.plan.read_text()
            + "\n## Release Authorization\n\n"
            + "- Gated tree digest (every file except this plan): "
            + f"`{digest}`\n"
        )
        self._git("add", "-A")
        self._git("commit", "-q", "-m", "authorize")

    def test_the_working_tree_digest_equals_the_digest_of_what_it_commits(self):
        working = gated_tree.working_tree_digest("3.0.9")

        self.assertEqual(working, gated_tree.commit_digest("HEAD", "3.0.9"))

    def test_a_tracked_file_that_gitignore_matches_stays_in_the_digest(self):
        (self.repo / ".gitignore").write_text("env/\n")
        (self.repo / "env").mkdir()
        (self.repo / "env/app.env").write_text("A=1\n")
        self._git("add", "-A")
        self._git("add", "-f", "env/app.env")
        self._git("commit", "-q", "-m", "track an ignored file")

        working = gated_tree.working_tree_digest("3.0.9")

        self.assertEqual(working, gated_tree.commit_digest("HEAD", "3.0.9"))

    def test_a_modified_or_deleted_tracked_ignored_file_matches_its_commit(self):
        (self.repo / ".gitignore").write_text("env/\n")
        (self.repo / "env").mkdir()
        (self.repo / "env/a.env").write_text("A=1\n")
        (self.repo / "env/b.env").write_text("B=1\n")
        self._git("add", "-A")
        self._git("add", "-f", "env/a.env", "env/b.env")
        self._git("commit", "-q", "-m", "track ignored files")
        (self.repo / "env/a.env").write_text("A=2\n")
        (self.repo / "env/b.env").unlink()

        working = gated_tree.working_tree_digest("3.0.9")

        self._git("add", "-A")
        self._git("commit", "-q", "-m", "change them")
        self.assertEqual(working, gated_tree.commit_digest("HEAD", "3.0.9"))

    def test_the_working_tree_digest_does_not_touch_the_real_index(self):
        (self.repo / "untracked.py").write_text("x\n")

        gated_tree.working_tree_digest("3.0.9")

        self.assertEqual(self._git("status", "--porcelain"), "?? untracked.py")

    def test_a_plan_only_authorization_commit_is_the_gated_tree(self):
        self._authorize(gated_tree.working_tree_digest("3.0.9"))

        ok, reason = gated_tree.check_ref("HEAD", "3.0.9")

        self.assertTrue(ok, reason)

    def test_another_change_after_the_cut_is_not_the_gated_tree(self):
        digest = gated_tree.working_tree_digest("3.0.9")
        (self.repo / "code.py").write_text("two\n")
        self._git("add", "-A")
        self._git("commit", "-q", "-m", "drift")
        self._authorize(digest)

        ok, reason = gated_tree.check_ref("HEAD", "3.0.9")

        self.assertFalse(ok)
        self.assertIn("differs", reason)

    def test_a_plan_with_no_recorded_digest_is_not_proven(self):
        ok, reason = gated_tree.check_ref("HEAD", "3.0.9")

        self.assertFalse(ok)
        self.assertIn("no gated tree digest", reason)

    def test_no_release_plan_for_the_version_is_not_proven(self):
        ok, reason = gated_tree.check_ref("HEAD", "9.9.9")

        self.assertFalse(ok)
        self.assertIn("expected one release plan", reason)


class ChooseTierTest(unittest.TestCase):
    def _ref(self, remote_ref, sha="1" * 40):
        return {
            "local_ref": remote_ref,
            "local_sha": sha,
            "remote_ref": remote_ref,
            "remote_sha": "0" * 40,
        }

    def test_a_branch_push_runs_the_quick_tier(self):
        tier, _ = pre_push_gate.choose_tier(
            [self._ref("refs/heads/feature")], env={}, tag_tree_check=self.fail
        )
        self.assertEqual(tier, "quick")

    def test_the_env_switch_forces_the_full_gate_on_any_push(self):
        tier, reason = pre_push_gate.choose_tier(
            [self._ref("refs/heads/feature")],
            env={"FORWARD_NETBOX_FULL_GATE": "1"},
        )
        self.assertEqual(tier, "full")
        self.assertIn("FORWARD_NETBOX_FULL_GATE", reason)

    def test_a_tag_with_the_gated_tree_runs_the_quick_tier(self):
        tier, _ = pre_push_gate.choose_tier(
            [self._ref("refs/tags/v3.0.9")],
            env={},
            tag_tree_check=lambda sha, version: (True, "gated"),
        )
        self.assertEqual(tier, "quick")

    def test_a_tag_whose_tree_is_not_proven_runs_the_full_gate(self):
        seen = []

        def check(sha, version):
            seen.append((sha, version))
            return False, "the tree differs from the one the cut gated"

        tier, reason = pre_push_gate.choose_tier(
            [self._ref("refs/tags/v3.0.9", "2" * 40)], env={}, tag_tree_check=check
        )

        self.assertEqual(tier, "full")
        self.assertIn("differs", reason)
        self.assertEqual(seen, [("2" * 40, "3.0.9")])

    def test_one_unproven_tag_among_several_refs_forces_the_full_gate(self):
        tier, _ = pre_push_gate.choose_tier(
            [self._ref("refs/heads/x"), self._ref("refs/tags/v3.0.9")],
            env={},
            tag_tree_check=lambda sha, version: (False, "no"),
        )
        self.assertEqual(tier, "full")

    def test_a_non_release_tag_is_treated_like_a_branch(self):
        tier, _ = pre_push_gate.choose_tier(
            [self._ref("refs/tags/scratch")], env={}, tag_tree_check=self.fail
        )
        self.assertEqual(tier, "quick")

    def test_parse_refs_reads_git_hook_lines(self):
        refs = pre_push_gate.parse_refs(
            "refs/heads/a " + "1" * 40 + " refs/heads/a " + "0" * 40 + "\n\nbad line\n"
        )
        self.assertEqual(len(refs), 1)
        self.assertEqual(refs[0]["remote_ref"], "refs/heads/a")

    def fail(self, *args):
        raise AssertionError("the tag check must not run for this push")


if __name__ == "__main__":
    unittest.main()
