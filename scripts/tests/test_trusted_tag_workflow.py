from __future__ import annotations

import unittest
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[2]


class TrustedTagWorkflowTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.workflow = (REPO_ROOT / ".github/workflows/trusted-tag.yml").read_text(
            encoding="utf-8"
        )

    def test_authorizer_uses_protected_repository_scoped_app_token(self):
        for fragment in (
            "environment: release-tag",
            "verify_only:",
            "actions/create-github-app-token@bcd2ba49218906704ab6c1aa796996da409d3eb1",
            "secrets.RELEASE_CONTROL_APP_ID",
            "secrets.RELEASE_CONTROL_APP_PRIVATE_KEY",
            "repositories: ${{ github.event.repository.name }}",
            "permission-administration: write",
            "permission-actions: read",
            "permission-contents: read",
            "permission-environments: read",
            "permission-pull-requests: read",
            "permission-statuses: read",
            "GH_TOKEN: ${{ steps.release-control-token.outputs.token }}",
        ):
            with self.subTest(fragment=fragment):
                self.assertIn(fragment, self.workflow)

    def test_release_control_token_cannot_write_repository_contents(self):
        self.assertNotIn("permission-contents: write", self.workflow)

    def test_deploy_key_is_the_only_tag_writing_identity(self):
        self.assertIn("secrets.RELEASE_TAG_DEPLOY_KEY", self.workflow)
        self.assertIn(
            'git push "git@github.com:${GITHUB_REPOSITORY}.git"',
            self.workflow,
        )


if __name__ == "__main__":
    unittest.main()
