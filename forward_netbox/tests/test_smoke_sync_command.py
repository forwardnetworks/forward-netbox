from io import StringIO
from types import SimpleNamespace
from unittest.mock import Mock
from unittest.mock import patch

from django.contrib.auth import get_user_model
from django.core.management import call_command
from django.core.management.base import CommandError
from django.test import TestCase

from forward_netbox.choices import ForwardExecutionBackendChoices
from forward_netbox.models import ForwardSource
from forward_netbox.models import ForwardSync


class ForwardSmokeSyncCommandTest(TestCase):
    def setUp(self):
        self.source = ForwardSource.objects.create(
            name="private-source-name",
            type="saas",
            url="https://fwd.app",
            status="ready",
            parameters={
                "username": "local-user",
                "password": "local-secret",
                "verify": True,
                "network_id": "private-network-id",
            },
        )

    def test_check_source_automatically_selects_existing_source_and_redacts_output(
        self,
    ):
        stdout = StringIO()
        with patch.object(ForwardSource, "validate_connection") as validate:
            call_command("forward_smoke_sync", "--check-source", stdout=stdout)

        validate.assert_called_once_with()
        output = stdout.getvalue()
        self.assertIn("configured Forward source", output)
        self.assertNotIn(self.source.name, output)
        self.assertNotIn("private-network-id", output)
        self.assertNotIn("local-secret", output)

    def test_named_source_does_not_fall_back_to_another_source(self):
        with self.assertRaisesMessage(
            CommandError, "selected Forward source is unavailable"
        ):
            call_command(
                "forward_smoke_sync",
                "--check-source",
                "--source-name",
                "missing-source",
                stdout=StringIO(),
                stderr=StringIO(),
            )

    def test_validate_only_uses_single_branch_and_redacted_results(self):
        get_user_model().objects.create_superuser(
            username="smoke-admin",
            email="smoke@example.com",
            password="test-password",
        )
        result = SimpleNamespace(
            model_string="dcim.device",
            row_count=2,
            runtime_ms=12.5,
        )
        fetcher = Mock()
        fetcher.resolve_context.return_value = Mock()
        fetcher.fetch_sample_results.return_value = [result]
        stdout = StringIO()

        with (
            patch.object(ForwardSource, "validate_connection"),
            patch(
                "forward_netbox.management.commands.forward_smoke_sync.ForwardQueryFetcher",
                return_value=fetcher,
            ),
        ):
            call_command(
                "forward_smoke_sync",
                "--validate-only",
                "--models",
                "dcim.device",
                "--sync-name",
                "redacted-smoke-sync",
                stdout=stdout,
            )

        sync = ForwardSync.objects.get(name="redacted-smoke-sync")
        self.assertEqual(
            sync.parameters["execution_backend"],
            ForwardExecutionBackendChoices.SINGLE_BRANCH,
        )
        self.assertEqual(sync.source, self.source)
        output = stdout.getvalue()
        self.assertIn("dcim.device | rows=2 | runtime_ms=12.5", output)
        self.assertNotIn(self.source.name, output)
        self.assertNotIn("private-network-id", output)
        self.assertNotIn("local-secret", output)
