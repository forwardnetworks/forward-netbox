from datetime import timedelta
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

from django.contrib.auth import get_user_model
from django.test import TestCase
from django.utils import timezone

from forward_netbox.models import ForwardSource, ForwardSnapshot
from forward_netbox.choices import ForwardSourceTypeChoices


class ForwardSourceModelTest(TestCase):
    def setUp(self):
        self.base_kwargs = {
            "name": "Forward Local",
            "type": ForwardSourceTypeChoices.LOCAL,
            "url": "https://forward.example.com",
        }

    def test_create_source_with_network_id(self):
        source = ForwardSource.objects.create(network_id="net-12345", **self.base_kwargs)
        self.assertEqual(source.network_id, "net-12345")

    def test_update_source_network_id(self):
        source = ForwardSource.objects.create(**self.base_kwargs)
        source.network_id = "net-67890"
        source.save()
        source.refresh_from_db()
        self.assertEqual(source.network_id, "net-67890")

    def test_delete_source_with_network_id(self):
        source = ForwardSource.objects.create(network_id="net-abc", **self.base_kwargs)
        source_pk = source.pk
        source.delete()
        self.assertFalse(ForwardSource.objects.filter(pk=source_pk).exists())

    @patch("forward_netbox.models.ForwardSource.get_client")
    def test_sync_creates_latest_snapshot_entry(self, mock_get_client):
        now = timezone.now()
        mock_client = MagicMock()
        mock_client.list_snapshots.return_value = [
            {
                "snapshot_id": "100",
                "name": "Snap 100",
                "status": "loaded",
                "start": now.isoformat(),
                "end": (now + timedelta(minutes=1)).isoformat(),
                "processed_at_millis": int(now.timestamp() * 1000),
            },
            {
                "snapshot_id": "200",
                "name": "Snap 200",
                "status": "loaded",
                "start": (now + timedelta(minutes=2)).isoformat(),
                "end": (now + timedelta(minutes=3)).isoformat(),
                "processed_at_millis": int((now.timestamp() + 300) * 1000),
            },
        ]
        mock_get_client.return_value = mock_client

        source = ForwardSource.objects.create(**self.base_kwargs)
        User = get_user_model()
        user = User.objects.create_superuser(
            username="tester", email="tester@example.com", password="pass"
        )
        job = SimpleNamespace(pk=1, data=None, user=user)

        source.sync(job=job)

        latest = ForwardSnapshot.objects.get(source=source, snapshot_id="$last")
        self.assertEqual(latest.resolve_snapshot_id(), "200")
        self.assertEqual(latest.status, "loaded")
