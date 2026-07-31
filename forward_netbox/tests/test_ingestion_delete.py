"""Ingestions must be deletable, and a protected one must say why.

2.6.3 fixed a `NoReverseMatch` crash on the Ingestions list by removing the
delete action from the table rather than registering the view the action
pointed at. The crash went away and so did the ability to delete an ingestion —
reported by a customer against 2.6.5.

Most relations to an ingestion cascade, but `ForwardContributorBaseline` is
PROTECT: it is durable convergence evidence and deleting the ingestion that
produced it would strand it. That case must be reported, not surfaced as an
unhandled `ProtectedError`.
"""

from unittest.mock import patch

from django.contrib.auth import get_user_model
from django.test import TestCase
from django.urls import reverse

from forward_netbox.models import ForwardIngestion
from forward_netbox.models import ForwardSource
from forward_netbox.models import ForwardSync
from forward_netbox.utilities.forward_api import LATEST_PROCESSED_SNAPSHOT
from forward_netbox.views import _ingestion_delete_refusal


class IngestionDeleteRouteTest(TestCase):
    def test_the_delete_route_exists(self):
        # The absence of this route is what 2.6.3 worked around by removing the
        # button; asserting it directly stops that recurring.
        self.assertEqual(
            reverse("plugins:forward_netbox:forwardingestion_delete", args=[1]),
            "/plugins/forward/ingestion/1/delete/",
        )

    def test_the_bulk_delete_route_exists(self):
        self.assertEqual(
            reverse("plugins:forward_netbox:forwardingestion_bulk_delete"),
            "/plugins/forward/ingestion/delete/",
        )

    def test_the_table_offers_the_delete_action(self):
        from forward_netbox.tables import ForwardIngestionTable

        actions = ForwardIngestionTable.base_columns["actions"].actions
        self.assertIn("delete", actions)


class IngestionDeleteRefusalTest(TestCase):
    def setUp(self):
        self.user = get_user_model().objects.create_user(username="ingestion-delete")
        self.source = ForwardSource.objects.create(
            name="ingestion-delete-source",
            type="saas",
            url="https://fwd.app",
            parameters={
                "username": "user@example.com",
                "password": "secret",
                "verify": True,
                "network_id": "net-1",
            },
        )
        self.sync = ForwardSync.objects.create(
            name="ingestion-delete-sync",
            source=self.source,
            user=self.user,
            parameters={"snapshot_id": LATEST_PROCESSED_SNAPSHOT},
        )
        self.ingestion = ForwardIngestion.objects.create(
            sync=self.sync,
            snapshot_selector=LATEST_PROCESSED_SNAPSHOT,
            snapshot_id="snapshot-delete",
        )

    def _baseline(self, model):
        return model.objects.create(
            sync=self.sync,
            ingestion=self.ingestion,
            snapshot_id="snapshot-delete",
            network_fingerprint="nf",
            map_set_fingerprint="mf",
            scope_config_fingerprint="cf",
            scope_membership_fingerprint="sf",
            scope_payload_checksum="pc",
        )

    def test_an_unreferenced_ingestion_is_deletable(self):
        self.assertEqual(_ingestion_delete_refusal(self.ingestion), "")
        self.ingestion.delete()
        self.assertFalse(ForwardIngestion.objects.filter(pk=self.ingestion.pk).exists())

    def test_a_contributor_baseline_blocks_the_delete_and_is_named(self):
        # The real PROTECT relation against a real row, not a mock.
        from django.db import models as django_models

        from forward_netbox.models import ForwardContributorBaseline

        field = ForwardContributorBaseline._meta.get_field("ingestion")
        self.assertIs(
            field.remote_field.on_delete,
            django_models.PROTECT,
            "ForwardContributorBaseline.ingestion is no longer PROTECT; this "
            "test no longer covers the case it was written for",
        )
        self._baseline(ForwardContributorBaseline)

        refusal = _ingestion_delete_refusal(self.ingestion)

        self.assertTrue(refusal, "a protected ingestion must be refused")
        self.assertIn("ForwardContributorBaseline", refusal)
        self.assertIn("cannot be deleted", refusal)

    def test_the_refusal_names_what_to_do(self):
        from forward_netbox.models import ForwardContributorBaseline

        self._baseline(ForwardContributorBaseline)
        self.assertIn("remove them first", _ingestion_delete_refusal(self.ingestion))

    def test_cascading_children_do_not_block_the_delete(self):
        # Issues cascade, so they must not be reported as protecting.
        from forward_netbox.models import ForwardIngestionIssue

        ForwardIngestionIssue.objects.create(
            ingestion=self.ingestion,
            message="probe",
            exception="ProbeError",
        )
        self.assertEqual(_ingestion_delete_refusal(self.ingestion), "")


class BaselineDeleteRefusesOnConfirmationTest(TestCase):
    """The refusal must come before the dependent-object wall, not after it.

    NetBox's delete view lists every dependent object on the confirmation page,
    and an ingestion owns one `ForwardDeviceIdentity` per synced device. A
    customer deleting the baseline ingestion met several hundred device names
    rendered into a popup and only learned the delete was impossible after
    confirming it — for the one ingestion in the system that is *meant* to
    refuse, because it holds the contributor baseline.
    """

    def test_the_confirmation_page_refuses_instead_of_listing_dependents(self):
        from django.contrib.auth import get_user_model
        from django.test import Client
        from django.urls import reverse

        from forward_netbox.models import ForwardIngestion
        from forward_netbox.models import ForwardSource
        from forward_netbox.models import ForwardSync

        source = ForwardSource.objects.create(
            name="confirm-source", url="https://fwd.example.invalid"
        )
        sync = ForwardSync.objects.create(name="confirm-sync", source=source)
        ingestion = ForwardIngestion.objects.create(sync=sync)

        user = get_user_model().objects.create_superuser(
            username="confirm-user", email="", password="x"
        )
        client = Client()
        client.force_login(user)
        url = reverse(
            "plugins:forward_netbox:forwardingestion_delete",
            kwargs={"pk": ingestion.pk},
        )

        with patch(
            "forward_netbox.views._ingestion_delete_refusal",
            return_value="held by convergence evidence",
        ):
            response = client.get(url)

        # Redirected away rather than rendering the dependent-object list.
        self.assertEqual(response.status_code, 302)

    def test_a_deletable_ingestion_still_reaches_the_confirmation_page(self):
        # The guard must not block the ordinary case; every non-baseline
        # ingestion still deletes normally.
        from django.contrib.auth import get_user_model
        from django.test import Client
        from django.urls import reverse

        from forward_netbox.models import ForwardIngestion
        from forward_netbox.models import ForwardSource
        from forward_netbox.models import ForwardSync

        source = ForwardSource.objects.create(
            name="ok-source", url="https://fwd.example.invalid"
        )
        sync = ForwardSync.objects.create(name="ok-sync", source=source)
        ingestion = ForwardIngestion.objects.create(sync=sync)

        user = get_user_model().objects.create_superuser(
            username="ok-user", email="", password="x"
        )
        client = Client()
        client.force_login(user)
        url = reverse(
            "plugins:forward_netbox:forwardingestion_delete",
            kwargs={"pk": ingestion.pk},
        )

        with patch("forward_netbox.views._ingestion_delete_refusal", return_value=""):
            response = client.get(url)

        self.assertEqual(response.status_code, 200)
