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
from forward_netbox.views import _ingestion_delete_refusal_detail


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
        # The baseline case now says it is expected rather than reading as a
        # fault; a customer reported the old red error as a defect twice.
        self.assertIn("is the baseline for this sync", refusal)
        self.assertIn("expected, not a failure", refusal)

    def test_the_refusal_names_what_to_do(self):
        from forward_netbox.models import ForwardContributorBaseline

        self._baseline(ForwardContributorBaseline)
        self.assertIn(
            "remove those records first", _ingestion_delete_refusal(self.ingestion)
        )

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
            "forward_netbox.views._ingestion_delete_refusal_detail",
            return_value=("held by convergence evidence", False),
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


class ReconciliationNoLongerPinsAnIngestionTest(TestCase):
    """Reconciliation rows are children of an ingestion, not a lock on it.

    On 2.7.0 the refusal became honest and named every PROTECT reference, which
    exposed the real problem: it named `ForwardOwnershipReconciliation`, and no
    UI, API, or management command can delete those rows. The message asked for
    an action the product does not offer, so the ingestion was undeletable
    forever.

    A reconciliation row records only "this sync finished this domain at this
    ingestion". It re-points when its domain reconciles again — but a domain
    that stops running freezes its row on an old ingestion, and
    `required_ownership_domains` then treats that frozen row as proof the domain
    is still required, so the same row also keeps ownership permanently
    incomplete. Cascading clears both.
    """

    def setUp(self):
        self.source = ForwardSource.objects.create(
            name="cascade-source",
            type="saas",
            url="https://fwd.example.invalid",
            parameters={"network_id": "net-cascade"},
        )
        self.sync = ForwardSync.objects.create(
            name="cascade-sync",
            source=self.source,
            parameters={"snapshot_id": LATEST_PROCESSED_SNAPSHOT},
        )
        self.older = ForwardIngestion.objects.create(
            sync=self.sync,
            snapshot_id="snapshot-older",
            baseline_ready=True,
        )
        self.newer = ForwardIngestion.objects.create(
            sync=self.sync,
            snapshot_id="snapshot-newer",
            baseline_ready=True,
        )

    def _reconcile(self, ingestion, domain, status=None):
        from forward_netbox.models import ForwardOwnershipReconciliation

        return ForwardOwnershipReconciliation.objects.update_or_create(
            sync=self.sync,
            domain=domain,
            defaults={
                "ingestion_id": ingestion.pk,
                "snapshot_id": ingestion.snapshot_id,
                "status": status or ForwardOwnershipReconciliation.Status.COMPLETED,
            },
        )[0]

    def test_the_reconciliation_fk_cascades_and_its_siblings_still_protect(self):
        # The whole fix rests on exactly one of the four provenance models
        # cascading. Pin that split so a future edit to the shared mixin cannot
        # quietly take the other three with it — those describe live NetBox
        # rows and must keep protecting their provenance.
        from django.db import models as django_models

        from forward_netbox.models import ForwardDeviceIdentity
        from forward_netbox.models import ForwardDeviceTagClaim
        from forward_netbox.models import ForwardOwnershipReconciliation
        from forward_netbox.models import ForwardVirtualParentClaim

        self.assertIs(
            ForwardOwnershipReconciliation._meta.get_field(
                "ingestion"
            ).remote_field.on_delete,
            django_models.CASCADE,
        )
        for model in (
            ForwardDeviceIdentity,
            ForwardDeviceTagClaim,
            ForwardVirtualParentClaim,
        ):
            with self.subTest(model=model._meta.label):
                self.assertIs(
                    model._meta.get_field("ingestion").remote_field.on_delete,
                    django_models.PROTECT,
                )

    def test_an_ingestion_held_only_by_reconciliation_rows_is_deletable(self):
        # The customer's shape: a domain reconciled at an old ingestion and
        # never again, so its row froze there and pinned it.
        from forward_netbox.models import ForwardOwnershipReconciliation

        stale = self._reconcile(
            self.older,
            ForwardOwnershipReconciliation.Domain.VIRTUAL_PARENTS,
        )
        self._reconcile(
            self.newer,
            ForwardOwnershipReconciliation.Domain.STATUS_TAGS,
        )

        self.assertEqual(_ingestion_delete_refusal(self.older), "")

        self.older.delete()

        self.assertFalse(ForwardIngestion.objects.filter(pk=self.older.pk).exists())
        self.assertFalse(
            ForwardOwnershipReconciliation.objects.filter(pk=stale.pk).exists(),
            "the reconciliation row must go with the ingestion it describes",
        )

    def test_the_newest_completed_ingestion_is_refused_and_says_why(self):
        # Cascading alone would let an operator delete the evidence that
        # ownership has converged and regress the sync to Incomplete. That is
        # the one case the database can no longer refuse for us.
        from forward_netbox.models import ForwardOwnershipReconciliation

        for domain in (
            ForwardOwnershipReconciliation.Domain.STATUS_TAGS,
            ForwardOwnershipReconciliation.Domain.VIRTUAL_PARENTS,
        ):
            self._reconcile(self.newer, domain)

        refusal, expected = _ingestion_delete_refusal_detail(self.newer)

        self.assertTrue(refusal)
        self.assertIn("ownership reconciliation is currently complete", refusal)
        # Names an action the product actually offers, which is precisely what
        # the old "remove them first" message did not.
        self.assertIn("Run the sync again", refusal)
        self.assertTrue(expected, "an intact ownership record is not a fault")

    def test_a_pending_reconciliation_does_not_make_an_ingestion_current(self):
        # Only COMPLETED evidence is worth protecting; a pending row is a note
        # that work is outstanding, and it would otherwise pin forever.
        from forward_netbox.models import ForwardOwnershipReconciliation

        self._reconcile(
            self.newer,
            ForwardOwnershipReconciliation.Domain.STATUS_TAGS,
            status=ForwardOwnershipReconciliation.Status.PENDING,
        )

        self.assertEqual(_ingestion_delete_refusal(self.newer), "")

    def test_the_refusal_lifts_once_a_newer_ingestion_reconciles(self):
        # The refusal must be a wait, not a wall: what the message promises has
        # to actually happen.
        from forward_netbox.models import ForwardOwnershipReconciliation

        domains = (
            ForwardOwnershipReconciliation.Domain.STATUS_TAGS,
            ForwardOwnershipReconciliation.Domain.VIRTUAL_PARENTS,
        )
        for domain in domains:
            self._reconcile(self.older, domain)
        self.assertTrue(_ingestion_delete_refusal(self.older))

        for domain in domains:
            self._reconcile(self.newer, domain)

        self.assertEqual(_ingestion_delete_refusal(self.older), "")
        self.older.delete()
        self.assertFalse(ForwardIngestion.objects.filter(pk=self.older.pk).exists())
        self.assertEqual(
            ForwardOwnershipReconciliation.objects.filter(sync=self.sync).count(),
            len(domains),
            "the surviving rows belong to the newer ingestion and must remain",
        )

    def test_virtual_parent_claims_still_pin_their_ingestion(self):
        # Unchanged on purpose: a claim describes a live NetBox device, so its
        # provenance must survive.
        from dcim.models import Device
        from dcim.models import DeviceRole
        from dcim.models import DeviceType
        from dcim.models import Manufacturer
        from dcim.models import Site
        from django.db.models.deletion import ProtectedError

        from forward_netbox.models import ForwardVirtualParentClaim

        manufacturer = Manufacturer.objects.create(
            name="Cascade Manufacturer", slug="cascade-manufacturer"
        )
        device_type = DeviceType.objects.create(
            manufacturer=manufacturer, model="Cascade Model", slug="cascade-model"
        )
        role = DeviceRole.objects.create(name="Cascade Role", slug="cascade-role")
        site = Site.objects.create(name="Cascade Site", slug="cascade-site")
        child = Device.objects.create(
            name="cascade-child", device_type=device_type, role=role, site=site
        )
        parent = Device.objects.create(
            name="cascade-parent", device_type=device_type, role=role, site=site
        )
        ForwardVirtualParentClaim.objects.create(
            sync=self.sync,
            device=child,
            parent_device=parent,
            ingestion=self.newer,
            snapshot_id=self.newer.snapshot_id,
        )

        refusal, expected = _ingestion_delete_refusal_detail(self.newer)

        self.assertIn("ForwardVirtualParentClaim", refusal)
        self.assertFalse(expected)
        with self.assertRaises(ProtectedError):
            self.newer.delete()

    def test_the_baseline_ingestion_is_still_refused_and_still_protected(self):
        # The baseline protection must not be weakened by any of this, and its
        # message must stay the baseline one even when the ingestion also holds
        # current ownership evidence.
        from django.db.models.deletion import ProtectedError

        from forward_netbox.models import ForwardContributorBaseline
        from forward_netbox.models import ForwardOwnershipReconciliation

        for domain in (
            ForwardOwnershipReconciliation.Domain.STATUS_TAGS,
            ForwardOwnershipReconciliation.Domain.VIRTUAL_PARENTS,
        ):
            self._reconcile(self.newer, domain)
        ForwardContributorBaseline.objects.create(
            sync=self.sync,
            ingestion=self.newer,
            snapshot_id=self.newer.snapshot_id,
            network_fingerprint="nf",
            map_set_fingerprint="mf",
            scope_config_fingerprint="cf",
            scope_membership_fingerprint="sf",
            scope_payload_checksum="pc",
        )

        refusal, expected = _ingestion_delete_refusal_detail(self.newer)

        self.assertIn("is the baseline for this sync", refusal)
        self.assertTrue(expected)
        with self.assertRaises(ProtectedError):
            self.newer.delete()
