"""Ingestions must be deletable, and a protected one must say why.

2.6.3 fixed a `NoReverseMatch` crash on the Ingestions list by removing the
delete action from the table rather than registering the view the action
pointed at. The crash went away and so did the ability to delete an ingestion —
reported by a customer against 2.6.5.

Most relations to an ingestion cascade. The contributor baseline is the one
that matters: the LIVE generation is durable convergence evidence and must
outlive nothing, while every superseded generation is an emptied husk that used
to protect its ingestion forever. That split is enforced by a `pre_delete`
receiver rather than by PROTECT, because PROTECT cannot express it, and it must
be reported rather than surfaced as an unhandled `ProtectedError`.
"""

from unittest.mock import patch

from django.contrib.auth import get_user_model
from django.db import transaction
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

    def test_the_live_baseline_blocks_the_delete_and_says_why(self):
        # The relation is CASCADE now so a spent baseline can be collected; the
        # live one is kept by the pre_delete receiver instead. Assert the shape
        # deliberately, so flipping it back is a failing test rather than a
        # silently lost guarantee.
        from django.db import models as django_models

        from forward_netbox.models import ForwardContributorBaseline

        field = ForwardContributorBaseline._meta.get_field("ingestion")
        self.assertIs(
            field.remote_field.on_delete,
            django_models.CASCADE,
            "ForwardContributorBaseline.ingestion is no longer CASCADE; the "
            "spent-baseline collection this test covers depends on it",
        )
        baseline = self._baseline(ForwardContributorBaseline)
        baseline.is_current = True
        baseline.status = ForwardContributorBaseline.Status.CURRENT
        baseline.save(update_fields=["is_current", "status"])

        refusal, expected = _ingestion_delete_refusal_detail(self.ingestion)

        self.assertTrue(refusal, "the live baseline must be refused")
        self.assertIn("current contributor baseline", refusal)
        # Expected rather than a fault; a customer reported the old red error
        # as a defect twice.
        self.assertIn("expected, not a failure", refusal)
        self.assertTrue(expected)

    def test_the_refusal_names_what_to_do(self):
        from forward_netbox.models import ForwardContributorBaseline

        baseline = self._baseline(ForwardContributorBaseline)
        baseline.is_current = True
        baseline.save(update_fields=["is_current"])
        self.assertIn("Run the sync again", _ingestion_delete_refusal(self.ingestion))

    def test_a_spent_baseline_is_collected_with_its_ingestion(self):
        # The customer case: every ingestion that ever promoted left a baseline
        # behind, and nothing removed it, so the backlog grew by one per
        # successful sync until three were undeletable at once.
        from forward_netbox.models import ForwardContributorBaseline

        baseline = self._baseline(ForwardContributorBaseline)
        baseline.status = ForwardContributorBaseline.Status.SUPERSEDED
        baseline.is_current = False
        baseline.save(update_fields=["status", "is_current"])

        self.assertEqual(_ingestion_delete_refusal(self.ingestion), "")

        self.ingestion.delete()

        self.assertFalse(ForwardIngestion.objects.filter(pk=self.ingestion.pk).exists())
        self.assertFalse(
            ForwardContributorBaseline.objects.filter(pk=baseline.pk).exists(),
            "the spent baseline must go with its ingestion, not outlive it",
        )

    def test_a_live_baseline_is_never_collected_even_by_a_queryset_delete(self):
        # The guarantee moved from PROTECT to a pre_delete receiver, and the
        # whole point of that receiver over a check in the view is that it fires
        # for querysets too.
        from django.db.models.deletion import ProtectedError

        from forward_netbox.models import ForwardContributorBaseline

        baseline = self._baseline(ForwardContributorBaseline)
        baseline.is_current = True
        baseline.status = ForwardContributorBaseline.Status.CURRENT
        baseline.save(update_fields=["is_current", "status"])

        # A refused delete aborts the transaction it ran in, so the assertions
        # after it need a surviving one.
        with transaction.atomic():
            with self.assertRaises(ProtectedError):
                ForwardIngestion.objects.filter(pk=self.ingestion.pk).delete()

        self.assertTrue(ForwardIngestion.objects.filter(pk=self.ingestion.pk).exists())
        self.assertTrue(
            ForwardContributorBaseline.objects.filter(pk=baseline.pk).exists()
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
        # The four provenance models resolve three different ways, and each is
        # deliberate. Reconciliation CASCADEs: it is a child record of the
        # ingestion. The other three SET_NULL: they describe live NetBox rows,
        # so their evidence must survive the run being deleted, but the stamp
        # naming that run is provenance rather than a dependency and must not
        # pin it. Pin the split so an edit to the shared mixin cannot quietly
        # collapse them onto one behaviour.
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
                field = model._meta.get_field("ingestion")
                self.assertIs(
                    field.remote_field.on_delete,
                    django_models.SET_NULL,
                )
                self.assertTrue(
                    field.null,
                    "SET_NULL requires the column to be nullable",
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

    def test_a_virtual_parent_claim_survives_its_ingestion_without_pinning_it(self):
        # A claim describes a live NetBox device, so the claim must survive the
        # run being deleted. It used to achieve that by refusing the delete,
        # which pinned the ingestion forever once the claim stopped being
        # re-pointed. SET_NULL keeps the claim and drops only the stamp.
        from dcim.models import Device
        from dcim.models import DeviceRole
        from dcim.models import DeviceType
        from dcim.models import Manufacturer
        from dcim.models import Site

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

        self.assertNotIn("ForwardVirtualParentClaim", refusal)

        self.newer.delete()

        claim = ForwardVirtualParentClaim.objects.get(device=child)
        self.assertIsNone(claim.ingestion_id)
        self.assertEqual(claim.parent_device_id, parent.pk)

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
            is_current=True,
            status=ForwardContributorBaseline.Status.CURRENT,
        )

        refusal, expected = _ingestion_delete_refusal_detail(self.newer)

        self.assertIn("current contributor baseline", refusal)
        self.assertTrue(expected)
        # The refusal is now raised by the pre_delete receiver rather than by a
        # PROTECT constraint, and it aborts the surrounding transaction either
        # way - so keep it in its own atomic block.
        with transaction.atomic():
            with self.assertRaises(ProtectedError):
                self.newer.delete()


class StaleProvenanceStampDoesNotPinTest(TestCase):
    """The customer's 2700/2709: pinned by ownership evidence, not by a child.

    A device that leaves Forward's scope stops being re-pointed. Its identity
    and tag claims freeze on the last ingestion that saw them, and while that
    stamp was PROTECT it pinned that ingestion permanently - one more
    undeletable ingestion for every scope change.

    The evidence is NOT stale. The device still exists in NetBox and is still
    owned, which is why deleting the evidence was never the right answer: a
    device leaves scope for entirely benign reasons, such as someone editing a
    tag in Forward. Only the stamp is old.
    """

    def setUp(self):
        from dcim.models import Device
        from dcim.models import DeviceRole
        from dcim.models import DeviceType
        from dcim.models import Manufacturer
        from dcim.models import Site

        self.user = get_user_model().objects.create_user(username="stale-stamp")
        source = ForwardSource.objects.create(
            name="stale-stamp-source",
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
            name="stale-stamp-sync",
            source=source,
            user=self.user,
            parameters={"snapshot_id": LATEST_PROCESSED_SNAPSHOT},
        )
        self.departed = ForwardIngestion.objects.create(
            sync=self.sync,
            snapshot_selector=LATEST_PROCESSED_SNAPSHOT,
            snapshot_id="snapshot-old",
        )
        site = Site.objects.create(name="stale-site", slug="stale-site")
        manufacturer = Manufacturer.objects.create(name="stale-mfr", slug="stale-mfr")
        device_type = DeviceType.objects.create(
            manufacturer=manufacturer, model="stale-model", slug="stale-model"
        )
        role = DeviceRole.objects.create(name="stale-role", slug="stale-role")
        self.device = Device.objects.create(
            name="stale-device", site=site, device_type=device_type, role=role
        )

    def test_evidence_frozen_on_an_old_ingestion_no_longer_pins_it(self):
        from forward_netbox.models import ForwardDeviceIdentity

        identity = ForwardDeviceIdentity.objects.create(
            sync=self.sync,
            ingestion=self.departed,
            source_device_key="stale-device",
            device=self.device,
            snapshot_id="snapshot-old",
        )
        self.assertEqual(_ingestion_delete_refusal(self.departed), "")

        self.departed.delete()

        self.assertFalse(ForwardIngestion.objects.filter(pk=self.departed.pk).exists())
        identity.refresh_from_db()
        # The ownership survives intact; only the pointer to the deleted run is
        # gone. Releasing it would hand a live device to whatever claims it next.
        self.assertIsNone(identity.ingestion_id)
        self.assertEqual(identity.sync_id, self.sync.pk)
        self.assertEqual(identity.device_id, self.device.pk)
        self.assertEqual(identity.source_device_key, "stale-device")
        self.assertEqual(identity.snapshot_id, "snapshot-old")

    def test_a_frozen_tag_claim_does_not_pin_its_ingestion_either(self):
        from extras.models import Tag

        from forward_netbox.models import ForwardDeviceTagClaim

        tag = Tag.objects.create(name="Departed.Owner", slug="departedowner")
        claim = ForwardDeviceTagClaim.objects.create(
            sync=self.sync,
            ingestion=self.departed,
            device=self.device,
            tag=tag,
            claim_type="scope",
            snapshot_id="snapshot-old",
        )
        self.assertEqual(_ingestion_delete_refusal(self.departed), "")

        self.departed.delete()

        claim.refresh_from_db()
        self.assertIsNone(claim.ingestion_id)
        self.assertEqual(claim.device_id, self.device.pk)
        self.assertEqual(claim.claim_type, "scope")

    def test_a_null_stamp_is_not_counted_as_a_cross_sync_mismatch(self):
        # `exclude(ingestion__sync_id=F("sync_id"))` compiles to a LEFT OUTER
        # JOIN guarded by `ingestion.sync_id IS NOT NULL`, so a null stamp
        # satisfies the negation and would be counted as a stamp pointing at
        # ANOTHER sync's ingestion. It is not that; it is an absent stamp.
        #
        # This would never have self-corrected: an identity for a device that
        # left Forward's scope is never re-stamped, so the count would stay
        # non-zero forever - failing `forward_ownership_audit
        # --fail-on-inconsistent`, warning the ownership health check, and
        # stopping stuck-recovery short-circuiting on a converged sync.
        from forward_netbox.models import ForwardDeviceIdentity
        from forward_netbox.utilities.ownership import ownership_integrity_summary

        ForwardDeviceIdentity.objects.create(
            sync=self.sync,
            ingestion=self.departed,
            source_device_key="stale-device",
            device=self.device,
            snapshot_id="snapshot-old",
        )
        self.departed.delete()

        self.assertEqual(ownership_integrity_summary()["provenance_sync_mismatches"], 0)
