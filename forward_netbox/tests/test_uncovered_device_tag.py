"""The ``forward-uncovered`` tag: the bucket an operator could not enumerate.

A customer reported 552 devices under "carry no include tag" while orphans read
0, and asked which ones they were. Nothing could answer him. The panel showed a
count and a 25-name sample, the CLI audit prints that same truncated sample, and
the one badge that looked like a filter linked to `?tag_id__n=&q=` - both
parameters empty, so it opened the unfiltered device list.

The other two buckets have always been answerable, and for one reason: a
maintained tag is applied to them, so `?tag=forward-backfilled` lists every one.
This tag closes the gap the same way rather than a new way.

Only the `owned_untagged` half is tagged. The `unclaimed` half is another
source's or an operator's, and a device this sync never created is not this
sync's to label.
"""

from unittest.mock import Mock
from unittest.mock import patch
from uuid import uuid4

from core.choices import JobStatusChoices
from core.models import Job
from dcim.models import Device
from dcim.models import DeviceRole
from dcim.models import DeviceType
from dcim.models import Manufacturer
from dcim.models import Site
from django.contrib.auth import get_user_model
from django.contrib.contenttypes.models import ContentType
from django.test import Client
from django.test import TestCase
from django.urls import reverse

from forward_netbox.choices import ForwardSyncStatusChoices
from forward_netbox.models import ForwardDeviceIdentity
from forward_netbox.models import ForwardIngestion
from forward_netbox.models import ForwardSource
from forward_netbox.models import ForwardSync
from forward_netbox.utilities.scope_reconciliation import tag_backfilled_devices
from forward_netbox.utilities.scope_reconciliation import UNCOVERED_TAG_SLUG


class UncoveredDeviceTagTest(TestCase):
    def setUp(self):
        self.source = ForwardSource.objects.create(
            name="unc-src",
            type="saas",
            url="https://fwd.app",
            status="ready",
            parameters={
                "username": "u@example.com",
                "password": "p",
                "verify": True,
                "network_id": "net-1",
                "device_tag_include_tags": ["Prod_Core"],
                "device_tag_include_match": "any",
            },
        )
        self.sync = ForwardSync.objects.create(
            name="unc-sync",
            source=self.source,
            status=ForwardSyncStatusChoices.COMPLETED,
            parameters={"snapshot_id": "latestProcessed"},
        )
        self.ingestion = ForwardIngestion.objects.create(
            sync=self.sync,
            snapshot_id="snap-1",
            baseline_ready=True,
        )
        mfr = Manufacturer.objects.create(name="MfrN", slug="mfr-n")
        self.dt = DeviceType.objects.create(manufacturer=mfr, model="dt-n", slug="dt-n")
        self.role = DeviceRole.objects.create(name="RoleN", slug="role-n")
        self.site = Site.objects.create(name="SiteN", slug="site-n")

    def _device(self, name):
        return Device.objects.create(
            name=name, device_type=self.dt, role=self.role, site=self.site
        )

    def _own(self, device, *, sync=None, key=None):
        """Record that a sync created this device, without claiming scope.

        Deliberately not via `reconcile_sync_scope_tag_claims`: that also writes
        a scope claim, which would make the device an ORPHAN once it left the
        result. The shape being reproduced here is the customer's - orphans 0,
        uncovered large - and it needs ownership without a surviving claim.
        """
        return ForwardDeviceIdentity.objects.create(
            sync=sync or self.sync,
            source_device_key=key or device.name,
            device=device,
            ingestion_id=self.ingestion.pk,
            snapshot_id="snap-1",
        )

    def _run(self, rows):
        fwd_client = Mock()
        fwd_client.run_nqe_query = Mock(return_value=rows)
        with (
            patch.object(ForwardSource, "get_client", return_value=fwd_client),
            patch.object(ForwardSync, "resolve_snapshot_id", return_value="snap-1"),
        ):
            return tag_backfilled_devices(self.sync)

    def _tagged(self, slug):
        return set(
            Device.objects.filter(tags__slug=slug).values_list("name", flat=True)
        )

    # --- what the tag is for ------------------------------------------------

    def test_a_device_this_sync_created_and_no_longer_covers_is_tagged(self):
        self._device("dev-covered")
        self._own(self._device("dev-gone"))

        result = self._run([{"name": "dev-covered", "completed": True}])

        self.assertEqual(self._tagged(UNCOVERED_TAG_SLUG), {"dev-gone"})
        self.assertEqual(result["total_uncovered"], 1)

    def test_it_is_applied_even_though_the_device_is_not_an_orphan(self):
        """The customer's exact shape, and the whole reason this tag exists.

        An orphan is a device this sync PREVIOUSLY CLAIMED and no longer sees.
        A device it created but never held a scope claim for is not an orphan of
        it, so `forward-out-of-scope` reads zero - and before this tag there was
        nothing else to filter on.
        """
        self._device("dev-covered")
        self._own(self._device("dev-gone"))

        self._run([{"name": "dev-covered", "completed": True}])

        self.assertEqual(self._tagged("forward-out-of-scope"), set())
        self.assertEqual(self._tagged(UNCOVERED_TAG_SLUG), {"dev-gone"})

    def test_a_device_this_sync_never_created_is_not_tagged(self):
        self._device("dev-covered")
        self._device("someone-elses")

        self._run([{"name": "dev-covered", "completed": True}])

        self.assertEqual(self._tagged(UNCOVERED_TAG_SLUG), set())

    def test_a_covered_device_is_not_tagged(self):
        self._own(self._device("dev-covered"))

        self._run([{"name": "dev-covered", "completed": True}])

        self.assertEqual(self._tagged(UNCOVERED_TAG_SLUG), set())

    def test_the_tag_is_removed_when_the_device_returns_to_the_result(self):
        """A device disabled in Forward is the common case, and it comes back.

        The tag has to follow the bucket in both directions or it becomes a
        permanent mark on devices that are fine - the failure mode the count it
        replaces already had.
        """
        self._device("dev-covered")
        self._own(self._device("dev-gone"))

        self._run([{"name": "dev-covered", "completed": True}])
        self.assertEqual(self._tagged(UNCOVERED_TAG_SLUG), {"dev-gone"})

        result = self._run(
            [
                {"name": "dev-covered", "completed": True},
                {"name": "dev-gone", "completed": True},
            ]
        )

        self.assertEqual(self._tagged(UNCOVERED_TAG_SLUG), set())
        self.assertEqual(result["total_uncovered"], 0)

    def test_a_backfilled_device_is_not_uncovered(self):
        """Backfilled is the opposite assertion and must not overlap.

        `forward-backfilled` says the device IS in scope but was not freshly
        collected. It is still in the tag-scope result, so it is covered.
        """
        self._own(self._device("dev-backfilled"))

        self._run([{"name": "dev-backfilled", "completed": False}])

        self.assertEqual(self._tagged("forward-backfilled"), {"dev-backfilled"})
        self.assertEqual(self._tagged(UNCOVERED_TAG_SLUG), set())

    # --- the cross-sync rule ------------------------------------------------

    def test_a_device_another_sync_still_scopes_is_not_desired(self):
        """A per-sync absence must never be published as a global verdict.

        `uncovered` is a negative status tag, so it carries the same rule as
        `out_of_scope`: subtract every positive scope claim, whoever holds it.
        Without this the tag lands on devices another source is actively
        managing, and says something false about them.
        """
        from forward_netbox.models import ForwardDeviceTagClaim
        from forward_netbox.utilities.ownership import _desired_tag_device_ids
        from forward_netbox.utilities.scope_reconciliation import (
            UNCOVERED_TAG_NAME,
        )
        from extras.models import Tag

        device = self._device("dev-shared")
        other_source = ForwardSource.objects.create(
            name="other-src",
            type="saas",
            url="https://fwd.app",
            status="ready",
            parameters={**self.source.parameters},
        )
        other_sync = ForwardSync.objects.create(
            name="other-sync",
            source=other_source,
            parameters={"snapshot_id": "latestProcessed"},
        )
        uncovered_tag = Tag.objects.create(
            slug=UNCOVERED_TAG_SLUG, name=UNCOVERED_TAG_NAME
        )
        scope_tag = Tag.objects.create(slug="prod-core", name="Prod_Core")
        ForwardDeviceTagClaim.objects.create(
            sync=self.sync,
            device=device,
            tag=uncovered_tag,
            claim_type="uncovered",
            ingestion_id=self.ingestion.pk,
            snapshot_id="snap-1",
        )

        self.assertEqual(
            _desired_tag_device_ids(uncovered_tag.pk, "uncovered"), {device.pk}
        )

        other_ingestion = ForwardIngestion.objects.create(
            sync=other_sync, snapshot_id="snap-1", baseline_ready=True
        )
        ForwardDeviceTagClaim.objects.create(
            sync=other_sync,
            device=device,
            tag=scope_tag,
            claim_type="scope",
            ingestion_id=other_ingestion.pk,
            snapshot_id="snap-1",
        )

        self.assertEqual(_desired_tag_device_ids(uncovered_tag.pk, "uncovered"), set())


class MaintainedStatusTagContractTest(TestCase):
    """Pins the lists a fourth status tag would have to be added to.

    Each of these was a literal pair spelled inline, and a claim type missing
    from any one of them fails silently: the claims accumulate correctly and the
    tag is simply never applied to anything.
    """

    def test_status_claim_types_cover_every_non_scope_claim_type(self):
        from forward_netbox.models import ForwardDeviceTagClaim
        from forward_netbox.utilities.ownership import STATUS_CLAIM_TYPES

        self.assertEqual(
            set(STATUS_CLAIM_TYPES),
            {
                value
                for value, _label in ForwardDeviceTagClaim.ClaimType.choices
                if value != "scope"
            },
        )

    def test_both_claim_type_enums_agree(self):
        from forward_netbox.models import ForwardDeviceTagClaim
        from forward_netbox.models import ForwardManagedDeviceTag

        self.assertEqual(
            set(ForwardDeviceTagClaim.ClaimType.values),
            set(ForwardManagedDeviceTag.ClaimType.values),
        )

    def test_every_maintained_tag_description_fits_the_column(self):
        """`Tag.description` is 200 characters and the write is not truncated.

        A longer one raises `DataError` at tag-creation time, which is the FIRST
        run on a deployment that has never had this bucket - so the failure
        lands on a customer, not here. Caught exactly this way.
        """
        from extras.models import Tag
        from forward_netbox.utilities.scope_reconciliation import (
            BACKFILLED_TAG_DESCRIPTION,
        )
        from forward_netbox.utilities.scope_reconciliation import (
            OUT_OF_SCOPE_TAG_DESCRIPTION,
        )
        from forward_netbox.utilities.scope_reconciliation import (
            UNCOVERED_TAG_DESCRIPTION,
        )

        limit = Tag._meta.get_field("description").max_length
        for description in (
            BACKFILLED_TAG_DESCRIPTION,
            OUT_OF_SCOPE_TAG_DESCRIPTION,
            UNCOVERED_TAG_DESCRIPTION,
        ):
            self.assertLessEqual(len(description), limit, description)

    def test_every_maintained_status_slug_is_reserved(self):
        """An include tag normalizing onto a status slug is refused up front.

        The managed-tag registry allows a slug exactly one claim type, so the
        collision would otherwise surface as an `OwnershipConflictError` on
        every run, with no remedy but renaming the tag in Forward.
        """
        from forward_netbox.utilities.scope_reconciliation import BACKFILLED_TAG_SLUG
        from forward_netbox.utilities.scope_reconciliation import OUT_OF_SCOPE_TAG_SLUG
        from forward_netbox.utilities.tag_contracts import RESERVED_STATUS_TAG_SLUGS

        self.assertEqual(
            RESERVED_STATUS_TAG_SLUGS,
            frozenset({BACKFILLED_TAG_SLUG, OUT_OF_SCOPE_TAG_SLUG, UNCOVERED_TAG_SLUG}),
        )


class UncoveredPanelTest(TestCase):
    """The panel defects the tag was added to fix."""

    def setUp(self):
        self.source = ForwardSource.objects.create(
            name="panel-src",
            type="saas",
            url="https://fwd.app",
            status="ready",
            parameters={
                "username": "u@example.com",
                "password": "p",
                "verify": True,
                "network_id": "net-1",
                "device_tag_include_tags": ["Prod_Core"],
                "device_tag_include_match": "any",
            },
        )
        self.sync = ForwardSync.objects.create(
            name="panel-sync",
            source=self.source,
            status=ForwardSyncStatusChoices.COMPLETED,
            parameters={"snapshot_id": "latestProcessed"},
        )
        self.ingestion = ForwardIngestion.objects.create(
            sync=self.sync, snapshot_id="snap-1", baseline_ready=True
        )
        mfr = Manufacturer.objects.create(name="MfrP", slug="mfr-p")
        self.dt = DeviceType.objects.create(manufacturer=mfr, model="dt-p", slug="dt-p")
        self.role = DeviceRole.objects.create(name="RoleP", slug="role-p")
        self.site = Site.objects.create(name="SiteP", slug="site-p")

    def _render(self):
        device = Device.objects.create(
            name="dev-uncovered", device_type=self.dt, role=self.role, site=self.site
        )
        ForwardDeviceIdentity.objects.create(
            sync=self.sync,
            source_device_key="dev-uncovered",
            device=device,
            ingestion_id=self.ingestion.pk,
            snapshot_id="snap-1",
        )
        Device.objects.create(
            name="dev-unclaimed", device_type=self.dt, role=self.role, site=self.site
        )
        fwd_client = Mock()
        fwd_client.run_nqe_query = Mock(return_value=[])
        user = get_user_model().objects.create_user(username="admin-unc", password="x")
        user.is_superuser = True
        user.is_staff = True
        user.save()
        client = Client()
        client.force_login(user)
        with (
            patch.object(ForwardSource, "get_client", return_value=fwd_client),
            patch.object(ForwardSync, "resolve_snapshot_id", return_value="snap-1"),
        ):
            from forward_netbox.jobs import _scope_reconciliation_work

            job = Job.objects.create(
                object_type=ContentType.objects.get_for_model(ForwardSync),
                object_id=self.sync.pk,
                name=f"{self.sync.name} - scope reconciliation",
                status=JobStatusChoices.STATUS_COMPLETED,
                job_id=uuid4(),
            )
            _scope_reconciliation_work(job)
            return client.get(
                reverse(
                    "plugins:forward_netbox:forwardsync_scope_reconciliation",
                    kwargs={"pk": self.sync.pk},
                )
            )

    def test_the_badge_links_to_a_real_filter(self):
        """THE defect. The href had both parameters empty.

        `?tag_id__n=&q=` is not a filter - NetBox ignores empty values - so the
        one actionable badge on the panel opened the full device list.
        """
        response = self._render()

        self.assertEqual(response.status_code, 200)
        self.assertContains(response, f"?tag={UNCOVERED_TAG_SLUG}")
        self.assertNotContains(response, "tag_id__n=&amp;q=")

    def test_the_sample_names_are_rendered(self):
        """They were computed and returned, and then never displayed.

        Every other bucket on this page renders its sample. This one did not,
        so the split was two bare numbers with no way to see behind them.
        """
        response = self._render()

        self.assertContains(response, "dev-uncovered")
        self.assertContains(response, "dev-unclaimed")
        self.assertContains(response, "Uncovered, Created by This Sync")
        self.assertContains(response, "Uncovered, Not Claimed by This Sync")
