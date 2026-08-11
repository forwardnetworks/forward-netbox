# The customer's live blocker on 2.7.6, named by the conflict catalogue as
# `tag-mutation-identity-unresolved`.
#
# Forward reported eleven devices carrying the include tags that did not exist
# in their NetBox, out of roughly 3400. `reconcile_source_device_tag_claims`
# refused the ENTIRE mutation if any name failed to resolve, so both the
# scope-tag and status-tag domains failed on every run: ownership never
# completed, convergence stayed blocked, and every drift figure read "Not
# measured" with no remedy available to them.
#
# The two resolution failures are deliberately treated differently, and these
# tests pin both halves:
#
#   missing   - no device of that name exists, so there is nothing to tag and
#               nothing to release. Skipping changes no NetBox row.
#   ambiguous - several devices share the name. `desired_ids` drives both the
#               add and the remove, so dropping the key could release a claim
#               from a device that holds one, or tag the wrong device.
#
# Ambiguity was originally a refusal for exactly that reason, and the same
# customer then sat behind THAT for a release: one tie out of roughly 3400
# names failed both tag domains on every run, so ownership still never
# completed and drift still read "Not measured". Refusing a whole domain to
# protect one name was the same all-or-nothing mistake made twice. An ambiguous
# name is now HELD - never added, never released, refreshed to the current
# generation so it does not pin `stale_claims` - and the tests below pin the
# hold, the refresh, and the fact that the domain completes regardless.
from dcim.models import Device
from dcim.models import DeviceRole
from dcim.models import DeviceType
from dcim.models import Manufacturer
from dcim.models import Site
from django.test import TestCase

from forward_netbox.models import ForwardDeviceIdentity
from forward_netbox.models import ForwardDeviceTagClaim
from forward_netbox.models import ForwardIngestion
from forward_netbox.models import ForwardOwnershipReconciliation
from forward_netbox.models import ForwardSource
from forward_netbox.models import ForwardSync
from forward_netbox.utilities.ownership import device_name_ambiguity_report
from forward_netbox.utilities.ownership import reconcile_source_device_tag_claims

SLUG = "s-example"


class AbsentDeviceDoesNotBlockTagDomainTest(TestCase):
    def setUp(self):
        source = ForwardSource.objects.create(
            name="absent-src",
            type="saas",
            url="https://fwd.app",
            parameters={"network_id": "net-1"},
        )
        self.sync = ForwardSync.objects.create(name="absent-sync", source=source)
        self.ingestion = ForwardIngestion.objects.create(
            sync=self.sync, snapshot_id="snap-absent"
        )
        self.site = Site.objects.create(name="absent-site", slug="absent-site")
        manufacturer = Manufacturer.objects.create(name="absent-mfr", slug="absent-mfr")
        self.device_type = DeviceType.objects.create(
            manufacturer=manufacturer, model="am", slug="am"
        )
        self.role = DeviceRole.objects.create(name="absent-role", slug="absent-role")
        self.present = self._device("present-device")

    def _device(self, name, site=None):
        return Device.objects.create(
            name=name,
            site=site or self.site,
            device_type=self.device_type,
            role=self.role,
        )

    def _twin_named(self, name):
        """Two devices sharing a name. NetBox scopes name uniqueness to the
        site, so genuine ambiguity means the same name in two sites - which is
        precisely how it arises in a real estate."""
        second_site = Site.objects.create(
            name=f"{name}-other-site", slug=f"{name}-other-site"
        )
        return self._device(name), self._device(name, site=second_site)

    def _tag(self):
        from extras.models import Tag

        tag, _created = Tag.objects.get_or_create(
            slug=SLUG,
            defaults={"name": "S.Example", "color": "9e9e9e"},
        )
        return tag

    def _claim(self, device, ingestion=None):
        ingestion = ingestion or self.ingestion
        return ForwardDeviceTagClaim.objects.create(
            sync=self.sync,
            device=device,
            tag=self._tag(),
            claim_type="scope",
            ingestion_id=ingestion.pk,
            snapshot_id=ingestion.snapshot_id,
        )

    def _reconcile(self, names):
        return reconcile_source_device_tag_claims(
            self.sync,
            names,
            slug=SLUG,
            name="S.Example",
            color="9e9e9e",
            description="",
            claim_type="scope",
            generation=self.ingestion.pk,
            snapshot_id=self.ingestion.snapshot_id,
        )

    def test_a_device_absent_from_netbox_is_skipped_not_fatal(self):
        # The customer's shape: most names resolve, a few do not exist at all.
        result = self._reconcile({"present-device", "never-existed"})

        self.assertEqual(result["skipped_absent_devices"], 1)
        self.assertTrue(
            ForwardDeviceTagClaim.objects.filter(device=self.present).exists(),
            "the devices that DO exist were not claimed",
        )

    def test_the_domain_completes_despite_an_absent_device(self):
        self._reconcile({"present-device", "never-existed"})

        domain = ForwardOwnershipReconciliation.objects.filter(
            sync=self.sync,
            domain=ForwardOwnershipReconciliation.Domain.SCOPE_TAGS,
        ).first()
        self.assertIsNotNone(domain)
        self.assertEqual(
            domain.status,
            ForwardOwnershipReconciliation.Status.COMPLETED,
            "ownership still did not complete, so convergence stays blocked",
        )

    def test_only_absent_names_still_completes(self):
        # Nothing resolves. There is still nothing unsafe about proceeding.
        result = self._reconcile({"never-existed", "also-never-existed"})
        self.assertEqual(result["skipped_absent_devices"], 2)
        self.assertEqual(result["total"], 0)

    def test_an_ambiguous_name_no_longer_refuses(self):
        # Two devices share a name. Neither is tagged - that would be a guess -
        # but the rest of the mutation proceeds.
        self._twin_named("twin")

        result = self._reconcile({"present-device", "twin"})

        self.assertEqual(result["ambiguous_device_names"], 1)
        self.assertTrue(
            ForwardDeviceTagClaim.objects.filter(device=self.present).exists(),
            "the unambiguous devices were not claimed",
        )

    def test_an_ambiguous_name_is_never_claimed_on_a_guess(self):
        first, second = self._twin_named("twin")

        self._reconcile({"twin"})

        self.assertFalse(
            ForwardDeviceTagClaim.objects.filter(device__in=[first, second]).exists(),
            "a device was tagged on a name that identifies two of them",
        )

    def test_an_ambiguous_name_does_not_release_a_claim_it_already_holds(self):
        # The reason ambiguity was fatal: `desired_ids` drives the remove too,
        # so a held name must be excluded from the release set, not dropped.
        first, second = self._twin_named("twin")
        claim = self._claim(first)

        result = self._reconcile({"present-device", "twin"})

        self.assertEqual(result["held_ambiguous_devices"], 1)
        self.assertTrue(
            ForwardDeviceTagClaim.objects.filter(pk=claim.pk).exists(),
            "a claim was released for a device whose name is ambiguous",
        )
        self.assertFalse(
            ForwardDeviceTagClaim.objects.filter(device=second).exists(),
            "the other candidate was claimed on a guess",
        )

    def test_a_name_that_left_forward_is_still_released(self):
        # The hold covers names Forward still reports. A name absent from this
        # mutation is not held by anything, ambiguous or not - otherwise a
        # device that genuinely left scope could never be untagged.
        first, _second = self._twin_named("twin")
        claim = self._claim(first)

        self._reconcile({"present-device"})

        self.assertFalse(ForwardDeviceTagClaim.objects.filter(pk=claim.pk).exists())

    def test_a_held_claim_is_refreshed_so_it_does_not_pin_integrity(self):
        # A held claim left at an older generation counts toward `stale_claims`,
        # which feeds `integrity_issue_count`, which gates `complete`. Holding
        # without refreshing would swap one permanent block for another.
        first, _second = self._twin_named("twin")
        older = ForwardIngestion.objects.create(
            sync=self.sync, snapshot_id="snap-older"
        )
        claim = self._claim(first, ingestion=older)

        result = self._reconcile({"present-device", "twin"})

        claim.refresh_from_db()
        self.assertEqual(claim.ingestion_id, self.ingestion.pk)
        self.assertEqual(result["held_ambiguous_devices"], 1)

    def test_the_domain_completes_despite_an_ambiguous_name(self):
        self._twin_named("twin")

        self._reconcile({"present-device", "twin"})

        domain = ForwardOwnershipReconciliation.objects.filter(
            sync=self.sync,
            domain=ForwardOwnershipReconciliation.Domain.SCOPE_TAGS,
        ).first()
        self.assertIsNotNone(domain)
        self.assertEqual(
            domain.status,
            ForwardOwnershipReconciliation.Status.COMPLETED,
            "ownership still did not complete, so convergence stays blocked",
        )

    def test_a_name_whose_only_twin_belongs_to_another_source_key_resolves(self):
        # The common shape, and not really a tie at all: NetBox scopes device
        # name uniqueness to the site, so a device that moves site or is
        # re-created leaves two rows - and one of them is already bound to a
        # different Forward device. Binding this name to that row could never
        # have succeeded, so excluding it leaves exactly one candidate.
        first, second = self._twin_named("twin")
        ForwardDeviceIdentity.objects.create(
            sync=self.sync,
            source_device_key="a-different-forward-device",
            device=first,
            ingestion_id=self.ingestion.pk,
            snapshot_id=self.ingestion.snapshot_id,
        )

        result = self._reconcile({"twin"})

        self.assertEqual(result["ambiguous_device_names"], 0)
        self.assertTrue(
            ForwardDeviceTagClaim.objects.filter(device=second).exists(),
            "the one candidate that was free was not claimed",
        )
        self.assertFalse(ForwardDeviceTagClaim.objects.filter(device=first).exists())

    def test_a_device_whose_forward_name_changed_keeps_its_tags(self):
        # The regression the missing/held distinction exists to prevent. The
        # device's OLD identity row still points at it (departed source keys are
        # never pruned), so under the new name every candidate is excluded as
        # already-bound. Treating that as "absent" would strip its Forward tags
        # silently on every run.
        renamed = self._device("new-forward-name")
        ForwardDeviceIdentity.objects.create(
            sync=self.sync,
            source_device_key="old-forward-name",
            device=renamed,
            ingestion_id=self.ingestion.pk,
            snapshot_id=self.ingestion.snapshot_id,
        )
        claim = self._claim(renamed)

        result = self._reconcile({"present-device", "new-forward-name"})

        self.assertTrue(
            ForwardDeviceTagClaim.objects.filter(pk=claim.pk).exists(),
            "a renamed device was silently untagged",
        )
        self.assertEqual(result["skipped_absent_devices"], 0, "it is not absent")
        self.assertEqual(result["held_ambiguous_devices"], 1)

    def test_the_audit_names_the_devices_behind_a_held_name(self):
        # The reconcile result reports a count because it is persisted. The
        # audit runs on the operator's own console, where the names may be
        # shown - a count with nothing to act on is not a diagnosis.
        first, second = self._twin_named("twin")

        report = device_name_ambiguity_report(self.sync)

        self.assertEqual(report["ambiguous_names"], ["twin"])
        self.assertEqual(
            sorted(entry["device_id"] for entry in report["devices"]["twin"]),
            sorted([first.pk, second.pk]),
        )

    def test_the_audit_excludes_a_pair_that_an_existing_binding_resolves(self):
        first, _second = self._twin_named("twin")
        ForwardDeviceIdentity.objects.create(
            sync=self.sync,
            source_device_key="a-different-forward-device",
            device=first,
            ingestion_id=self.ingestion.pk,
            snapshot_id=self.ingestion.snapshot_id,
        )

        report = device_name_ambiguity_report(self.sync)

        self.assertEqual(report["ambiguous_names"], [])
        self.assertEqual(report["resolved_by_existing_binding"], ["twin"])
        self.assertEqual(report["duplicated_names"], 1)

    def test_a_fully_resolvable_run_is_unchanged(self):
        other = self._device("second-device")

        result = self._reconcile({"present-device", "second-device"})

        self.assertEqual(result["skipped_absent_devices"], 0)
        self.assertEqual(result["total"], 2)
        self.assertTrue(ForwardDeviceTagClaim.objects.filter(device=other).exists())
