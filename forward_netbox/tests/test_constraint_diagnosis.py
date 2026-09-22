"""A constraint failure has to name the row, or it is not diagnosable.

A sync died on `dcim_device_unique_name_site` and the ingestion issue carried
the constraint name, the raise site, and nothing else. The Forward side had to
be ruled out by pulling every device row for the network by hand, because
nothing the operator could reach or send us said which device.

These tests pin the two shapes that actually occur - a new row colliding with
one already in NetBox, and two rows in one batch sharing a key - plus the
transaction assumption the whole thing rests on.
"""

from dcim.models import Device
from dcim.models import DeviceRole
from dcim.models import DeviceType
from dcim.models import Manufacturer
from dcim.models import Site
from django.db import connection
from django.db import IntegrityError
from django.db import transaction
from django.test import TestCase

from forward_netbox.utilities.constraint_diagnosis import annotate_integrity_error


class DeviceConstraintFixture(TestCase):
    def setUp(self):
        self.site = Site.objects.create(name="Site A", slug="site-a")
        manufacturer = Manufacturer.objects.create(name="Vendor", slug="vendor")
        self.device_type = DeviceType.objects.create(
            manufacturer=manufacturer, model="Model", slug="model"
        )
        self.role = DeviceRole.objects.create(name="Role", slug="role")

    def _device(self, name):
        return Device(
            name=name,
            site=self.site,
            device_type=self.device_type,
            role=self.role,
            status="active",
        )


class ExistingRowConflictTest(DeviceConstraintFixture):
    def test_the_conflicting_row_is_named_by_primary_key(self):
        existing = Device.objects.create(
            name="conflicted",
            site=self.site,
            device_type=self.device_type,
            role=self.role,
            status="active",
        )
        incoming = self._device("conflicted")

        with transaction.atomic():
            try:
                with transaction.atomic():
                    Device.objects.bulk_create([incoming])
            except IntegrityError as exc:
                annotate_integrity_error(
                    exc,
                    Device,
                    create_objects=[incoming],
                    using="default",
                )
                caught = exc

        diagnosis = caught.safe_diagnosis
        self.assertEqual(diagnosis["constraint_name"], "dcim_device_unique_name_site")
        self.assertTrue(diagnosis["constraint_fields_resolved"])
        self.assertEqual(sorted(diagnosis["constraint_fields"]), ["name", "site"])
        # NetBox writes this one as `Lower("name"), "site"`, so it is a
        # case-insensitive rule and the lookup that finds the conflict has to
        # be too.
        self.assertTrue(diagnosis["constraint_case_insensitive"])
        self.assertEqual(diagnosis["conflict_kind"], "existing_row")
        self.assertEqual(diagnosis["conflicting_pks"], [existing.pk])
        # The sentence an operator reads is driven by this attribute.
        self.assertEqual(caught.netbox_pk, existing.pk)

    def test_the_values_are_recorded_for_the_gui_tier_only(self):
        Device.objects.create(
            name="conflicted",
            site=self.site,
            device_type=self.device_type,
            role=self.role,
            status="active",
        )
        incoming = self._device("conflicted")

        with transaction.atomic():
            try:
                with transaction.atomic():
                    Device.objects.bulk_create([incoming])
            except IntegrityError as exc:
                annotate_integrity_error(
                    exc, Device, create_objects=[incoming], using="default"
                )
                caught = exc

        rows = caught.operator_detail["conflicting_rows"]
        self.assertEqual(rows[0]["name"], "conflicted")
        # And the value-free half never carries it.
        self.assertNotIn("conflicted", str(caught.safe_diagnosis))


class CaseInsensitiveConflictTest(DeviceConstraintFixture):
    def test_a_differently_cased_existing_row_is_still_found(self):
        """The failure a plain equality lookup would miss entirely.

        `dcim_device_unique_name_site` keys on `Lower(name)`, so NetBox rejects
        `CORE-SW-01` against a stored `core-sw-01` - and a diagnosis that
        looked up `name=` exactly would report no conflict on the very row the
        database just refused.
        """
        existing = Device.objects.create(
            name="core-sw-01",
            site=self.site,
            device_type=self.device_type,
            role=self.role,
            status="active",
        )
        incoming = self._device("CORE-SW-01")

        with transaction.atomic():
            try:
                with transaction.atomic():
                    Device.objects.bulk_create([incoming])
            except IntegrityError as exc:
                annotate_integrity_error(
                    exc, Device, create_objects=[incoming], using="default"
                )
                caught = exc

        self.assertEqual(caught.safe_diagnosis["conflict_kind"], "existing_row")
        self.assertEqual(caught.safe_diagnosis["conflicting_pks"], [existing.pk])


class InBatchDuplicateTest(DeviceConstraintFixture):
    def test_two_rows_in_one_write_sharing_a_key_are_distinguished(self):
        first = self._device("twinned")
        second = self._device("twinned")

        with transaction.atomic():
            try:
                with transaction.atomic():
                    Device.objects.bulk_create([first, second])
            except IntegrityError as exc:
                annotate_integrity_error(
                    exc,
                    Device,
                    create_objects=[first, second],
                    using="default",
                )
                caught = exc

        diagnosis = caught.safe_diagnosis
        # Not "existing_row": nothing was in the table, both rows were in the
        # statement. The two want opposite fixes, so they must not read alike.
        self.assertEqual(diagnosis["conflict_kind"], "in_batch_duplicate")
        self.assertEqual(diagnosis["conflict_count"], 1)


class DiagnosisIsSafeToRunTest(DeviceConstraintFixture):
    def test_a_select_is_legal_after_the_savepoint_rolled_back(self):
        """The assumption the whole helper rests on.

        Every bulk path nests `atomic` inside `atomic`, so the failed statement
        rolls back to a savepoint and the connection stays usable. If a
        refactor ever flattens that nesting, this fails loudly here instead of
        turning every diagnosis into `current transaction is aborted` on a
        customer's failing sync.
        """
        Device.objects.create(
            name="probe",
            site=self.site,
            device_type=self.device_type,
            role=self.role,
            status="active",
        )
        incoming = self._device("probe")

        with transaction.atomic():
            try:
                with transaction.atomic():
                    Device.objects.bulk_create([incoming])
            except IntegrityError:
                self.assertFalse(connection.needs_rollback)
                self.assertEqual(Device.objects.filter(name="probe").count(), 1)

    def test_a_broken_diagnosis_never_replaces_the_real_error(self):
        class Exploding:
            _meta = property(lambda self: 1 / 0)

        error = IntegrityError("original")
        annotate_integrity_error(
            error, Exploding(), create_objects=[object()], using="default"
        )

        self.assertEqual(str(error), "original")

    def test_an_unresolvable_constraint_says_so_rather_than_guessing(self):
        error = IntegrityError("no driver diagnostics here")

        annotate_integrity_error(error, Device, create_objects=[], using="default")

        self.assertFalse(error.safe_diagnosis["constraint_fields_resolved"])
        self.assertNotIn("constraint_fields", error.safe_diagnosis)
