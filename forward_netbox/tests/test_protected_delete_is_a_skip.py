"""A ProtectedError on any delete path is a SKIP, never a failed row.

A customer's 2.8.2 sync recorded:

    dcim.device row processing failed (ProtectedError).

Three things wrong with that one line. It names neither what held the device
nor which device it was - but the word that matters is *failed*. A skipped row
is a row the sync declined to change; a FAILED row blocks baseline promotion
permanently, so the drift report goes back to reading "Not measured" for the
entire deployment. Not deleting a device is a small thing. Wedging the
convergence bookkeeping over it is not.

`delete_by_coalesce` had converted `ProtectedError` into a
`ForwardDependencySkipError` for years. The device delete simply did not go
through it: inside a branch it deletes through the branch collector, so the
exception went straight past the conversion into the generic `except
Exception` handler, which records a failure.

The lesson is the sibling-branch one. A guard that exists on one delete path
and not on the other is the same defect as no guard at all, for whichever path
lacks it.
"""

from django.db.models.deletion import ProtectedError
from django.test import SimpleTestCase

from forward_netbox.exceptions import ForwardDependencySkipError
from forward_netbox.utilities.sync_primitives import protected_delete_skip


class _FakeMeta:
    label_lower = "dcim.device"


class _FakeModel:
    _meta = _FakeMeta()


class _FakeDevice:
    pk = 4242


class ProtectedDeleteSkipTest(SimpleTestCase):
    def _skip(self, protected_objects=(), obj=None):
        error = ProtectedError("blocked", protected_objects)
        return protected_delete_skip(
            _FakeModel, error, obj=obj, context={"name": "a-device"}
        )

    def test_it_is_a_skip_not_a_failure(self):
        # The whole point: ForwardDependencySkipError is what the recorder
        # words as "skipped" and counts as a skip.
        self.assertIsInstance(self._skip(), ForwardDependencySkipError)

    def test_it_records_the_direction(self):
        skip = self._skip()
        self.assertTrue(skip.dependency_is_protecting)

    def test_it_names_the_row(self):
        skip = self._skip(obj=_FakeDevice())
        self.assertEqual("4242", skip.netbox_pk)

    def test_it_survives_an_object_without_a_pk(self):
        # A diagnostic on an error path must never replace the error.
        self.assertIsNone(self._skip(obj=object()).netbox_pk)

    def test_it_carries_the_model_string(self):
        self.assertEqual("dcim.device", self._skip().model_string)

    def test_the_context_is_carried_for_redaction_not_display(self):
        # `context` is reduced to key names by the recorder; it is passed so
        # the shape is recorded, never the values.
        self.assertEqual({"name": "a-device"}, self._skip().context)


class ProtectedObjectsAreNamedTest(SimpleTestCase):
    """The dependency is the actionable half, and it is schema-only."""

    def test_it_names_the_models_still_referencing_the_row(self):
        from dcim.models import Interface

        skip = protected_delete_skip(
            _FakeModel,
            ProtectedError("blocked", [Interface()]),
            obj=_FakeDevice(),
        )
        self.assertIn("dcim.interface", skip.dependency)

    def test_an_unreadable_protected_set_still_yields_a_skip(self):
        # `_protecting_model_labels` swallows its own failures on purpose: a
        # diagnostic that cannot be computed must not replace the skip.
        class _Hostile:
            @property
            def protected_objects(self):
                raise RuntimeError("nope")

        error = ProtectedError("blocked", ())
        error.__class__ = type("_E", (ProtectedError,), {})
        skip = protected_delete_skip(_FakeModel, error, obj=_FakeDevice())
        self.assertIsInstance(skip, ForwardDependencySkipError)
        self.assertEqual("4242", skip.netbox_pk)
