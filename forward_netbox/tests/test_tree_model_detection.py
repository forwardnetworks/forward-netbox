"""The tree detector must prove it still detects something.

`_is_bulk_safe` decides whether a model may be `bulk_create`d during a merge.
Tree models may not: they recompute hierarchy state on save, and a bulk insert
skips it, leaving a corrupted tree with no error anywhere.

It asked one question - `issubclass(model_class, MPTTModel)` - and answering
that question wrongly is silent in the dangerous direction. NetBox 4.7 replaces
the deprecated `NestedGroupModel` with an ltree implementation. If django-mptt
goes with it, the import fails loudly and someone fixes it. If mptt merely
stays installed as somebody's transitive dependency while NetBox models stop
inheriting from it, `issubclass` answers False for every former tree model,
every one becomes "bulk safe", and the merge corrupts hierarchies quietly.

That is the shape of defect this codebase keeps finding: a check that reports a
confident answer about something it is no longer actually measuring. So the
detector is no longer trusted to be right by construction - this asserts it
still recognises a real nested-group model in the installed NetBox. When that
stops being true the suite fails, which is the whole point.
"""

from django.test import SimpleTestCase

from forward_netbox.utilities.bulk_merge import _is_bulk_safe
from forward_netbox.utilities.bulk_merge import _is_tree_model


class TheDetectorStillDetectsTest(SimpleTestCase):
    """Canary. These are real NetBox models, not fixtures."""

    def test_a_known_nested_group_model_is_recognised(self):
        from dcim.models import Region

        self.assertTrue(
            _is_tree_model(Region),
            "Region is a nested group in NetBox; a detector that calls it flat "
            "has stopped working and bulk_create will corrupt the hierarchy",
        )

    def test_known_nested_group_models_are_not_bulk_safe(self):
        from dcim.models import Location
        from dcim.models import Region
        from dcim.models import SiteGroup

        for model in (Region, SiteGroup, Location):
            with self.subTest(model=model.__name__):
                self.assertFalse(_is_bulk_safe(model))

    def test_a_flat_model_is_still_bulk_safe(self):
        # The detector must not answer "tree" for everything, which would be a
        # safe-but-useless way to pass the canary above.
        from dcim.models import Manufacturer
        from dcim.models import Site

        for model in (Site, Manufacturer):
            with self.subTest(model=model.__name__):
                self.assertTrue(_is_bulk_safe(model))


class DetectionSurvivesTheBaseClassMovingTest(SimpleTestCase):
    """The field signature is the half that outlives an upstream refactor."""

    def test_a_model_with_the_mptt_columns_is_a_tree_without_inheriting(self):
        class _Field:
            def __init__(self, name):
                self.name = name

        class _Meta:
            fields = [
                _Field(name)
                for name in ("id", "name", "lft", "rght", "tree_id", "level")
            ]

        class _NotAnMpttSubclass:
            _meta = _Meta()

        self.assertTrue(_is_tree_model(_NotAnMpttSubclass))
        self.assertFalse(_is_bulk_safe(_NotAnMpttSubclass))

    def test_a_partial_signature_is_not_enough(self):
        # `level` alone appears on plenty of flat models. All four columns
        # together are what identifies the bookkeeping.
        class _Field:
            def __init__(self, name):
                self.name = name

        class _Meta:
            fields = [_Field(name) for name in ("id", "name", "level")]

        class _Flat:
            _meta = _Meta()

        self.assertFalse(_is_tree_model(_Flat))
        self.assertTrue(_is_bulk_safe(_Flat))
