"""These models are not nested groups, and must not claim to be.

Every Forward API serializer inherited `NestedGroupModelSerializer`, whose only
addition over `OrganizationalModelSerializer` is

    _depth = serializers.IntegerField(source='level', read_only=True)

None of these models have a `level` field and none of them are hierarchical, so
the base class was inaccurate from the start. It was harmless only because
`_depth` was never listed in any `Meta.fields`, which is a thin reason for a
declaration to be wrong.

NetBox 4.7 replaces the nested-group implementation behind that base class,
swapping django-mptt for ltree. A serializer that names it is a serializer tied
to an implementation these models have no business depending on.

`OrganizationalModelSerializer` is the same class minus `_depth` - it keeps
`OwnerMixin` and `NetBoxModelSerializer` exactly - so the swap changes no
rendered field.
"""

from django.test import SimpleTestCase


class NoForwardSerializerClaimsToBeANestedGroupTest(SimpleTestCase):
    def _serializers(self):
        import inspect

        from rest_framework.serializers import Serializer

        from forward_netbox.api import serializers as module

        return [
            value
            for _name, value in inspect.getmembers(module, inspect.isclass)
            if issubclass(value, Serializer) and value.__module__ == module.__name__
        ]

    def test_the_module_does_not_import_the_nested_group_base(self):
        from forward_netbox.api import serializers as module

        self.assertFalse(
            hasattr(module, "NestedGroupModelSerializer"),
            "these models are flat; naming the nested-group base ties them to "
            "an implementation NetBox 4.7 replaces",
        )

    def test_no_serializer_inherits_the_nested_group_base(self):
        from netbox.api.serializers import NestedGroupModelSerializer

        found = self._serializers()
        self.assertTrue(found, "no serializers discovered; the check is vacuous")
        for serializer in found:
            with self.subTest(serializer=serializer.__name__):
                self.assertNotIsInstance(
                    serializer.__mro__,
                    type(None),
                )
                self.assertNotIn(
                    NestedGroupModelSerializer,
                    serializer.__mro__,
                    f"{serializer.__name__} is not a nested group",
                )

    def test_no_serializer_renders_a_depth_field(self):
        # The reason the wrong base was survivable. Pin it, so a future
        # `fields = "__all__"` cannot start emitting a depth for a flat model.
        for serializer in self._serializers():
            fields = getattr(getattr(serializer, "Meta", None), "fields", ())
            if fields in ("__all__", None):
                self.fail(
                    f"{serializer.__name__} uses `__all__`; enumerate fields so "
                    "an inherited one cannot appear unnoticed"
                )
            with self.subTest(serializer=serializer.__name__):
                self.assertNotIn("_depth", tuple(fields))
