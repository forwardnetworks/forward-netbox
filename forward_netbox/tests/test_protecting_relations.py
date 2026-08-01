# A customer could not delete three ingestions in a row, and each new sync
# added another. The delete view is supposed to refuse up front with the model
# and count holding the row; instead it reported nothing, rendered NetBox's
# wall of several hundred dependent identity rows, and then failed with
# ProtectedError on confirm.
#
# The cause was not the protection. It was that the protection was invisible:
# `_meta.related_objects` omits relations declared `related_name="+"`, which is
# how every ownership FK in ForwardIngestionProvenanceMixin is declared. The
# database refused the delete while the checks that exist to predict that
# refusal could not see the relation at all.
#
# These tests pin the relation discovery itself, not one caller, because the
# same blind spot sat in two functions: the delete-refusal message and the
# merge's blocked-delete prediction.

from django.db import models
from django.test import TestCase

from forward_netbox.models import ForwardIngestion
from forward_netbox.utilities.bulk_merge import protecting_relations


class ProtectingRelationsSeesHiddenRelationsTest(TestCase):
    OWNERSHIP_MODELS = {
        "forward_netbox.ForwardDeviceIdentity",
        "forward_netbox.ForwardDeviceTagClaim",
        "forward_netbox.ForwardVirtualParentClaim",
        "forward_netbox.ForwardOwnershipReconciliation",
    }

    def test_hidden_protect_relations_are_discovered(self):
        # Each of these declares related_name="+", so none of them appear in
        # _meta.related_objects. All of them still refuse the delete in the
        # database, which is the only thing that actually matters.
        found = {
            relation.related_model._meta.label
            for relation in protecting_relations(ForwardIngestion)
        }
        self.assertTrue(
            self.OWNERSHIP_MODELS.issubset(found),
            f"hidden PROTECT relations missing from discovery: "
            f"{sorted(self.OWNERSHIP_MODELS - found)}",
        )

    def test_the_visible_baseline_protection_is_still_discovered(self):
        # Widening discovery must not lose the one relation that was already
        # being found; the baseline refusal is the expected, benign case.
        found = {
            relation.related_model._meta.label
            for relation in protecting_relations(ForwardIngestion)
        }
        self.assertIn("forward_netbox.ForwardContributorBaseline", found)

    def test_cascading_relations_are_not_reported_as_protecting(self):
        # Over-reporting would refuse deletes that the database would allow.
        for relation in protecting_relations(ForwardIngestion):
            with self.subTest(model=relation.related_model._meta.label):
                self.assertIn(
                    relation.field.remote_field.on_delete,
                    (models.PROTECT, models.RESTRICT),
                )

    def test_related_objects_alone_would_have_missed_them(self):
        # Pins the reason this helper exists. If a future Django release starts
        # including hidden relations in related_objects, this test fails and the
        # helper can be reconsidered rather than silently kept forever.
        visible = {
            relation.related_model._meta.label
            for relation in ForwardIngestion._meta.related_objects
        }
        self.assertFalse(
            self.OWNERSHIP_MODELS & visible,
            "related_objects now exposes hidden relations; revisit protecting_relations",
        )
