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
    # `ForwardOwnershipReconciliation` was the fourth entry here. It now
    # cascades: it is a child record of the ingestion rather than evidence held
    # against it, and holding it as PROTECT made ingestions undeletable while
    # naming rows no supported action could remove. It is still hidden, so it
    # still belongs in HIDDEN_MODELS below — discovery must keep seeing it, it
    # just must not be reported as protecting.
    #
    # `ForwardContributorBaseline` went the same way for the same reason: every
    # ingestion that ever promoted left a superseded husk behind that protected
    # it forever. The LIVE generation is still kept, by a `pre_delete` receiver
    # rather than by the constraint, because PROTECT cannot tell the two apart.
    OWNERSHIP_MODELS = {
        "forward_netbox.ForwardDeviceIdentity",
        "forward_netbox.ForwardDeviceTagClaim",
        "forward_netbox.ForwardVirtualParentClaim",
    }
    HIDDEN_MODELS = OWNERSHIP_MODELS | {
        "forward_netbox.ForwardOwnershipReconciliation",
    }

    def _hidden_relations(self):
        return {
            relation.related_model._meta.label: relation
            for relation in ForwardIngestion._meta._get_fields(
                forward=False, reverse=True, include_hidden=True
            )
            if getattr(relation, "related_model", None) is not None
        }

    def test_the_hidden_traversal_still_reaches_the_ownership_models(self):
        # The three ownership models are SET_NULL now (migration 0051), so they
        # are correctly absent from `protecting_relations`. That makes this the
        # test standing between the hidden-relation traversal and silently
        # returning nothing: if `include_hidden` is ever dropped, protection on
        # a `related_name="+"` FK becomes invisible again, which is the failure
        # that made every ingestion undeletable in 2.7.0.
        reachable = self._hidden_relations()
        missing = self.HIDDEN_MODELS - set(reachable)
        self.assertFalse(
            missing,
            f"hidden relations no longer reachable by traversal: {sorted(missing)}",
        )

    def test_a_provenance_stamp_does_not_protect_its_ingestion(self):
        # The stamp records which run last asserted the evidence. It was
        # PROTECT, so a device that left Forward's scope froze its evidence on
        # the last ingestion that saw it and pinned that ingestion forever - one
        # undeletable ingestion per scope change, for a customer.
        #
        # The rows are not stale: the devices still exist and are still owned.
        # Only the stamp is old, which is why pruning the evidence was never the
        # right fix.
        reachable = self._hidden_relations()
        for label in self.OWNERSHIP_MODELS:
            with self.subTest(model=label):
                self.assertIs(
                    reachable[label].field.remote_field.on_delete,
                    models.SET_NULL,
                )
        found = {
            relation.related_model._meta.label
            for relation in protecting_relations(ForwardIngestion)
        }
        self.assertFalse(
            self.OWNERSHIP_MODELS & found,
            "ownership evidence is reported as protecting its ingestion again",
        )

    def test_the_baseline_is_not_reported_as_protecting(self):
        # The baseline was the last VISIBLE protecting relation, and it now
        # cascades so a superseded generation can be collected with its
        # ingestion. Reporting it would refuse a delete the database allows.
        #
        # With the ownership stamps now SET_NULL, an ingestion may legitimately
        # have no protecting relation at all - what refuses a delete is the
        # `pre_delete` receiver guarding the live baseline and a running job.
        # `test_the_hidden_traversal_still_reaches_the_ownership_models` is what
        # now stands between discovery and silently seeing nothing.
        found = {
            relation.related_model._meta.label
            for relation in protecting_relations(ForwardIngestion)
        }
        self.assertNotIn("forward_netbox.ForwardContributorBaseline", found)

    def test_cascading_relations_are_not_reported_as_protecting(self):
        # Over-reporting would refuse deletes that the database would allow.
        for relation in protecting_relations(ForwardIngestion):
            with self.subTest(model=relation.related_model._meta.label):
                self.assertIn(
                    relation.field.remote_field.on_delete,
                    (models.PROTECT, models.RESTRICT),
                )

    def test_reconciliation_is_not_reported_as_protecting(self):
        # A reconciliation row records only that a sync finished a domain at an
        # ingestion, so it means nothing once that ingestion is gone and it
        # cascades. Reporting it would refuse a delete the database allows —
        # and name rows the operator has no supported way to remove, which is
        # exactly the dead end this replaced.
        found = {
            relation.related_model._meta.label
            for relation in protecting_relations(ForwardIngestion)
        }
        self.assertNotIn("forward_netbox.ForwardOwnershipReconciliation", found)

    def test_related_objects_alone_would_have_missed_them(self):
        # Pins the reason this helper exists. If a future Django release starts
        # including hidden relations in related_objects, this test fails and the
        # helper can be reconsidered rather than silently kept forever.
        visible = {
            relation.related_model._meta.label
            for relation in ForwardIngestion._meta.related_objects
        }
        self.assertFalse(
            self.HIDDEN_MODELS & visible,
            "related_objects now exposes hidden relations; revisit protecting_relations",
        )
