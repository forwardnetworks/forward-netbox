# Tasks #45 and #46: "every ingestion that ever promoted a baseline is
# permanently undeletable, and the backlog grows one row per successful sync",
# and "ForwardDeviceIdentity rows for departed source keys are the other cause".
#
# Both were fixed at the model - the contributor baseline CASCADEs with its
# ingestion, and the provenance stamp is SET_NULL - but the plans that recorded
# them stayed open, so the closure was invisible and one PROTECT added later
# would silently reopen it. This pins the property instead of the history:
# NOTHING may hold an ingestion by PROTECT.
#
# The two refusals an operator can still meet are deliberate and temporary -
# the live contributor baseline and the current ownership evidence - and both
# clear on the next sync. They are asserted here as the ONLY two.
from django.test import TestCase

from forward_netbox.models import ForwardContributorBaseline
from forward_netbox.models import ForwardDeviceIdentity
from forward_netbox.models import ForwardIngestion
from forward_netbox.models import ForwardIngestionIssue
from forward_netbox.models import ForwardSource
from forward_netbox.models import ForwardSync
from forward_netbox.models import ForwardWorkloadState
from forward_netbox.utilities.bulk_merge import protecting_relations
from forward_netbox.views import _ingestion_delete_refusal_detail


class NothingHoldsAnIngestionByProtectTest(TestCase):
    def test_no_protect_relation_targets_an_ingestion(self):
        held_by = [
            (relation.related_model._meta.label_lower, relation.field.name)
            for relation in protecting_relations(ForwardIngestion)
        ]
        self.assertEqual(
            held_by,
            [],
            "a PROTECT relation to ForwardIngestion makes every ingestion that "
            "reaches it permanently undeletable - tasks #45 and #46. Use "
            "CASCADE for bookkeeping about a run, or SET_NULL for a stamp.",
        )

    def test_the_bookkeeping_models_cascade(self):
        for model, field_name in (
            (ForwardIngestionIssue, "ingestion"),
            (ForwardWorkloadState, "ingestion"),
            (ForwardContributorBaseline, "ingestion"),
        ):
            field = model._meta.get_field(field_name)
            self.assertEqual(
                field.remote_field.on_delete.__name__,
                "CASCADE",
                f"{model._meta.label_lower}.{field_name}",
            )

    def test_the_identity_stamp_is_set_null_not_protect(self):
        # #46: a device that left Forward's scope froze its identity on the
        # last ingestion that saw it and pinned that ingestion forever.
        field = ForwardDeviceIdentity._meta.get_field("ingestion")
        self.assertEqual(field.remote_field.on_delete.__name__, "SET_NULL")
        self.assertTrue(field.null)


class OnlyTheTwoTemporaryRefusalsRemainTest(TestCase):
    def setUp(self):
        source = ForwardSource.objects.create(
            name="deletable-src", type="saas", url="https://fwd.app", status="ready"
        )
        self.sync = ForwardSync.objects.create(name="deletable-sync", source=source)

    def _ingestion(self, snapshot_id="snap-1", **kwargs):
        return ForwardIngestion.objects.create(
            sync=self.sync, snapshot_id=snapshot_id, **kwargs
        )

    def test_an_ordinary_ingestion_deletes(self):
        ingestion = self._ingestion()
        ForwardIngestionIssue.objects.create(
            ingestion=ingestion, phase="sync", model="dcim.device", message="x"
        )
        self.assertEqual(_ingestion_delete_refusal_detail(ingestion), ("", False))

        ingestion.delete()

        self.assertFalse(ForwardIngestion.objects.filter(pk=ingestion.pk).exists())
        self.assertFalse(ForwardIngestionIssue.objects.exists())

    def test_an_ingestion_stamped_on_a_device_identity_still_deletes(self):
        # The #46 shape: the identity outlives the run that recorded it.
        from dcim.models import Device
        from dcim.models import DeviceRole
        from dcim.models import DeviceType
        from dcim.models import Manufacturer
        from dcim.models import Site

        ingestion = self._ingestion()
        mfr = Manufacturer.objects.create(name="Del Mfr", slug="del-mfr")
        device = Device.objects.create(
            name="del-dev",
            site=Site.objects.create(name="Del Site", slug="del-site"),
            device_type=DeviceType.objects.create(
                manufacturer=mfr, model="Del DT", slug="del-dt"
            ),
            role=DeviceRole.objects.create(name="Del Role", slug="del-role"),
        )
        identity = ForwardDeviceIdentity.objects.create(
            sync=self.sync,
            ingestion=ingestion,
            source_device_key="del-dev",
            device=device,
        )

        ingestion.delete()

        identity.refresh_from_db()
        self.assertIsNone(identity.ingestion_id)
        self.assertEqual(identity.device_id, device.pk)

    def test_a_live_baseline_refusal_is_expected_and_says_how_it_clears(self):
        ingestion = self._ingestion()
        ForwardContributorBaseline.objects.create(
            sync=self.sync, ingestion=ingestion, is_current=True
        )
        refusal, expected = _ingestion_delete_refusal_detail(ingestion)
        self.assertTrue(expected)
        self.assertIn("Run the sync again", refusal)
