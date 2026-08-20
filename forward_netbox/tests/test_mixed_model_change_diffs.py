"""A change batch that carries two models must not corrupt either one's diffs.

The ipaddress apply emits the IPs it wrote and the Devices whose primary-ip
claims those IPs released, in one ``updated`` list. ``_sync_branch_change_diffs``
used to type the WHOLE batch from ``object_changes[0]`` and match existing
ChangeDiffs by bare ``object_id`` - so the Device changes were filed against
IPAddress-typed diffs whenever the pks collided, and new diffs were created
whose ``object_type`` said IPAddress while their ``original`` held a serialized
Device.

A later shard updating an IPAddress with the colliding pk found that corrupted
diff, and Branching's ``_update_conflicts`` - which iterates ``original``'s
keys and indexes ``modified`` directly - raised ``KeyError`` on the first
field one model has and the other does not. It cost a deployment three syncs of
about half an hour each, deterministically: the same data produced the same pk
collision in the same shard every run. Reproduced both ways by these tests on
the unfixed code: ``KeyError('vrf')`` when the IP's own diff came first, and a
second same-identity diff carrying Device content when the mixed batch came
first - whose Device-only key is not sync-contract vocabulary, which is why the
release that made contract keys nameable still reported nothing.

Device pk 1 and IPAddress pk 1 coexist in any NetBox; per-table sequences make
the collision ordinary, not exotic.
"""

from dcim.models import Device
from dcim.models import DeviceRole
from dcim.models import DeviceType
from dcim.models import Manufacturer
from dcim.models import Site
from django.contrib.auth import get_user_model
from django.test import TransactionTestCase
from ipam.models import IPAddress
from netbox.context import current_request
from netbox_branching.contextvars import active_branch
from netbox_branching.models import Branch
from netbox_branching.models import ChangeDiff

from forward_netbox.utilities.apply_engine_bulk import emit_branch_object_changes
from forward_netbox.utilities.branching import build_branch_request


class MixedModelChangeDiffTest(TransactionTestCase):

    def setUp(self):
        self.user = get_user_model().objects.create_user(username="diff-test")
        site = Site.objects.create(name="Diff Site", slug="diff-site")
        manufacturer = Manufacturer.objects.create(name="Diff Mfr", slug="diff-mfr")
        device_type = DeviceType.objects.create(
            manufacturer=manufacturer, model="Diff DT", slug="diff-dt"
        )
        role = DeviceRole.objects.create(name="Diff Role", slug="diff-role")
        self.device = Device.objects.create(
            name="diff-device",
            site=site,
            device_type=device_type,
            role=role,
            status="active",
        )
        # Force the pk collision the customer's data produced by chance: the
        # IP's pk must equal the Device's pk. Per-table sequences make this
        # ordinary in real deployments; here it is made explicit.
        self.ip = IPAddress(address="192.0.2.10/24", status="active")
        self.ip.pk = self.device.pk
        self.ip.save(force_insert=True)

        self.branch = Branch(name="mixed-diff-branch", owner=None)
        self.branch.save(provision=False)
        self.branch.provision(user=self.user)

    def tearDown(self):
        try:
            self.branch.deprovision()
        except Exception:
            pass

    def _emit(self, updated):
        token_branch = active_branch.set(self.branch)
        token_request = current_request.set(build_branch_request(self.user))
        try:
            emitted = emit_branch_object_changes((), updated)
        finally:
            current_request.reset(token_request)
            active_branch.set(None)
            active_branch.set(token_branch) if False else None
        self.assertTrue(emitted, "emit must have run for the test to mean anything")

    def test_a_mixed_batch_does_not_cross_models(self):
        # Shard 1: the IP is updated on its own, creating a legitimate
        # IPAddress diff for the colliding pk.
        self.ip.snapshot()
        self.ip.status = "reserved"
        self._emit([self.ip])

        # Shard 1 also released the device's primary-ip claim: the mixed batch,
        # exactly as the ipaddress write block emits it.
        self.ip.snapshot()
        self.ip.status = "dhcp"
        self.device.snapshot()
        self.device.comments = "primary ip released"
        self._emit([self.ip, self.device])

        diffs = {
            (diff.object_type.model, diff.object_id): diff
            for diff in ChangeDiff.objects.using(self.branch.connection_name).filter(
                branch=self.branch
            )
        }
        ip_diff = diffs.get(("ipaddress", self.ip.pk))
        device_diff = diffs.get(("device", self.device.pk))

        self.assertIsNotNone(ip_diff, "the IP kept its own diff")
        self.assertIsNotNone(
            device_diff,
            "the device gained a DEVICE-typed diff; before the fix its change "
            "was filed against the IPAddress diff that shares its pk",
        )
        # Each diff's content matches its own model. 'address' is an
        # IPAddress-only field; 'device_type' is a Device-only field.
        self.assertIn("address", ip_diff.modified)
        self.assertNotIn("device_type", ip_diff.modified)
        self.assertIn("device_type", device_diff.modified)
        self.assertNotIn("address", device_diff.modified)

    def test_the_customer_sequence_does_not_raise(self):
        """The exact three-step sequence that failed three syncs.

        Mixed emit first (creating whatever diffs it creates), then a later
        shard updates the colliding IP again. On the unfixed code this
        sequence produced TWO IPAddress-typed diffs for one object_id - one
        carrying Device content - and the deployment's later update matched
        the Device-content one, raising KeyError on a Device-only field.
        """
        self.ip.snapshot()
        self.ip.status = "reserved"
        self.device.snapshot()
        self.device.comments = "released"
        self._emit([self.ip, self.device])

        self.ip.snapshot()
        self.ip.status = "deprecated"
        self._emit([self.ip])  # raised KeyError before the fix

        ip_diff = (
            ChangeDiff.objects.using(self.branch.connection_name)
            .filter(branch=self.branch, object_id=self.ip.pk)
            .get(object_type__model="ipaddress")
        )
        self.assertIn("address", ip_diff.original)
        self.assertNotIn(
            "local_context_data",
            ip_diff.original,
            "a Device serialization reached an IPAddress diff",
        )
