# Almost every dependency skip is waiting on something ABSENT. The delete path
# is the inverse: the row is still REFERENCED and the database refuses to prune
# it. Both used to persist the same shape, so the two opposite conditions read
# identically.
#
# A customer's ingestion showed `netbox_dlm.softwareversion row processing
# skipped (ForwardDependencySkipError; netbox_dlm.inventoryitemsoftware)`. Read
# as a missing parent that is backwards - `inventoryitemsoftware` depends on
# `softwareversion`, not the reverse. It was a surviving child refusing the
# prune, which is the opposite situation and needs the opposite response.
from django.test import TestCase

from forward_netbox.exceptions import ForwardDependencySkipError
from forward_netbox.utilities.sync_reporting import dependency_phrase


class SkipDependencyDirectionTest(TestCase):
    def test_a_missing_parent_reads_as_waiting(self):
        detail = dependency_phrase(
            ForwardDependencySkipError(
                "Skipping DLM inventory item software.",
                model_string="netbox_dlm.inventoryitemsoftware",
                dependency="dcim.inventoryitem",
            )
        )
        self.assertIn("waiting on dcim.inventoryitem", detail)
        self.assertNotIn("still referenced by", detail)

    def test_a_surviving_child_reads_as_still_referenced(self):
        # The customer's exact shape.
        detail = dependency_phrase(
            ForwardDependencySkipError(
                "Skipping delete for `netbox_dlm.softwareversion`.",
                model_string="netbox_dlm.softwareversion",
                dependency="netbox_dlm.inventoryitemsoftware",
                dependency_is_protecting=True,
            )
        )
        self.assertIn(
            "still referenced by netbox_dlm.inventoryitemsoftware", detail
        )
        self.assertNotIn("waiting on", detail)

    def test_the_two_directions_never_produce_the_same_text(self):
        # The whole defect was that they did.
        shared = {
            "model_string": "netbox_dlm.softwareversion",
            "dependency": "netbox_dlm.devicesoftware",
        }
        waiting = dependency_phrase(
            ForwardDependencySkipError("m", **shared)
        )
        referenced = dependency_phrase(
            ForwardDependencySkipError("m", **shared, dependency_is_protecting=True)
        )
        self.assertNotEqual(waiting, referenced)

    def test_a_raiser_that_names_no_dependency_is_unchanged(self):
        # Most raisers have not been taught to name one; they must not regress.
        detail = dependency_phrase(
            ForwardDependencySkipError("m", model_string="dcim.device")
        )
        self.assertNotIn("waiting on", detail)
        self.assertNotIn("still referenced by", detail)
