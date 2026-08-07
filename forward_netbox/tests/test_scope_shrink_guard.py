# `out_of_scope` is decided purely by absence from the current Forward scope
# result, so a query that under-returns manufactures orphans out of valid
# devices - and the prune deletes them. The only guard was all-or-nothing: it
# refused at zero devices returned, so a result holding most of the fleet
# passed cleanly.
from unittest.mock import patch

from django.test import TestCase

from forward_netbox.models import ForwardSource
from forward_netbox.models import ForwardSync
from forward_netbox.utilities.scope_reconciliation import EmptyForwardScopeError
from forward_netbox.utilities.scope_reconciliation import prune_orphan_devices
from forward_netbox.utilities.scope_reconciliation import (
    SCOPE_SHRINK_REFUSAL_FLOOR,
)
from forward_netbox.utilities.scope_reconciliation import (
    SCOPE_SHRINK_REFUSAL_RATIO,
)
from forward_netbox.utilities.scope_reconciliation import ScopeShrinkGuardError


def _report(*, orphans, previously_managed, tagged=("kept-1",)):
    names = {f"orphan-{index}" for index in range(orphans)}
    return {
        "_out_of_scope": names,
        "_out_of_scope_pks": list(range(1, orphans + 1)),
        "_tagged_names": set(tagged),
        "_device_tagged_names": set(tagged),
        "netbox_out_of_scope": orphans,
        "forward_previously_managed": previously_managed,
    }


class ScopeShrinkGuardTest(TestCase):
    def setUp(self):
        source = ForwardSource.objects.create(
            name="shrink-src",
            type="saas",
            url="https://fwd.app",
            status="ready",
            parameters={
                "username": "u@example.com",
                "password": "p",
                "verify": True,
                "network_id": "net-1",
            },
        )
        self.sync = ForwardSync.objects.create(name="shrink-sync", source=source)

    def test_a_collapsed_scope_is_refused_before_anything_is_deleted(self):
        # Half the previously claimed fleet absent from one result is a query
        # fault, not a decommissioning.
        report = _report(orphans=500, previously_managed=1000)
        with self.assertRaises(ScopeShrinkGuardError) as caught:
            prune_orphan_devices(self.sync, report=report)
        message = str(caught.exception)
        self.assertIn("500", message)
        self.assertIn("1000", message)

    def test_ordinary_attrition_still_prunes(self):
        # The customer's live shape: 38 of ~3400. A guard that blocked this
        # would just be turned off.
        report = _report(orphans=38, previously_managed=3425)
        with patch(
            "forward_netbox.utilities.scope_reconciliation._prunable_device_order",
            return_value=([], set()),
        ):
            result = prune_orphan_devices(self.sync, report=report)
        self.assertEqual(result["pruned_device_count"], 0)

    def test_the_override_lets_a_genuine_removal_through(self):
        # An override that cannot fire is worse than no override.
        report = _report(orphans=500, previously_managed=1000)
        with patch(
            "forward_netbox.utilities.scope_reconciliation._prunable_device_order",
            return_value=([], set()),
        ):
            result = prune_orphan_devices(
                self.sync, report=report, allow_scope_shrink=True
            )
        self.assertEqual(result["pruned_device_count"], 0)

    def test_the_ratio_boundary_is_inclusive(self):
        at_limit = int(1000 * SCOPE_SHRINK_REFUSAL_RATIO)
        report = _report(orphans=at_limit, previously_managed=1000)
        with patch(
            "forward_netbox.utilities.scope_reconciliation._prunable_device_order",
            return_value=([], set()),
        ):
            prune_orphan_devices(self.sync, report=report)

    def test_a_small_deployment_is_not_blocked_by_a_meaningless_ratio(self):
        # 3 of 8 is 38%, and means nothing. A guard that fires on a lab or a
        # small sync is a guard that gets switched off - and it broke a real
        # fixture before this floor existed.
        report = _report(orphans=3, previously_managed=8)
        with patch(
            "forward_netbox.utilities.scope_reconciliation._prunable_device_order",
            return_value=([], set()),
        ):
            prune_orphan_devices(self.sync, report=report)

    def test_the_floor_is_inclusive(self):
        report = _report(
            orphans=SCOPE_SHRINK_REFUSAL_FLOOR,
            previously_managed=SCOPE_SHRINK_REFUSAL_FLOOR,
        )
        with patch(
            "forward_netbox.utilities.scope_reconciliation._prunable_device_order",
            return_value=([], set()),
        ):
            prune_orphan_devices(self.sync, report=report)

    def test_one_past_the_floor_at_full_ratio_refuses(self):
        count = SCOPE_SHRINK_REFUSAL_FLOOR + 1
        report = _report(orphans=count, previously_managed=count)
        with self.assertRaises(ScopeShrinkGuardError):
            prune_orphan_devices(self.sync, report=report)

    def test_no_prior_claims_does_not_divide_by_zero(self):
        report = _report(orphans=3, previously_managed=0)
        with patch(
            "forward_netbox.utilities.scope_reconciliation._prunable_device_order",
            return_value=([], set()),
        ):
            prune_orphan_devices(self.sync, report=report)

    def test_the_empty_scope_guard_still_fires_first(self):
        report = _report(orphans=5, previously_managed=10, tagged=())
        with self.assertRaises(EmptyForwardScopeError):
            prune_orphan_devices(self.sync, report=report)
