"""A Forward diff may not delete what baseline reconciliation refuses.

The two delete producers were guarded asymmetrically for two releases.
`compute_full_removals` enforces `BASELINE_REMOVAL_MODELS`, which refuses
`dcim.site`, `dcim.devicetype`, `ipam.vrf` and `dcim.device` by name with the
reasons written out. `_split_diff_rows` refused nothing: every `DELETED` row
became a delete for any model, and the only gate downstream is the ACI
suppression check.

A deployment on 2.8.1 reached `delete()` on an `ipam.vrf`, a `dcim.devicetype`
and three `dcim.site` rows in one sync. Nothing was lost only because PROTECT
refused them - a database constraint caught what a gate should have. A site
with no devices, or a device type with no devices, has nothing holding it.

These tests pin the NEGATIVE space. Asserting only that the permitted models
are still deletable is what let "every model" ship twice: a test suite that
never says "and this one must NOT be" cannot notice a model quietly becoming
deletable.
"""

from types import SimpleNamespace

from django.test import SimpleTestCase

from forward_netbox.utilities.branch_budget import DELETE_DEPENDENCY_MODEL_ORDER
from forward_netbox.utilities.full_removal_reconciliation import (
    BASELINE_REMOVAL_MODELS,
)
from forward_netbox.utilities.full_removal_reconciliation import DIFF_REMOVAL_MODELS
from forward_netbox.utilities.full_removal_reconciliation import (
    DIFF_REMOVAL_REFUSED_MODELS,
)
from forward_netbox.utilities.full_removal_reconciliation import diff_removals_allowed
from forward_netbox.utilities.full_removal_reconciliation import PRUNE_REMOVAL_MODELS
from forward_netbox.utilities.full_removal_reconciliation import prune_removals_allowed
from forward_netbox.utilities.sync import ForwardSyncRunner


def _models_with_a_delete_handler():
    """Every model the sync can actually delete.

    `delete_model_rows` finds handlers by naming convention, so this is the
    real attack surface rather than a list someone remembered to update.
    """
    return {
        model_string
        for model_string in DELETE_DEPENDENCY_MODEL_ORDER
        if hasattr(ForwardSyncRunner, f"_delete_{model_string.replace('.', '_')}")
    }


class DiffRemovalPolicyPartitionTest(SimpleTestCase):
    def test_every_deletable_model_is_classified(self):
        # The point of the whole exercise: a model added later cannot land in
        # neither set and silently inherit "deletable".
        deletable = _models_with_a_delete_handler()
        classified = DIFF_REMOVAL_MODELS | DIFF_REMOVAL_REFUSED_MODELS
        self.assertEqual(
            set(),
            deletable - classified,
            "a model has a delete handler but no removal policy",
        )
        self.assertEqual(
            set(),
            classified - deletable,
            "the removal policy names a model that cannot be deleted",
        )

    def test_the_two_sets_are_disjoint(self):
        self.assertEqual(set(), DIFF_REMOVAL_MODELS & DIFF_REMOVAL_REFUSED_MODELS)

    def test_the_diff_policy_is_at_least_as_strict_as_the_baseline_policy(self):
        # The asymmetry that caused this. Anything baseline reconciliation
        # refuses, a diff must refuse too - the diff signal is not stronger
        # evidence, it is the same absence reported by a different route.
        baseline_refused = _models_with_a_delete_handler() - BASELINE_REMOVAL_MODELS
        wrongly_permitted = (
            baseline_refused & DIFF_REMOVAL_MODELS
        ) - _DELIBERATELY_WIDER
        self.assertEqual(set(), wrongly_permitted)

    def test_everything_the_baseline_allows_the_diff_allows(self):
        self.assertTrue(BASELINE_REMOVAL_MODELS.issubset(DIFF_REMOVAL_MODELS))


# The diff list is wider than the baseline list on purpose: baseline
# reconciliation can only speak for models it has persisted rows for, while a
# diff speaks for whatever the query covers. Every entry is a row the plugin
# solely authors and no operator maintains by hand. ACI additionally keeps its
# own brake (`should_suppress_aci_deletes`).
_DELIBERATELY_WIDER = frozenset(
    {
        "dcim.virtualchassis",
        "extras.taggeditem",
        "netbox_peering_manager.peeringsession",
        "netbox_cisco_aci.acibridgedomain",
        "netbox_cisco_aci.acifabric",
        "netbox_cisco_aci.acifilter",
        "netbox_cisco_aci.acil3out",
        "netbox_cisco_aci.acinode",
        "netbox_cisco_aci.acipod",
        "netbox_cisco_aci.acitenant",
        "netbox_cisco_aci.acivrf",
    }
)


class RefusedModelsAreNamedTest(SimpleTestCase):
    """The customer's five models, one assertion each."""

    def test_a_site_is_never_auto_deleted_by_a_diff(self):
        self.assertFalse(diff_removals_allowed("dcim.site"))

    def test_a_device_is_never_auto_deleted_by_a_diff(self):
        # Operator-gated through Scope Reconciliation -> Prune orphans.
        self.assertFalse(diff_removals_allowed("dcim.device"))

    def test_shared_catalogues_are_never_auto_deleted_by_a_diff(self):
        for model_string in (
            "dcim.devicetype",
            "dcim.platform",
            "dcim.manufacturer",
            "dcim.devicerole",
            "netbox_dlm.softwareversion",
        ):
            with self.subTest(model_string=model_string):
                self.assertFalse(diff_removals_allowed(model_string))

    def test_global_ipam_is_never_auto_deleted_by_a_diff(self):
        for model_string in ("ipam.prefix", "ipam.vlan", "ipam.vrf"):
            with self.subTest(model_string=model_string):
                self.assertFalse(diff_removals_allowed(model_string))

    def test_device_derived_rows_are_still_deletable(self):
        # The fix must not stop the sync converging on the rows it owns.
        for model_string in (
            "dcim.interface",
            "dcim.macaddress",
            "dcim.cable",
            "ipam.ipaddress",
            "netbox_dlm.devicesoftware",
            "netbox_routing.bgppeer",
        ):
            with self.subTest(model_string=model_string):
                self.assertTrue(diff_removals_allowed(model_string))

    def test_an_unknown_model_fails_closed(self):
        self.assertFalse(diff_removals_allowed("dcim.somethingnew"))


class SplitDiffRowsAppliesThePolicyTest(SimpleTestCase):
    """The policy has to reach the rows, not just exist."""

    def _runner(self):
        runner = object.__new__(ForwardSyncRunner)
        runner._model_coalesce_fields = {
            "dcim.site": [("slug",), ("name",)],
            "ipam.vrf": [("name",)],
            "dcim.macaddress": [("mac_address",)],
        }
        runner.sync = None
        runner.warnings = []
        runner.logger = SimpleNamespace(
            log_warning=lambda message, obj=None: runner.warnings.append(message)
        )
        return runner

    def _deleted(self, before):
        return [{"type": "DELETED", "before": before, "after": None}]

    def test_a_deleted_site_row_is_held_back(self):
        runner = self._runner()

        upserts, deletes = runner._split_diff_rows(
            "dcim.site", self._deleted({"slug": "site-a", "name": "Site A"})
        )

        self.assertEqual([], upserts)
        self.assertEqual([], deletes)

    def test_a_deleted_vrf_row_is_held_back(self):
        runner = self._runner()

        _, deletes = runner._split_diff_rows(
            "ipam.vrf", self._deleted({"name": "vrf-a"})
        )

        self.assertEqual([], deletes)

    def test_holding_back_is_reported_and_names_the_model(self):
        # Silence would read as "there was nothing to remove". An operator
        # waiting for a stale row to disappear needs to know why it did not.
        runner = self._runner()

        runner._split_diff_rows(
            "dcim.site", self._deleted({"slug": "site-a", "name": "Site A"})
        )

        self.assertEqual(1, len(runner.warnings))
        message = runner.warnings[0]
        self.assertIn("Held back 1 Forward-diff delete(s) for dcim.site", message)
        self.assertIn("Prune orphans", message)

    def test_the_warning_carries_no_row_values(self):
        # The model is a schema identifier; the slug is customer data.
        runner = self._runner()

        runner._split_diff_rows(
            "dcim.site",
            self._deleted({"slug": "example-site-slug", "name": "Example Site"}),
        )

        self.assertNotIn("example-site-slug", runner.warnings[0])
        self.assertNotIn("Example Site", runner.warnings[0])

    def test_an_allowed_model_still_deletes_and_stays_quiet(self):
        runner = self._runner()

        _, deletes = runner._split_diff_rows(
            "dcim.macaddress", self._deleted({"mac_address": "00:00:00:00:00:01"})
        )

        self.assertEqual([{"mac_address": "00:00:00:00:00:01"}], deletes)
        self.assertEqual([], runner.warnings)

    def test_upserts_for_a_refused_model_are_untouched(self):
        # Only removal is refused. A site that Forward still reports must keep
        # syncing normally.
        runner = self._runner()

        upserts, deletes = runner._split_diff_rows(
            "dcim.site",
            [
                {
                    "type": "ADDED",
                    "before": None,
                    "after": {"slug": "site-b", "name": "Site B"},
                }
            ],
        )

        self.assertEqual([{"slug": "site-b", "name": "Site B"}], upserts)
        self.assertEqual([], deletes)
        self.assertEqual([], runner.warnings)

    def test_a_rename_still_deletes_the_superseded_row(self):
        # The line the policy must NOT cross. A MODIFIED row whose identity key
        # changed is Forward reporting the same object under a new identity,
        # and the after-side is written in this same batch. Refusing the delete
        # preserves nothing and strands a duplicate forever - an end-to-end
        # test that renames a site and asserts the old row is gone is what
        # caught the blanket version of this rule.
        runner = self._runner()

        upserts, deletes = runner._split_diff_rows(
            "dcim.site",
            [
                {
                    "type": "MODIFIED",
                    "before": {"slug": "site-before", "name": "Before"},
                    "after": {"slug": "site-after", "name": "After"},
                }
            ],
        )

        self.assertEqual([{"slug": "site-after", "name": "After"}], upserts)
        self.assertEqual([{"slug": "site-before", "name": "Before"}], deletes)
        self.assertEqual([], runner.warnings)

    def test_a_rename_and_an_absence_in_one_batch_are_split(self):
        # Both kinds of delete for a refused model at once: the rename survives,
        # the absence is held back, and the count in the message is the number
        # actually withheld rather than the total.
        runner = self._runner()

        _, deletes = runner._split_diff_rows(
            "dcim.site",
            [
                {
                    "type": "MODIFIED",
                    "before": {"slug": "site-before", "name": "Before"},
                    "after": {"slug": "site-after", "name": "After"},
                },
                {
                    "type": "DELETED",
                    "before": {"slug": "site-gone", "name": "Gone"},
                    "after": None,
                },
            ],
        )

        self.assertEqual([{"slug": "site-before", "name": "Before"}], deletes)
        self.assertIn("Held back 1 Forward-diff delete(s)", runner.warnings[0])

    def test_a_refused_model_with_no_deletes_says_nothing(self):
        runner = self._runner()

        runner._split_diff_rows(
            "dcim.site",
            [
                {
                    "type": "MODIFIED",
                    "before": {"slug": "site-a", "name": "Site A"},
                    "after": {"slug": "site-a", "name": "Site A renamed"},
                }
            ],
        )

        self.assertEqual([], runner.warnings)


class PruneRemovalPolicyTest(SimpleTestCase):
    """The third producer, found the same way as the second.

    A customer on 2.8.2 still had six `netbox_dlm.softwareversion`
    protected-delete skips after the diff and baseline paths were both gated.
    Rows dropped by device-tag scope become deletes whenever
    `device_tag_prune_out_of_scope` is on, and nothing consulted a model policy
    on the way.
    """

    def test_prune_may_remove_devices_and_sites(self):
        # This is what Prune orphans is FOR. Refusing it would break the
        # feature, not protect anything.
        self.assertTrue(prune_removals_allowed("dcim.device"))
        self.assertTrue(prune_removals_allowed("dcim.site"))

    def test_prune_may_not_remove_shared_catalogues(self):
        # "This device left tag scope" is not a statement about a catalogue.
        for model_string in (
            "netbox_dlm.softwareversion",
            "dcim.devicetype",
            "dcim.platform",
            "dcim.manufacturer",
            "dcim.devicerole",
        ):
            with self.subTest(model_string=model_string):
                self.assertFalse(prune_removals_allowed(model_string))

    def test_prune_may_not_remove_global_ipam(self):
        for model_string in ("ipam.prefix", "ipam.vlan", "ipam.vrf"):
            with self.subTest(model_string=model_string):
                self.assertFalse(prune_removals_allowed(model_string))

    def test_prune_still_removes_device_derived_rows(self):
        for model_string in ("dcim.interface", "ipam.ipaddress", "dcim.macaddress"):
            with self.subTest(model_string=model_string):
                self.assertTrue(prune_removals_allowed(model_string))

    def test_an_unknown_model_fails_closed(self):
        self.assertFalse(prune_removals_allowed("dcim.somethingnew"))

    def test_prune_is_the_diff_policy_plus_exactly_device_and_site(self):
        # The one respect in which the operator-gated path may be wider, stated
        # as an equality so a third model cannot be added quietly.
        self.assertEqual(
            {"dcim.device", "dcim.site"},
            PRUNE_REMOVAL_MODELS - DIFF_REMOVAL_MODELS,
        )

    def test_every_deletable_model_is_classified_for_prune_too(self):
        deletable = _models_with_a_delete_handler()
        self.assertEqual(
            set(),
            deletable - (PRUNE_REMOVAL_MODELS | DIFF_REMOVAL_REFUSED_MODELS),
        )
