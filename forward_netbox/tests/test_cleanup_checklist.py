# The Scope Reconciliation page shows a dozen counts and nothing says what to do
# about each. The checklist turns the stored report into ordered instructions and
# says, per item, whether anything is deleted. Synthetic data.
from django.test import SimpleTestCase

from forward_netbox.utilities.cleanup_checklist import cleanup_items
from forward_netbox.utilities.cleanup_checklist import DELETES_AFTER_REVIEW
from forward_netbox.utilities.cleanup_checklist import NO_DELETE_FIX_IN_FORWARD

URLS = {
    key: f"/{key}/"
    for key in (
        "same_name_more_than_once",
        "unmanaged_not_in_forward",
        "created_by_sync_not_in_forward",
        "out_of_scope_orphans",
        "in_forward_not_in_scope",
        "forward_could_not_collect",
    )
}

REPORT = {
    "forward_tagged_backfilled": 178,
    "backfilled_reason_breakdown": {
        "AUTHENTICATION_FAILED": 47,
        "PARSER_EXCEPTION)": 59,
        "INCOMPLETE_SETUP": 34,
        "UNKNOWN": 1,
    },
    "out_of_scope_quarantine": {"prune_eligible": 39},
    "unmanaged": {
        "unclaimed_absence": {"absent_from_snapshot": 35},
        "unclaimed_absent_device_ids": list(range(1, 36)),
        "owned_prune_candidates": 71,
        "owned_quarantine": {"prune_eligible": 39},
        "owned_absence": {"present_untagged": 24},
    },
}


def _by_key(items):
    return {item["key"]: item for item in items}


class CleanupChecklistTest(SimpleTestCase):
    def test_every_cleanup_count_gets_one_item_with_steps_and_a_list(self):
        items = _by_key(
            cleanup_items(
                REPORT,
                twin_count=2,
                relabel={"pair_count": 0, "held_count": 1},
                urls=URLS,
            )
        )

        self.assertEqual(set(items), set(URLS) | {"site_relabel_duplicates"})
        for item in items.values():
            self.assertTrue(item["steps"], item["key"])
            self.assertGreater(item["count"], 0, item["key"])
            if item["key"] != "site_relabel_duplicates":
                self.assertTrue(item["url"], item["key"])

    def test_nothing_to_do_gives_no_items(self):
        self.assertEqual(cleanup_items({}, urls=URLS), [])
        self.assertEqual(cleanup_items(None, urls=URLS), [])

    def test_items_with_a_zero_count_are_left_out(self):
        report = {"unmanaged": {"unclaimed_absence": {"absent_from_snapshot": 0}}}

        self.assertEqual(cleanup_items(report, urls=URLS), [])

    def test_only_reviewed_deletes_are_marked_as_deleting(self):
        items = _by_key(cleanup_items(REPORT, twin_count=2, relabel={}, urls=URLS))

        self.assertEqual(
            items["unmanaged_not_in_forward"]["kind"], DELETES_AFTER_REVIEW
        )
        self.assertEqual(
            items["forward_could_not_collect"]["kind"], NO_DELETE_FIX_IN_FORWARD
        )
        self.assertEqual(
            items["in_forward_not_in_scope"]["kind"], NO_DELETE_FIX_IN_FORWARD
        )

    def test_a_collection_problem_says_why_and_never_deletes(self):
        item = _by_key(cleanup_items(REPORT, urls=URLS))["forward_could_not_collect"]

        self.assertIn("59 PARSER_EXCEPTION", item["what"])
        self.assertNotIn(")", item["what"].split("(")[1].split(")")[0])
        self.assertIn("Nothing is deleted", item["what"])

    def test_the_unmanaged_item_warns_that_absence_is_normal_for_passive_kit(self):
        item = _by_key(cleanup_items(REPORT, urls=URLS))["unmanaged_not_in_forward"]

        self.assertIn("patch panels", item["what"])
        self.assertIn("confirmed are decommissioned", " ".join(item["steps"]))

    def test_a_duplicate_name_item_says_which_copy_to_keep(self):
        item = _by_key(cleanup_items({}, twin_count=3, urls=URLS))[
            "same_name_more_than_once"
        ]

        self.assertIn("not the one the sync", " ".join(item["steps"]))
        self.assertIn("Check the managed copy", " ".join(item["steps"]))

    def test_a_pending_site_relabel_pair_points_at_the_merge_button(self):
        item = _by_key(
            cleanup_items({}, relabel={"pair_count": 2, "held_count": 0}, urls=URLS)
        )["site_relabel_duplicates"]

        self.assertEqual(item["count"], 2)
        self.assertIn("Merge site-relabel duplicates", " ".join(item["steps"]))

    def test_held_groups_get_an_instruction_and_are_not_counted_as_twins(self):
        items = _by_key(
            cleanup_items({}, relabel={"pair_count": 0, "held_count": 1}, urls=URLS)
        )

        self.assertNotIn("same_name_more_than_once", items)
        self.assertIn("held group", " ".join(items["site_relabel_duplicates"]["steps"]))

    def test_the_delete_eligible_step_only_appears_when_something_is_eligible(self):
        none = {
            "unmanaged": {
                "owned_prune_candidates": 5,
                "owned_quarantine": {"prune_eligible": 0},
            }
        }
        some = {
            "unmanaged": {
                "owned_prune_candidates": 5,
                "owned_quarantine": {"prune_eligible": 3},
            }
        }

        self.assertNotIn(
            "Delete eligible",
            " ".join(
                _by_key(cleanup_items(none, urls=URLS))[
                    "created_by_sync_not_in_forward"
                ]["steps"]
            ),
        )
        self.assertIn(
            "3 have waited",
            " ".join(
                _by_key(cleanup_items(some, urls=URLS))[
                    "created_by_sync_not_in_forward"
                ]["steps"]
            ),
        )

    def test_prune_orphans_says_it_also_deletes_empty_sites(self):
        item = _by_key(cleanup_items(REPORT, urls=URLS))["out_of_scope_orphans"]

        self.assertIn("empty sites", " ".join(item["steps"]))

    def test_no_item_names_a_device(self):
        rendered = repr(
            cleanup_items(
                {
                    **REPORT,
                    "unmanaged": {
                        **REPORT["unmanaged"],
                        "unclaimed_sample": ["secret-host"],
                    },
                },
                twin_count=1,
                urls=URLS,
            )
        )

        self.assertNotIn("secret-host", rendered)


class UnmanagedCountMatchesItsListTest(SimpleTestCase):
    def test_the_count_is_the_list_not_the_census(self):
        report = {
            "unmanaged": {
                "unclaimed_absence": {"absent_from_snapshot": 35},
                "unclaimed_absent_device_ids": [1, 2, 3],
                "unclaimed_configuration_checked": True,
            }
        }

        item = _by_key(cleanup_items(report, urls=URLS))["unmanaged_not_in_forward"]

        self.assertEqual(item["count"], 3)

    def test_an_unreadable_configuration_says_so_and_offers_no_delete(self):
        report = {
            "unmanaged": {
                "unclaimed_absence": {"absent_from_snapshot": 35},
                "unclaimed_absent_device_ids": [],
                "unclaimed_configuration_checked": False,
            }
        }

        items = _by_key(cleanup_items(report, urls=URLS))

        self.assertNotIn("unmanaged_not_in_forward", items)
        self.assertEqual(
            items["unmanaged_not_in_forward_unverified"]["kind"],
            NO_DELETE_FIX_IN_FORWARD,
        )
        self.assertEqual(items["unmanaged_not_in_forward_unverified"]["count"], 35)


class HeldGroupAdviceTest(SimpleTestCase):
    def test_a_held_group_carries_its_own_remedy(self):
        item = _by_key(
            cleanup_items(
                {},
                relabel={
                    "pair_count": 0,
                    "held_count": 1,
                    "held_by_reason": [
                        {
                            "reason": "forward_site_matches_neither_device",
                            "count": 1,
                            "remedy": "Forward places this name at a third site.",
                        }
                    ],
                },
                urls=URLS,
            )
        )["site_relabel_duplicates"]

        steps = " ".join(item["steps"])
        self.assertIn("forward site matches neither device", steps)
        self.assertIn("third site", steps)
        self.assertNotIn("A held group names why", steps)
