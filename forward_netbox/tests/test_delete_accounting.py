# A customer's run said 869 objects deleted while the change log showed 504
# deletes, and nothing said why. The staged (logical) delete count and the deletes
# the merge wrote differ legitimately. The bundle reconciles them per model, names
# the causes, and never reads the difference as a loss. Synthetic data.
from types import SimpleNamespace

from django.test import SimpleTestCase

from forward_netbox.utilities.health_evidence import _delete_accounting
from forward_netbox.utilities.merge import _held_deletes_by_model


def _ingestion(staged, *, accounting=None):
    data = {"merge_accounting": accounting} if accounting is not None else {}
    return SimpleNamespace(
        deleted_change_count=staged, job=SimpleNamespace(data=data), merge_job=None
    )


ACCOUNTING = {
    "logical_action_by_model": {
        "ipam.prefix.delete": 504,
        "dcim.cable.delete": 350,
        "ipam.vrf.delete": 15,
        "dcim.interface.create": 9,
    },
    "held_deletes_by_model": {
        "ipam.vrf": {"deletes": 15, "referencing_rows": {"ipam.ipaddress": 10}}
    },
    "failed": 0,
    "skipped": 2,
}


class HeldDeletesTest(SimpleTestCase):
    def test_every_held_delete_is_counted_not_only_the_last_per_model(self):
        entries = [
            {"model": "ipam.vrf", "held_by": [{"model": "ipam.ipaddress", "count": 3}]},
            {"model": "ipam.vrf", "held_by": [{"model": "ipam.ipaddress", "count": 2}]},
            {"model": "dcim.site", "held_by": [{"model": "dcim.device", "count": 1}]},
        ]
        held = _held_deletes_by_model(entries)
        self.assertEqual(held["ipam.vrf"]["deletes"], 2)
        self.assertEqual(held["ipam.vrf"]["referencing_rows"], {"ipam.ipaddress": 5})
        self.assertEqual(held["dcim.site"]["deletes"], 1)

    def test_nothing_held_is_empty(self):
        self.assertEqual(_held_deletes_by_model(None), {})


class DeleteAccountingTest(SimpleTestCase):
    def test_the_difference_is_split_by_direction_and_never_negative(self):
        written = {"ipam.prefix": 504, "dcim.cabletermination": 700}

        out = _delete_accounting(_ingestion(869, accounting=ACCOUNTING), written)

        self.assertEqual(out["staged_deletes"], 869)
        self.assertEqual(out["written_deletes"], 1204)
        self.assertEqual(
            out["staged_more_than_logged"], {"dcim.cable": 350, "ipam.vrf": 15}
        )
        self.assertEqual(out["logged_more_than_staged"], {"dcim.cabletermination": 700})
        self.assertEqual(out["held_by_protecting_reference"]["ipam.vrf"]["deletes"], 15)
        self.assertEqual(out["skipped_at_merge"], 2)
        self.assertTrue(out["causes_of_a_difference"])

    def test_without_a_record_it_says_so_rather_than_inventing_a_split(self):
        out = _delete_accounting(_ingestion(869), {"ipam.prefix": 504})

        self.assertFalse(out["recorded_at_merge"])
        self.assertIn("not recorded", out["note"])
        self.assertNotIn("staged_deletes_by_model", out)
        self.assertEqual(out["written_deletes"], 504)

    def test_unavailable_change_log_is_stated_and_no_gap_is_invented(self):
        out = _delete_accounting(_ingestion(869, accounting=ACCOUNTING), None)

        self.assertIsNone(out["written_deletes"])
        self.assertIn("not available", out["note"])
        self.assertNotIn("staged_more_than_logged", out)

    def test_matching_counts_leave_no_difference(self):
        accounting = {
            "logical_action_by_model": {"ipam.prefix.delete": 504},
            "held_deletes_by_model": {},
        }
        out = _delete_accounting(
            _ingestion(504, accounting=accounting), {"ipam.prefix": 504}
        )
        self.assertEqual(out["staged_more_than_logged"], {})
        self.assertEqual(out["logged_more_than_staged"], {})

    def test_the_merge_job_record_wins_over_the_sync_job_record(self):
        ingestion = SimpleNamespace(
            deleted_change_count=1,
            job=SimpleNamespace(data={"merge_accounting": {"failed": 9}}),
            merge_job=SimpleNamespace(
                data={
                    "merge_accounting": {
                        "failed": 1,
                        "logical_action_by_model": {"ipam.prefix.delete": 1},
                    }
                }
            ),
        )
        self.assertEqual(_delete_accounting(ingestion, {})["failed_at_merge"], 1)
