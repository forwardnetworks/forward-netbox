from unittest.mock import patch

from django.test import TestCase
from utilities.exceptions import AbortRequest

from forward_netbox.models import ForwardSource
from forward_netbox.models import ForwardSync
from forward_netbox.utilities import ownership
from forward_netbox.utilities.ownership import ownership_lock_holder_summary
from forward_netbox.utilities.ownership import ownership_write_lock
from forward_netbox.utilities.ownership import OwnershipLockTimeout

ACQUIRE = "forward_netbox.utilities.ownership._try_acquire_ownership_lock"
HOLDER = "forward_netbox.utilities.ownership.ownership_lock_holder_summary"


class OwnershipLockWaitTest(TestCase):
    def test_an_unbounded_lock_still_waits_for_as_long_as_it_takes(self):
        with ownership_write_lock():
            pass

    def test_a_bounded_lock_is_taken_when_free(self):
        with patch(ACQUIRE, return_value=True) as acquire:
            with ownership_write_lock(max_wait_seconds=5):
                pass

        acquire.assert_called_once()

    def test_a_bounded_lock_polls_until_the_holder_lets_go(self):
        with patch(ACQUIRE, side_effect=[False, False, True]) as acquire:
            with patch.object(ownership, "OWNERSHIP_LOCK_POLL_SECONDS", 0):
                with ownership_write_lock(max_wait_seconds=5):
                    pass

        self.assertEqual(acquire.call_count, 3)

    def test_a_held_lock_times_out_and_names_the_holding_session(self):
        holder = (
            "held by database session 4242 (idle in transaction, "
            "transaction open 17m 3s, in that state 17m 3s)"
        )
        with patch(ACQUIRE, return_value=False), patch(HOLDER, return_value=holder):
            with patch.object(ownership, "OWNERSHIP_LOCK_POLL_SECONDS", 0):
                with self.assertRaises(OwnershipLockTimeout) as caught:
                    with ownership_write_lock(max_wait_seconds=0):
                        self.fail("the body must not run without the lock")

        message = caught.exception.message
        self.assertIn("4242", message)
        self.assertIn("idle in transaction", message)
        self.assertIn("nothing was changed", message)

    def test_the_timeout_is_an_abort_request_so_netbox_shows_it_on_the_page(self):
        self.assertTrue(issubclass(OwnershipLockTimeout, AbortRequest))

    def test_the_holder_summary_never_raises_and_carries_no_query_text(self):
        summary = ownership_lock_holder_summary()

        self.assertIsInstance(summary, str)
        self.assertNotIn("SELECT", summary.upper())

    def test_deleting_a_sync_stops_at_the_limit_instead_of_hanging(self):
        source = ForwardSource.objects.create(
            name="lock-src",
            type="saas",
            url="https://fwd.app",
            parameters={"username": "u@x", "password": "p", "network_id": "n"},
        )
        sync = ForwardSync.objects.create(
            name="lock-sync",
            source=source,
            parameters={"snapshot_id": "latestProcessed"},
        )

        with patch(ACQUIRE, return_value=False), patch(HOLDER, return_value="held"):
            with patch.object(ownership, "OWNERSHIP_LOCK_OPERATOR_WAIT_SECONDS", 0):
                with patch.object(ownership, "OWNERSHIP_LOCK_POLL_SECONDS", 0):
                    with self.assertRaises(OwnershipLockTimeout):
                        sync.delete()

        self.assertTrue(ForwardSync.objects.filter(pk=sync.pk).exists())

    def test_bulk_deleting_syncs_is_bounded_too(self):
        with patch(ACQUIRE, return_value=False), patch(HOLDER, return_value="held"):
            with patch.object(ownership, "OWNERSHIP_LOCK_OPERATOR_WAIT_SECONDS", 0):
                with patch.object(ownership, "OWNERSHIP_LOCK_POLL_SECONDS", 0):
                    with self.assertRaises(OwnershipLockTimeout):
                        ForwardSync.objects.all().delete()
