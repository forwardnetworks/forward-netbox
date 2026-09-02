# A delete the apply refused was still promoted as done.
#
# The durable workload state is staged BEFORE the branch applies and promoted
# at merge, so it recorded every delete the delta computed rather than every
# delete that happened. `newly_explicit_deletes` then treats a previous
# `delete` entry as settled, so the next run skipped it: the row stayed in
# NetBox, the plugin believed it was gone, nothing retried, and the report went
# quiet. 2.8.9 closed the PROTECT half by never staging those; this closes the
# rest, for a delete refused by any cause.
from types import SimpleNamespace
from unittest.mock import Mock

from django.test import TestCase

from forward_netbox.models import ForwardIngestion
from forward_netbox.models import ForwardSource
from forward_netbox.models import ForwardSync
from forward_netbox.models import ForwardWorkloadState
from forward_netbox.utilities.sync_reporting import persist_refused_delete_identities
from forward_netbox.utilities.sync_reporting import record_refused_delete
from forward_netbox.utilities.sync_reporting import REFUSED_DELETE_IDENTITIES_KEY
from forward_netbox.utilities.workload_state import build_state_entries
from forward_netbox.utilities.workload_state import decode_state_entries
from forward_netbox.utilities.workload_state import encode_state_entries
from forward_netbox.utilities.workload_state import promote_workload_states_locked
from forward_netbox.utilities.workload_state import refused_delete_identities

MODEL = "netbox_dlm.softwareversion"
COALESCE = [["platform_slug", "version"]]


class RefusedDeleteTest(TestCase):
    def setUp(self):
        source = ForwardSource.objects.create(
            name="refused-src", type="saas", url="https://fwd.app", status="ready"
        )
        self.sync = ForwardSync.objects.create(name="refused-sync", source=source)
        self.ingestion = ForwardIngestion.objects.create(
            sync=self.sync, snapshot_id="snap-1"
        )

    def _runner(self):
        return SimpleNamespace(
            logger=Mock(),
            _model_coalesce_fields={MODEL: COALESCE},
        )

    def _row(self, version="1.0"):
        return {"platform_slug": "ios", "version": version}

    def _stage(self, *, deletes, upserts=()):
        entries = build_state_entries(MODEL, list(upserts), COALESCE)
        entries.update(
            build_state_entries(MODEL, list(deletes), COALESCE, action="delete")
        )
        payload, checksum = encode_state_entries(entries)
        return ForwardWorkloadState.objects.create(
            sync=self.sync,
            ingestion=self.ingestion,
            model_string=MODEL,
            parameter_hash="p",
            identity_contract_hash="c",
            payload=payload,
            payload_checksum=checksum,
            row_count=len(entries),
            snapshot_id="snap-1",
            is_current=False,
        )

    def _promoted_entries(self):
        state = ForwardWorkloadState.objects.get(sync=self.sync, is_current=True)
        return decode_state_entries(state.payload, state.payload_checksum)

    def test_a_refused_delete_is_absent_from_the_promoted_state(self):
        runner = self._runner()
        record_refused_delete(runner, MODEL, self._row("1.0"))
        persist_refused_delete_identities(runner, self.ingestion)
        self.ingestion.save(update_fields=["snapshot_info"])
        self._stage(deletes=[self._row("1.0"), self._row("2.0")])

        promote_workload_states_locked(self.ingestion)

        entries = self._promoted_entries()
        # 2.0 was deleted and stays recorded; 1.0 was refused and is absent, so
        # the next delta recomputes it as a fresh delete and retries.
        self.assertEqual(len(entries), 1)
        self.assertNotIn("1.0", str(entries))
        self.assertIn("2.0", str(entries))

    def test_an_upsert_entry_is_never_dropped(self):
        runner = self._runner()
        record_refused_delete(runner, MODEL, self._row("1.0"))
        persist_refused_delete_identities(runner, self.ingestion)
        self.ingestion.save(update_fields=["snapshot_info"])
        # Same identity, upsert action: a refusal must not remove a row the
        # run actually wrote.
        self._stage(deletes=[], upserts=[self._row("1.0")])

        promote_workload_states_locked(self.ingestion)

        self.assertEqual(len(self._promoted_entries()), 1)

    def test_an_ordinary_run_promotes_untouched(self):
        state = self._stage(deletes=[self._row("1.0")])
        before = (state.payload_checksum, state.row_count)

        promote_workload_states_locked(self.ingestion)

        state.refresh_from_db()
        self.assertEqual((state.payload_checksum, state.row_count), before)
        self.assertTrue(state.is_current)

    def test_nothing_is_persisted_when_no_delete_was_refused(self):
        runner = self._runner()
        self.assertEqual(persist_refused_delete_identities(runner, self.ingestion), 0)
        self.assertNotIn(
            REFUSED_DELETE_IDENTITIES_KEY, self.ingestion.snapshot_info or {}
        )

    def test_the_recorded_identity_is_the_state_key_not_a_row(self):
        runner = self._runner()
        record_refused_delete(runner, MODEL, self._row("1.0"))
        persist_refused_delete_identities(runner, self.ingestion)
        recorded = refused_delete_identities(self.ingestion)[MODEL]
        entries = build_state_entries(
            MODEL, [self._row("1.0")], COALESCE, action="delete"
        )
        self.assertEqual(recorded, set(entries))

    def test_an_unkeyable_row_is_recorded_as_empty_and_ignored(self):
        # A row with no usable coalesce key cannot be tombstoned either, so it
        # must not crash the recorder or match a real identity.
        runner = self._runner()
        record_refused_delete(runner, MODEL, {})
        persist_refused_delete_identities(runner, self.ingestion)
        self.assertEqual(refused_delete_identities(self.ingestion).get(MODEL), set())
