# After a clean sync a customer asked where the include tags they had added went,
# what the 869 deletions were, and whether Forward had collected fewer devices.
# The bundle could not say: the merged branch is deleted with its changes, the
# tag history was not exported, and the collection change between runs was left
# to the reader. Synthetic names only; counts only in the output.
import uuid
from unittest.mock import patch

from core.models import ObjectChange
from dcim.models import Device
from dcim.models import DeviceRole
from dcim.models import DeviceType
from dcim.models import Manufacturer
from dcim.models import Site
from django.contrib.contenttypes.models import ContentType
from django.test import SimpleTestCase
from django.test import TestCase
from users.models import User

from forward_netbox.models import ForwardIngestion
from forward_netbox.models import ForwardSource
from forward_netbox.models import ForwardSync
from forward_netbox.utilities import bundle_diagnostics as diagnostics
from forward_netbox.utilities.scope_reconciliation import _collection_failure_reason


class FailureTokenTest(SimpleTestCase):
    def test_a_collection_failure_is_its_token(self):
        self.assertEqual(
            _collection_failure_reason(
                "DeviceSnapshotResult.collectionFailed("
                "DeviceCollectionError.AUTHENTICATION_FAILED)"
            ),
            "AUTHENTICATION_FAILED",
        )

    def test_a_processing_failure_has_no_closing_bracket(self):
        self.assertEqual(
            _collection_failure_reason(
                "DeviceSnapshotResult.processingFailed("
                "DeviceProcessingError.PARSER_EXCEPTION)"
            ),
            "PARSER_EXCEPTION",
        )

    def test_an_unparseable_value_does_not_keep_a_bracket(self):
        self.assertEqual(_collection_failure_reason("Something.odd)"), "odd")


class TrendTest(SimpleTestCase):
    def report(self, completed, not_collected, reasons, job_pk):
        return {
            "job_pk": job_pk,
            "completed": None,
            "in_scope_completed": completed,
            "not_collected": not_collected,
            "not_collected_by_reason": reasons,
        }

    def test_change_against_the_previous_report_names_the_new_failures(self):
        reports = [
            self.report(
                3387, 178, {"PARSER_EXCEPTION": 59, "AUTHENTICATION_FAILED": 47}, 3
            ),
            self.report(
                3509, 76, {"PARSER_EXCEPTION": 7, "AUTHENTICATION_FAILED": 2}, 2
            ),
        ]
        with patch.object(
            diagnostics, "_scope_trend_reports", return_value=iter(reports)
        ):
            trend = diagnostics._snapshot_collection_trend(object())

        change = trend["change_vs_previous"]
        self.assertEqual(change["completed_delta"], -122)
        self.assertEqual(change["not_collected_delta"], 102)
        self.assertEqual(
            change["reason_deltas"],
            {"PARSER_EXCEPTION": 52, "AUTHENTICATION_FAILED": 45},
        )

    def test_duplicate_reports_from_one_run_are_collapsed(self):
        same = self.report(3387, 178, {"A": 1}, 3)
        reports = [same, dict(same, job_pk=2), self.report(3509, 76, {"A": 1}, 1)]
        with patch.object(
            diagnostics, "_scope_trend_reports", return_value=iter(reports)
        ):
            trend = diagnostics._snapshot_collection_trend(object())
        self.assertEqual(len(trend["runs"]), 2)

    def test_a_single_report_has_no_change(self):
        with patch.object(
            diagnostics,
            "_scope_trend_reports",
            return_value=iter([self.report(10, 0, {}, 1)]),
        ):
            self.assertIsNone(
                diagnostics._snapshot_collection_trend(object())["change_vs_previous"]
            )


class IncludeTagTimelineTest(TestCase):
    def setUp(self):
        source = ForwardSource.objects.create(
            name="tag-tl-source",
            type="saas",
            url="https://fwd.app",
            parameters={
                "username": "u@example.com",
                "password": "secret",
                "verify": True,
                "network_id": "net-1",
                "device_tag_include_tags": ["Tag One"],
            },
        )
        self.sync_user = User.objects.create(username="tag-tl-sync")
        self.other_user = User.objects.create(username="tag-tl-person")
        self.sync = ForwardSync.objects.create(
            name="tag-tl-sync",
            source=source,
            user=self.sync_user,
            parameters={"snapshot_id": "latestProcessed"},
        )
        site = Site.objects.create(name="tl-site", slug="tl-site")
        mfr = Manufacturer.objects.create(name="tl-m", slug="tl-m")
        dt = DeviceType.objects.create(manufacturer=mfr, model="tl-d", slug="tl-d")
        role = DeviceRole.objects.create(name="tl-r", slug="tl-r")
        self.device = Device.objects.create(
            name="tl-device", site=site, device_type=dt, role=role, status="active"
        )
        self.sync_request = uuid.uuid4()
        ForwardIngestion.objects.create(
            sync=self.sync, change_request_id=self.sync_request
        )

    def change(self, user, before, after, *, by_sync_run=False):
        ObjectChange.objects.create(
            request_id=self.sync_request if by_sync_run else uuid.uuid4(),
            user=user,
            changed_object_type=ContentType.objects.get_for_model(Device),
            changed_object_id=self.device.pk,
            object_repr="x",
            action="update",
            prechange_data={"tags": before},
            postchange_data={"tags": after},
        )

    def test_gains_and_losses_are_split_by_who_made_them_without_naming_the_tag(self):
        self.change(self.other_user, [], ["Tag One"])
        self.change(self.other_user, ["Tag One"], [], by_sync_run=True)
        self.change(self.sync_user, ["Other"], ["Other", "Unrelated"], by_sync_run=True)

        timeline = diagnostics._include_tag_timeline(self.sync)

        self.assertEqual(timeline["gained_by_other"], 1)
        self.assertEqual(timeline["gained_by_sync_run"], 0)
        self.assertEqual(timeline["lost_by_sync_run"], 1)
        self.assertEqual(timeline["lost_by_other"], 0)
        self.assertEqual(timeline["include_tag_count"], 1)
        self.assertNotIn("Tag One", str(timeline))

    def test_a_manual_merge_user_does_not_hide_a_sync_runs_change(self):
        # The merge replays changes under the operator's user; the request id is
        # what says a sync run wrote it.
        self.change(self.other_user, ["Tag One"], [], by_sync_run=True)
        timeline = diagnostics._include_tag_timeline(self.sync)
        self.assertEqual(timeline["lost_by_sync_run"], 1)

    def test_a_tag_recorded_by_name_matches_the_slug_of_the_include_tag(self):
        self.change(self.other_user, ["tag-one"], [])
        self.assertEqual(
            diagnostics._include_tag_timeline(self.sync)["lost_by_other"], 1
        )
