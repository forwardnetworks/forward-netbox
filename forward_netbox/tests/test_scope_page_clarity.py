"""The Scope Reconciliation page shows the split, never a figure that can be
misread as the uncovered count; the cleanup checklist covers generic SNMP
endpoints and devices the management step recorded nothing for.

A customer read a summed row (128) as 128 uncovered devices; the truth was
4 uncovered (created by this sync) + 124 not managed (never created by it).
"""

import re
from uuid import uuid4

from core.choices import JobStatusChoices
from core.models import Job
from django.contrib.auth import get_user_model
from django.contrib.contenttypes.models import ContentType
from django.test import Client
from django.test import SimpleTestCase
from django.test import TestCase
from django.urls import reverse

from forward_netbox.models import ForwardSource
from forward_netbox.models import ForwardSync
from forward_netbox.utilities.cleanup_checklist import cleanup_items
from forward_netbox.utilities.cleanup_checklist import DELETES_AFTER_REVIEW

URLS = {"in_forward_not_in_scope": "/tagless/"}


class ScopePageSplitRowsTest(TestCase):
    def setUp(self):
        source = ForwardSource.objects.create(
            name="clarity-src",
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
        self.sync = ForwardSync.objects.create(
            name="clarity-sync",
            source=source,
            parameters={"snapshot_id": "latestProcessed"},
        )
        user = get_user_model().objects.create_user(
            username="clarity-admin", password="x"
        )
        user.is_superuser = True
        user.is_staff = True
        user.save()
        self.client = Client()
        self.client.force_login(user)
        self.url = reverse(
            "plugins:forward_netbox:forwardsync_scope_reconciliation",
            kwargs={"pk": self.sync.pk},
        )

    def _rows(self):
        text = self.client.get(self.url).content.decode()
        rows = {
            re.sub(r"<[^>]+>", "", label).strip(): " ".join(
                re.sub(r"<[^>]+>", " ", cell).split()
            )
            for label, cell in re.findall(
                r'<th[^>]*scope="row"[^>]*>(.*?)</th>\s*<td[^>]*>(.*?)</td>',
                text,
                re.S,
            )
        }
        return text, rows

    def test_uncovered_and_not_managed_are_separate_rows_and_the_sum_is_text(self):
        Job.objects.create(
            object_type=ContentType.objects.get_for_model(ForwardSync),
            object_id=self.sync.pk,
            name=f"{self.sync.name} - scope reconciliation",
            status=JobStatusChoices.STATUS_COMPLETED,
            job_id=uuid4(),
            data={
                "unmanaged": {
                    "untagged_total": 128,
                    "owned_untagged": 4,
                    "unclaimed": 124,
                }
            },
        )

        text, rows = self._rows()

        self.assertTrue(rows["Uncovered: created by this sync"].startswith("4 "))
        self.assertTrue(rows["Not managed by this sync"].startswith("124 "))
        self.assertFalse([label for label in rows if "uncovered + not" in label])
        # 128 is never a row's figure; it appears only in the explanatory line.
        self.assertFalse([cell for cell in rows.values() if cell.startswith("128")])
        self.assertIn(
            "128 devices carry no include tag: 4 uncovered + 124 not managed", text
        )


class GenericEndpointAndUnrecordedPrimaryIpItemsTest(SimpleTestCase):
    GENERIC = {
        "unmanaged": {
            "owned_absence": {
                "present_untagged": 10,
                "endpoint_detail": [
                    {"reason": "endpoint_generic", "count": 6},
                    {"reason": "endpoint_untagged", "count": 4},
                ],
            }
        }
    }

    def _by_key(self, items):
        return {item["key"]: item for item in items}

    def test_generic_endpoints_name_exactly_the_two_choices_with_the_count(self):
        items = self._by_key(
            cleanup_items(
                self.GENERIC,
                urls={"generic_endpoints_not_imported": "/uncovered/"},
            )
        )

        item = items["generic_endpoints_not_imported"]
        self.assertEqual(item["count"], 6)
        self.assertEqual(item["url"], "/uncovered/")
        self.assertEqual(item["kind"], DELETES_AFTER_REVIEW)
        self.assertEqual(len(item["steps"]), 2)
        keep, remove = item["steps"]
        self.assertIn("Import generic SNMP endpoints", keep)
        self.assertIn("delete", remove.lower())

    def test_the_add_a_tag_advice_no_longer_counts_the_generic_endpoints(self):
        items = self._by_key(cleanup_items(self.GENERIC, urls=URLS))

        self.assertEqual(items["in_forward_not_in_scope"]["count"], 4)

    def test_only_generic_endpoints_leave_no_add_a_tag_item(self):
        report = {
            "unmanaged": {
                "owned_absence": {
                    "present_untagged": 6,
                    "endpoint_detail": [{"reason": "endpoint_generic", "count": 6}],
                }
            }
        }

        keys = [item["key"] for item in cleanup_items(report, urls=URLS)]

        self.assertEqual(keys, ["generic_endpoints_not_imported"])

    def test_unrecorded_primary_ip_gives_the_mgmt_tag_step(self):
        items = self._by_key(cleanup_items(None, no_primary_ip_unrecorded=7))

        item = items["primary_ip_none_recorded"]
        self.assertEqual(item["count"], 7)
        steps = " ".join(item["steps"])
        self.assertIn("Mgmt_<interface>", steps)
        self.assertIn("by hand", steps)

    def test_nothing_to_do_adds_no_item(self):
        self.assertEqual(cleanup_items(None, no_primary_ip_unrecorded=0), [])
