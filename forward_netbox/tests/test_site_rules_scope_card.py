# Before a sync moves devices, an operator needs to see what their rules will do:
# how many devices each rule places and how many stay at their Forward location,
# and a rule that matches nothing (a mistyped tag or pattern) has to stand out.
# The support bundle carries the same figures under the page's own labels, with the
# counts but not the operator's patterns or site names.
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
from forward_netbox.utilities.bundle_ui_figures import scope_figures
from forward_netbox.views import _bundle_safe_report
from forward_netbox.views import _site_rules_card

SUMMARY = {
    "available": True,
    "total": 10,
    "pinned_by_tag": 1,
    "by_location": 3,
    "rules": [
        {
            "rank": 1,
            "kind": "location",
            "pattern": "Hall*",
            "site": "campus",
            "devices": 5,
        },
        {"rank": 2, "kind": "tag", "pattern": "Prod", "site": "core", "devices": 0},
        {
            "rank": 3,
            "kind": "location",
            "pattern": "Annex*",
            "site": "annex",
            "devices": 1,
        },
    ],
}


class CardPayloadTest(SimpleTestCase):
    def test_no_rules_means_no_card(self):
        self.assertFalse(_site_rules_card({})["available"])
        self.assertFalse(
            _site_rules_card({"site_rules": {"available": False}})["available"]
        )

    def test_the_figures_are_the_page_labels_in_order(self):
        card = _site_rules_card({"site_rules": SUMMARY})

        self.assertEqual(
            card["figures"],
            [
                ("Site rules", 3),
                ("Placed by a NetBox_Site_ tag", 1),
                ("Left at their Forward location", 3),
            ],
        )

    def test_a_rule_that_places_nothing_is_flagged(self):
        card = _site_rules_card({"site_rules": SUMMARY})

        self.assertEqual(
            [r["matches_nothing"] for r in card["rules"]], [False, True, False]
        )
        self.assertEqual(card["rules_matching_nothing"], 1)


class BundleTest(SimpleTestCase):
    def test_the_bundle_keeps_counts_and_drops_patterns_and_site_names(self):
        safe = _bundle_safe_report({"site_rules": SUMMARY})

        rules = safe["site_rules"]["rules"]
        self.assertEqual([r["devices"] for r in rules], [5, 0, 1])
        self.assertEqual([r["rank"] for r in rules], [1, 2, 3])
        text = str(safe)
        for name in ("Hall*", "Prod", "campus", "Annex*", "annex"):
            self.assertNotIn(name, text)

    def test_the_page_figures_are_in_the_bundle_under_the_page_labels(self):
        rows = {
            row["label"]: row["value"]
            for row in scope_figures({"site_rules": SUMMARY})
            if row["label"] not in ("Include tags",)
        }

        self.assertEqual(rows["Site rules"], 3)
        self.assertEqual(rows["Placed by a NetBox_Site_ tag"], 1)
        self.assertEqual(rows["Left at their Forward location"], 3)
        self.assertEqual(rows["Site rule 1 (location)"], 5)
        self.assertEqual(rows["Site rule 2 (tag)"], 0)

    def test_no_rules_adds_no_figures(self):
        labels = [row["label"] for row in scope_figures({})]

        self.assertFalse([label for label in labels if "ite rule" in label])


class PageTest(TestCase):
    def setUp(self):
        source = ForwardSource.objects.create(
            name="card-src",
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
            name="card-sync",
            source=source,
            parameters={"snapshot_id": "latestProcessed"},
        )
        user = get_user_model().objects.create_user(username="card-admin", password="x")
        user.is_superuser = True
        user.is_staff = True
        user.save()
        self.client = Client()
        self.client.force_login(user)
        self.url = reverse(
            "plugins:forward_netbox:forwardsync_scope_reconciliation",
            kwargs={"pk": self.sync.pk},
        )

    def _report(self, **data):
        Job.objects.create(
            object_type=ContentType.objects.get_for_model(ForwardSync),
            object_id=self.sync.pk,
            name=f"{self.sync.name} - scope reconciliation",
            status=JobStatusChoices.STATUS_COMPLETED,
            job_id=uuid4(),
            data=data,
        )
        return self.client.get(self.url).content.decode()

    def test_the_card_shows_what_each_rule_places(self):
        text = self._report(site_rules=SUMMARY)

        self.assertIn("Site Mapping Rules", text)
        flat = " ".join(re.sub(r"<[^>]+>", " ", text).split())
        self.assertIn("Placed by a NetBox_Site_ tag 1", flat)
        self.assertIn("Left at their Forward location 3", flat)
        self.assertIn("Hall*", flat)
        self.assertIn("campus", flat)

    def test_a_rule_that_matches_no_device_is_flagged_on_the_page(self):
        text = self._report(site_rules=SUMMARY)

        # One badge, on the one rule that places nothing (the warning above the
        # table says the same words in a sentence).
        self.assertEqual(text.count('text-bg-warning">matches no device'), 1)

    def test_the_card_says_the_forward_location_is_the_default(self):
        text = self._report(site_rules=SUMMARY)

        self.assertIn("unless a mapping applies", text)

    def test_no_rules_no_card(self):
        text = self._report(unmanaged={"untagged_total": 0})

        self.assertNotIn("Site Mapping Rules", text)
