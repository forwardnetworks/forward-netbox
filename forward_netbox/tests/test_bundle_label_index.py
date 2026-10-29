import json
import re
from collections import Counter
from pathlib import Path

from django.contrib.auth import get_user_model
from django.test import TestCase
from django.urls import reverse

from forward_netbox.models import ForwardSource
from forward_netbox.models import ForwardSync
from forward_netbox.utilities.bundle_label_index import PAGE_LABEL_PATHS
from forward_netbox.utilities.export_redaction import export_safe_payload

TEMPLATES = Path(__file__).resolve().parents[1] / "templates" / "forward_netbox"
PAGES = {
    "Health": "forwardsync_health.html",
    "Scope Reconciliation": "forwardsync_scope_reconciliation.html",
    "Drift report": "forwardsync_drift_report.html",
}
ROW_LABEL = re.compile(r'<th[^>]*scope="row"[^>]*>\s*{% trans "([^"]+)" %}\s*</th>')


class BundleLabelIndexTest(TestCase):
    """A figure a customer reads off a page can be found in their support bundle
    by the page's own label. Adding a row to a page without saying where the
    bundle holds it fails here, so the rule does not depend on remembering it."""

    def page_labels(self, page):
        return set(ROW_LABEL.findall((TEMPLATES / PAGES[page]).read_text()))

    def test_every_page_row_has_a_place_in_the_bundle(self):
        for page in PAGES:
            missing = self.page_labels(page) - set(PAGE_LABEL_PATHS[page])
            self.assertFalse(
                missing,
                f"{page} rows with no entry in bundle_label_index: {sorted(missing)}",
            )

    def test_no_entry_outlives_its_row(self):
        for page in PAGES:
            stale = set(PAGE_LABEL_PATHS[page]) - self.page_labels(page)
            self.assertFalse(
                stale, f"{page} entries whose row is gone: {sorted(stale)}"
            )

    def test_a_label_repeated_on_a_page_maps_to_every_row(self):
        for page, name in PAGES.items():
            counts = Counter(ROW_LABEL.findall((TEMPLATES / name).read_text()))
            for label, count in counts.items():
                paths = PAGE_LABEL_PATHS[page][label]
                self.assertEqual(
                    len(paths) if isinstance(paths, list) else 1,
                    count,
                    f"{page} / {label} appears {count} time(s)",
                )

    def test_no_path_goes_through_a_key_the_export_filter_changes(self):
        # The filter drops or renames keys by name, so a path through one points at
        # something the bundle does not hold.
        for page, label, path in self.all_paths():
            for key in path.split("."):
                if key == "<integration>":
                    continue
                self.assertEqual(
                    export_safe_payload({key: 1}),
                    {key: 1},
                    f"{page} / {label}: {path} goes through {key!r}",
                )

    def all_paths(self):
        for page, rows in PAGE_LABEL_PATHS.items():
            for label, value in rows.items():
                for path in value if isinstance(value, list) else [value]:
                    yield page, label, path

    def test_every_path_starts_in_a_bundle_section(self):
        # Built from a sync that has never run: every section is present.
        user = get_user_model().objects.create_superuser(
            username="label-index-admin", password="TestPassword123!", email="a@b.c"
        )
        source = ForwardSource.objects.create(
            name="label-index-src",
            type="saas",
            url="https://fwd.app",
            parameters={
                "username": "u@example.com",
                "password": "x",
                "network_id": "n",
            },
        )
        sync = ForwardSync.objects.create(
            name="label-index-sync",
            source=source,
            parameters={"snapshot_id": "latestProcessed"},
        )
        self.client.force_login(user)
        payload = json.loads(
            self.client.get(
                reverse(
                    "plugins:forward_netbox:forwardsync_support_bundle",
                    kwargs={"pk": sync.pk},
                )
            ).content
        )
        for page, label, path in self.all_paths():
            self.assertIn(path.split(".")[0], payload, f"{page} / {label}")
