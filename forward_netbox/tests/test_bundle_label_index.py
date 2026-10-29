import json
import re
from collections import Counter
from pathlib import Path

from django.contrib.auth import get_user_model
from django.test import TestCase
from django.urls import reverse

from forward_netbox.models import ForwardSource
from forward_netbox.models import ForwardSync
from forward_netbox.utilities.bundle_label_index import PAGE_CARD_PATHS
from forward_netbox.utilities.bundle_label_index import PAGE_LABEL_PATHS
from forward_netbox.utilities.export_redaction import export_safe_payload

TEMPLATES = Path(__file__).resolve().parents[1] / "templates" / "forward_netbox"
PAGES = {
    "Health": "forwardsync_health.html",
    "Scope Reconciliation": "forwardsync_scope_reconciliation.html",
    "Drift report": "forwardsync_drift_report.html",
    "Ingestion": "forwardingestion.html",
    "Module readiness": "forwardsync_module_readiness.html",
    "Dependency preview": "forwardsync_dependency_preview.html",
    "Audits": "forwardsync_audits.html",
}
CARD_HEADING = re.compile(
    r'<h5[^>]*class="card-header[^>]*>\s*{% trans "([^"]+)" %}', re.S
)
ANY_CARD_HEADING = re.compile(r'class="card-header')
# A card the page pulls in from another template, or a raw-data card titled in the
# include: `{% include '...configcontext_data.html' with title="Model Results" %}`.
INCLUDE_TITLE = re.compile(r"configcontext_data\.html' with title=\"([^\"]+)\"")
INCLUDE = re.compile(r"{% include 'forward_netbox/partials/([\w]+\.html)'")
PARTIALS = TEMPLATES / "partials"

# Partials that hold a card for a page above, or none (buttons, tabs, status).
CARD_PARTIALS = {"ingestion_statistics.html", "change_explainability.html"}
NO_CARD_PARTIALS = {
    "ingestion_merge_button.html",
    "object_tabs.html",
    "ingestion_status.html",
    "job_logs.html",
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
                # One path per row, or more when one row reads several fields.
                self.assertGreaterEqual(
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
        for index in (PAGE_LABEL_PATHS, PAGE_CARD_PATHS):
            for page, rows in index.items():
                for label, value in rows.items():
                    for path in value if isinstance(value, list) else [value]:
                        yield page, label, path

    def test_every_card_has_a_place_in_the_bundle(self):
        # A card holds tables labelled from data and figures written inside
        # sentences, which no row label reaches; its heading leads to them.
        for page, name in PAGES.items():
            text = (TEMPLATES / name).read_text()
            headings = set(CARD_HEADING.findall(text))
            headings |= set(INCLUDE_TITLE.findall(text))
            for partial in INCLUDE.findall(text):
                if partial in CARD_PARTIALS:
                    headings |= set(
                        CARD_HEADING.findall((PARTIALS / partial).read_text())
                    )
                else:
                    self.assertIn(
                        partial,
                        NO_CARD_PARTIALS | {"ingestion_status.html"},
                        f"{page} includes {partial}: say whether it holds a card",
                    )
            self.assertEqual(
                len(ANY_CARD_HEADING.findall(text)),
                len(CARD_HEADING.findall(text)),
                f"{page} has a card heading that is not a plain trans string",
            )
            self.assertEqual(
                headings,
                set(PAGE_CARD_PATHS[page]),
                f"{page}: cards and PAGE_CARD_PATHS disagree",
            )

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


class BundleMatchesPagesTest(TestCase):
    """The value a page shows for a row is the value the bundle holds at the path
    the index gives for that row's label. Rendered from one populated sync, so a
    path that points at a neighbouring field, or a value computed two ways, fails."""

    @classmethod
    def setUpTestData(cls):
        from core.choices import JobStatusChoices
        from core.models import Job
        from django.contrib.contenttypes.models import ContentType
        from django.utils import timezone

        from forward_netbox.models import ForwardIngestion

        cls.user = get_user_model().objects.create_superuser(
            username="pages-admin", password="TestPassword123!", email="p@e.c"
        )
        source = ForwardSource.objects.create(
            name="pages-src",
            type="saas",
            url="https://fwd.app",
            parameters={
                "username": "u@example.com",
                "password": "x",
                "network_id": "n",
            },
        )
        cls.sync = ForwardSync.objects.create(
            name="pages-sync",
            source=source,
            parameters={"snapshot_id": "latestProcessed"},
        )
        cls.ingestion = ForwardIngestion.objects.create(
            sync=cls.sync,
            snapshot_selector="latestProcessed",
            snapshot_id="snap-77",
            baseline_ready=True,
            applied_change_count=7,
            failed_change_count=3,
            created_change_count=2,
            updated_change_count=4,
            deleted_change_count=1,
            snapshot_info={"state": "PROCESSED"},
            snapshot_metrics={"numSuccessfulDevices": 3829},
        )
        now = timezone.now()
        content_type = ContentType.objects.get_for_model(ForwardSync)

        def job(name, data, serial):
            Job.objects.create(
                object_type=content_type,
                object_id=cls.sync.pk,
                name=f"{cls.sync.name} - {name}",
                status=JobStatusChoices.STATUS_COMPLETED,
                job_id=f"123e4567-e89b-12d3-a456-4266141749{serial}",
                created=now,
                started=now,
                completed=now,
                data=data,
            )

        job(
            "scope reconciliation",
            {
                "netbox_device_count": 410,
                "forward_in_scope_completed": 301,
                "forward_in_scope_endpoints": 12,
                "forward_tagged_backfilled": 63,
                "netbox_out_of_scope": 58,
                "netbox_empty_orphan_site_count": 6,
                "forward_missing_in_netbox": 9,
                "unmanaged": {"untagged_total": 270, "owned_untagged": 58},
            },
            "01",
        )
        job(
            "module readiness",
            {
                "payload": {
                    "candidate_rows": 40,
                    "existing_bay_rows": 31,
                    "unique_missing_bays": 9,
                    "missing_device_rows": 4,
                }
            },
            "02",
        )
        job(
            "dependency preview",
            {
                "generated_at": now.isoformat(),
                "context": {"snapshot_id": "snap-77", "snapshot_selector": "latest"},
                "plan_preview": {
                    "planned_shards": 89,
                    "estimated_changes": 721,
                    "model_count": 17,
                    "retry_risk": "medium",
                    "delete_dependency_plan": {
                        "status": "ready",
                        "delete_rows": 29,
                        "delete_shards": 3,
                        "max_delete_shard_changes": 11,
                    },
                },
                "plan_items": [],
                "model_results": [],
            },
            "03",
        )

    def bundle(self):
        self.client.force_login(self.user)
        response = self.client.get(
            reverse(
                "plugins:forward_netbox:forwardsync_support_bundle",
                kwargs={"pk": self.sync.pk},
            )
        )
        self.assertEqual(response.status_code, 200)
        return json.loads(response.content)

    def rows(self, url):
        import html

        text = self.client.get(url).content.decode()
        rows = {}
        for label, cell in re.findall(
            r'<th[^>]*scope="row"[^>]*>(.*?)</th>\s*<td[^>]*>(.*?)</td>', text, re.S
        ):
            label = html.unescape(re.sub(r"<[^>]+>", "", label)).strip()
            rows.setdefault(label, []).append(
                " ".join(html.unescape(re.sub(r"<[^>]+>", " ", cell)).split())
            )
        return rows

    def test_a_row_shows_what_the_bundle_holds_at_its_path(self):
        bundle = self.bundle()
        pages = {
            "Scope Reconciliation": reverse(
                "plugins:forward_netbox:forwardsync_scope_reconciliation",
                kwargs={"pk": self.sync.pk},
            ),
            "Module readiness": reverse(
                "plugins:forward_netbox:forwardsync_module_readiness",
                kwargs={"pk": self.sync.pk},
            ),
            "Dependency preview": reverse(
                "plugins:forward_netbox:forwardsync_dependency_preview",
                kwargs={"pk": self.sync.pk},
            ),
            "Health": reverse(
                "plugins:forward_netbox:forwardsync_health",
                kwargs={"pk": self.sync.pk},
            ),
            "Drift report": reverse(
                "plugins:forward_netbox:forwardsync_drift_report",
                kwargs={"pk": self.sync.pk},
            ),
            "Ingestion": reverse(
                "plugins:forward_netbox:forwardingestion",
                kwargs={"pk": self.ingestion.pk},
            ),
        }
        compared = {}
        wrong = []
        for page, url in pages.items():
            shown = self.rows(url)
            for label, value in PAGE_LABEL_PATHS[page].items():
                if isinstance(value, list) or label not in shown:
                    continue
                held = bundle
                for key in value.split("."):
                    held = held.get(key) if isinstance(held, dict) else None
                # Falsy values render as a placeholder dash, and timestamps and
                # booleans are formatted by the page; compare the rest.
                cell = shown[label][0]
                # A path that leads nowhere while the page shows a number.
                if held is None and "<" not in value and re.fullmatch(r"\d+", cell):
                    wrong.append(
                        f"{page} / {label}: shows {cell!r}, nothing at {value}"
                    )
                if (
                    not held
                    or isinstance(held, (bool, dict, list))
                    or re.match(r"\d{4}-\d\d-\d\dT", str(held))
                ):
                    continue
                compared[page] = compared.get(page, 0) + 1
                if isinstance(held, str):
                    found = held in cell
                else:
                    # A whole token, so a 4 is not found inside a 40.
                    found = str(held) in re.split(r"[\s/,()=]+", cell)
                if not found:
                    wrong.append(
                        f"{page} / {label}: page {cell!r}, bundle {held!r} at {value}"
                    )
        self.assertFalse(wrong, "\n".join(wrong))
        # A page that compared nothing proves nothing.
        for page in pages:
            self.assertGreaterEqual(compared.get(page, 0), 3, page)
        self.assertGreaterEqual(compared.get("Health", 0), 3)
