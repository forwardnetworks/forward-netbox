"""Site-relabel duplicates are never silent.

A customer upgraded, and the repair for their 221 duplicate pairs sat on one
page, in a card that rendered only when a pair was mergeable - every pair was
held, so nothing anywhere said so, and three blocked primary-IP issues named
the stale copies without saying what they were. These pin every place the
backlog now surfaces: the Health tab, the sync page, the Scope Reconciliation
card, the blocked-IP issue and its page, and the post-sync job log.
"""

import uuid
from unittest.mock import Mock
from unittest.mock import patch

from core.choices import JobStatusChoices
from core.models import Job
from dcim.models import Device
from dcim.models import DeviceRole
from dcim.models import DeviceType
from dcim.models import Manufacturer
from dcim.models import Site
from django.contrib.auth import get_user_model
from django.contrib.contenttypes.models import ContentType
from django.test import TestCase
from django.urls import reverse
from django.utils import timezone

from forward_netbox.models import ForwardIngestion
from forward_netbox.models import ForwardIngestionIssue
from forward_netbox.models import ForwardSource
from forward_netbox.models import ForwardSync
from forward_netbox.utilities.health import _site_relabel_duplicates_check
from forward_netbox.utilities.sync_ipam import _site_relabel_partners
from forward_netbox.utilities.sync_ipam import record_unowned_primary_ip_holder_skip


class SiteRelabelPromptFixture(TestCase):
    def setUp(self):
        self.old_site = Site.objects.create(name="old-site", slug="old-site")
        self.new_site = Site.objects.create(name="new-site", slug="new-site")
        manufacturer = Manufacturer.objects.create(name="MfrP", slug="mfr-p")
        self.device_type = DeviceType.objects.create(
            manufacturer=manufacturer, model="dt-p", slug="dt-p"
        )
        self.role = DeviceRole.objects.create(name="RoleP", slug="role-p")
        self.source = ForwardSource.objects.create(
            name="prompt-src",
            type="saas",
            url="https://fwd.app",
            status="ready",
            parameters={"network_id": "net-1"},
        )
        self.sync = ForwardSync.objects.create(
            name="prompt-sync",
            source=self.source,
            parameters={"snapshot_id": "latestProcessed"},
        )

    def _device(self, name, site):
        return Device.objects.create(
            name=name, site=site, role=self.role, device_type=self.device_type
        )

    def _pair(self, name="core-sw-01"):
        return self._device(name, self.old_site), self._device(name, self.new_site)

    def _report(self, forward_sites):
        return Job.objects.create(
            object_type=ContentType.objects.get_for_model(ForwardSync),
            object_id=self.sync.pk,
            name=f"{self.sync.name} - scope reconciliation",
            status=JobStatusChoices.STATUS_COMPLETED,
            completed=timezone.now(),
            job_id=uuid.uuid4(),
            data={
                "forward_site_id_by_device_pk": {
                    str(pk): site for pk, site in forward_sites.items()
                },
                "forward_site_ambiguous_device_ids": [],
            },
        )

    def _superuser_client(self):
        user = get_user_model().objects.create_user(username="prompt-admin")
        user.is_superuser = True
        user.is_staff = True
        user.save()
        self.client.force_login(user)
        return self.client


class HealthCheckTest(SiteRelabelPromptFixture):
    def test_no_duplicates_means_no_check(self):
        self._device("solo-01", self.old_site)
        self.assertIsNone(_site_relabel_duplicates_check(self.sync))

    def test_mergeable_pairs_warn_and_link_to_the_repair(self):
        older, newer = self._pair()
        self._report({older.pk: self.new_site.pk, newer.pk: self.new_site.pk})

        check = _site_relabel_duplicates_check(self.sync)

        self.assertEqual(check["status"], "warn")
        self.assertIn("1 duplicate device pair(s)", check["message"])
        self.assertIn("Merge site-relabel duplicates", check["message"])
        self.assertEqual(
            check["url"],
            reverse(
                "plugins:forward_netbox:forwardsync_scope_reconciliation",
                kwargs={"pk": self.sync.pk},
            ),
        )

    def test_held_pairs_say_why_and_what_to_do(self):
        # No scope report yet: every pair is held, which used to show nothing.
        self._pair()

        check = _site_relabel_duplicates_check(self.sync)

        self.assertEqual(check["status"], "warn")
        self.assertIn("1 held because", check["message"])
        self.assertIn("refresh Scope Reconciliation", check["message"])

    def test_the_check_skips_the_per_pair_protecting_scan(self):
        older, newer = self._pair()
        self._report({older.pk: self.new_site.pk, newer.pk: self.new_site.pk})

        with patch(
            "forward_netbox.utilities.bulk_merge.describe_protecting_references"
        ) as scan:
            _site_relabel_duplicates_check(self.sync)

        scan.assert_not_called()


class PagesTest(SiteRelabelPromptFixture):
    def test_the_sync_page_shows_the_backlog_with_a_link(self):
        older, newer = self._pair()
        self._report({older.pk: self.new_site.pk, newer.pk: self.new_site.pk})
        client = self._superuser_client()

        response = client.get(
            reverse("plugins:forward_netbox:forwardsync", args=[self.sync.pk])
        )

        self.assertEqual(response.status_code, 200)
        self.assertContains(response, "Site-relabel duplicates:")
        self.assertContains(response, "Open Scope Reconciliation")

    def test_the_scope_page_card_renders_when_every_pair_is_held(self):
        self._pair()
        client = self._superuser_client()

        response = client.get(
            reverse(
                "plugins:forward_netbox:forwardsync_scope_reconciliation",
                kwargs={"pk": self.sync.pk},
            )
        )

        self.assertEqual(response.status_code, 200)
        self.assertContains(response, 'id="site-relabel-duplicates"')
        self.assertContains(response, "cannot be proven safe to merge yet")
        self.assertContains(response, "refresh Scope Reconciliation")
        # Nothing is mergeable, so no merge button.
        self.assertNotContains(
            response,
            reverse(
                "plugins:forward_netbox:forwardsync_merge_site_relabel_duplicates",
                kwargs={"pk": self.sync.pk},
            ),
        )

    def test_the_issue_page_links_to_the_repair(self):
        ingestion = ForwardIngestion.objects.create(sync=self.sync)
        issue = ForwardIngestionIssue.objects.create(
            ingestion=ingestion,
            model="ipam.ipaddress",
            message=(
                "IP address #1 was not moved to device #3: ... Device #2 is a "
                "site-relabel duplicate of #3: Scope Reconciliation -> Merge "
                "site-relabel duplicates keeps the older device."
            ),
        )
        client = self._superuser_client()

        response = client.get(
            reverse("plugins:forward_netbox:forwardingestionissue", args=[issue.pk])
        )

        self.assertEqual(response.status_code, 200)
        self.assertContains(response, "Open Scope Reconciliation to merge")


class BlockedPrimaryIpHintTest(SiteRelabelPromptFixture):
    def _message(self, runner, holder_pks, destination):
        with patch("forward_netbox.utilities.sync_ipam.record_issue") as record:
            record_unowned_primary_ip_holder_skip(
                runner,
                ip_pk=99,
                holder_pks=holder_pks,
                destination_device_pk=destination,
            )
        return record.call_args.args[2]

    def test_a_holder_that_is_a_duplicate_names_its_partner_and_the_repair(self):
        older, newer = self._pair()
        runner = Mock(sync=self.sync)

        message = self._message(runner, [older.pk], newer.pk)

        self.assertIn(
            f"Device #{older.pk} is a site-relabel duplicate of #{newer.pk}", message
        )
        self.assertIn("Merge site-relabel duplicates", message)

    def test_an_ordinary_holder_gets_no_hint(self):
        holder = self._device("solo-01", self.old_site)
        other = self._device("other-01", self.new_site)

        message = self._message(Mock(sync=self.sync), [holder.pk], other.pk)

        self.assertNotIn("site-relabel", message)

    def test_the_lookup_runs_once_per_run(self):
        self._pair()
        runner = Mock(sync=self.sync)
        with patch(
            "forward_netbox.utilities.scope_reconciliation.site_relabel_pairs",
            wraps=__import__(
                "forward_netbox.utilities.scope_reconciliation",
                fromlist=["site_relabel_pairs"],
            ).site_relabel_pairs,
        ) as lookup:
            _site_relabel_partners(runner)
            _site_relabel_partners(runner)

        self.assertEqual(lookup.call_count, 1)

    def test_a_runner_without_a_real_sync_gets_no_query(self):
        with patch(
            "forward_netbox.utilities.scope_reconciliation.site_relabel_pairs"
        ) as lookup:
            self.assertEqual(_site_relabel_partners(Mock()), {})
            self.assertEqual(_site_relabel_partners(Mock(sync=Mock(pk=None))), {})

        lookup.assert_not_called()


class PostSyncLogTest(SiteRelabelPromptFixture):
    def test_the_tag_pass_logs_the_backlog(self):
        from forward_netbox.jobs import _log_site_relabel_backlog

        self._pair()
        job = Mock()

        _log_site_relabel_backlog(job, self.sync)

        record = job.log.call_args.args[0]
        self.assertEqual(record.levelname, "WARNING")
        self.assertIn("0 site-relabel duplicate device pair(s)", record.getMessage())
        self.assertIn("1 are held", record.getMessage())

    def test_no_backlog_logs_nothing(self):
        from forward_netbox.jobs import _log_site_relabel_backlog

        job = Mock()

        _log_site_relabel_backlog(job, self.sync)

        job.log.assert_not_called()
