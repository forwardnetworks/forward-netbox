"""Scope is decided by name; which copy of a duplicated name is current comes
from the sync's own device map.

2.9.9 made scope membership also require the device's site to match a slug
derived from Forward's `locationName`. Sites are created from the sync's
device map instead (its `site`/`site_slug` columns, whatever variant or pinned
query the sync runs), so on a real estate that derived slug matched none of
them: every live device read as out of scope, and the next tag pass would have
labelled the whole fleet uncovered. These tests pin the replacement: scope is
name-only again, and the device map's own site answer - resolved to a NetBox
site the way the apply resolves one - is recorded, by primary key, for the
site-relabel repair.
"""

from types import SimpleNamespace
from unittest.mock import Mock
from unittest.mock import patch

from dcim.models import Device
from dcim.models import DeviceRole
from dcim.models import DeviceType
from dcim.models import Manufacturer
from dcim.models import Site
from django.test import TestCase
from extras.models import Tag

from forward_netbox.models import ForwardDeviceIdentity
from forward_netbox.models import ForwardDeviceTagClaim
from forward_netbox.models import ForwardSource
from forward_netbox.models import ForwardSync
from forward_netbox.utilities.scope_reconciliation import compute_scope_reconciliation


def _fake_fetcher(device_map_rows=None, *, error=None):
    """A `ForwardQueryFetcher` stand-in returning one `dcim.device` workload."""

    class _Fetcher:
        def __init__(self, sync, client, logger):
            pass

        def resolve_context(self):
            return SimpleNamespace(snapshot_id="snap-1")

        def fetch_workloads(self, context, **kwargs):
            if error is not None:
                raise error
            assert kwargs["model_strings"] == ["dcim.device"]
            return [SimpleNamespace(upsert_rows=list(device_map_rows or ()))]

    return _Fetcher


class ScopeReconciliationSiteAwareTest(TestCase):
    def setUp(self):
        # The customer's shape: NetBox site slugs that are NOT the Django slugify of
        # the Forward location - the NQE slugify turns `_`, `.` and `&` into
        # `-`/`and`, Django keeps or drops them.
        self.old_site = Site.objects.create(name="atl_colo.1", slug="atl-colo-1")
        self.new_site = Site.objects.create(name="dfw & dc2", slug="dfw-and-dc2")
        mfr = Manufacturer.objects.create(name="MfrS", slug="mfr-s")
        self.dt = DeviceType.objects.create(manufacturer=mfr, model="dt-s", slug="dt-s")
        self.role = DeviceRole.objects.create(name="RoleS", slug="role-s")
        self.source = ForwardSource.objects.create(
            name="site-aware-source",
            type="saas",
            url="https://fwd.app",
            status="ready",
            parameters={
                "network_id": "net-1",
                "device_tag_include_tags": ["Prod_Core"],
                "device_tag_include_match": "any",
                "device_tag_prune_absence_runs": 0,
                "device_tag_prune_absence_hours": 0,
            },
        )
        self.sync = ForwardSync.objects.create(
            name="site-aware-sync",
            source=self.source,
            parameters={"snapshot_id": "latestProcessed"},
        )
        self.tag = Tag.objects.create(name="Prod_Core", slug="prod-core")

    def _device(self, name, site):
        return Device.objects.create(
            name=name, site=site, role=self.role, device_type=self.dt
        )

    def _claim(self, device):
        return ForwardDeviceTagClaim.objects.create(
            sync=self.sync, device=device, tag=self.tag, claim_type="scope"
        )

    def _scope_row(self, name, location):
        return {"name": name, "completed": True, "location": location}

    def _map_row(self, name, site):
        return {"name": name, "site": site.name, "site_slug": site.slug}

    def _report(self, scope_rows, device_map_rows=(), *, fetch_error=None, census=None):
        client = Mock()
        client.run_nqe_query = Mock(return_value=scope_rows)
        patches = [
            patch.object(ForwardSource, "get_client", return_value=client),
            patch.object(ForwardSync, "resolve_snapshot_id", return_value="snap-1"),
            patch(
                "forward_netbox.utilities.query_fetch.ForwardQueryFetcher",
                _fake_fetcher(device_map_rows, error=fetch_error),
            ),
        ]
        if census is not None:
            patches.append(
                patch(
                    "forward_netbox.utilities.scope_reconciliation._absence_census",
                    return_value=census,
                )
            )
        for patcher in patches:
            patcher.start()
            self.addCleanup(patcher.stop)
        return compute_scope_reconciliation(self.sync)

    # -- the 2.9.9 regression ---------------------------------------------

    def test_a_site_slug_that_differs_from_the_location_keeps_the_device_in_scope(self):
        # 2.9.9 compared slugify("atl_colo.1") == "atl_colo1" with the NetBox
        # slug "atl-colo-1" and called the live device out of scope. Every
        # device on an estate like this read as out of scope.
        devices = [self._device(f"sw-{i}", self.old_site) for i in range(3)]
        for device in devices:
            self._claim(device)
            ForwardDeviceIdentity.objects.create(
                sync=self.sync, source_device_key=device.name, device=device
            )
        report = self._report(
            [self._scope_row(device.name, "atl_colo.1") for device in devices],
            [self._map_row(device.name, self.old_site) for device in devices],
        )

        self.assertEqual(report["netbox_out_of_scope"], 0)
        self.assertEqual(report["_out_of_scope_pks"], [])
        self.assertEqual(report["unmanaged"]["owned_untagged"], 0)

    def test_scope_ignores_site_so_neither_copy_of_a_pair_is_out_of_scope(self):
        old = self._device("core-sw-01", self.old_site)
        new = self._device("core-sw-01", self.new_site)
        self._claim(old)
        self._claim(new)
        report = self._report(
            [self._scope_row("core-sw-01", "dfw & dc2")],
            [self._map_row("core-sw-01", self.new_site)],
        )

        # Neither copy is tagged uncovered or out of scope: which one is stale
        # is the site-relabel repair's question, not the prune's.
        self.assertEqual(report["_out_of_scope_pks"], [])
        self.assertEqual(
            report["forward_site_id_by_device_pk"],
            {str(old.pk): self.new_site.pk, str(new.pk): self.new_site.pk},
        )

    # -- Forward's site answer, from the device map -------------------------

    def test_the_device_map_site_resolves_by_slug_then_by_name(self):
        old = self._device("core-sw-01", self.old_site)
        self._device("core-sw-01", self.new_site)
        other_old = self._device("edge-01", self.old_site)
        self._device("edge-01", self.new_site)
        report = self._report(
            [self._scope_row("core-sw-01", ""), self._scope_row("edge-01", "")],
            [
                # Slug hit.
                {"name": "core-sw-01", "site": "ignored", "site_slug": "dfw-and-dc2"},
                # No slug match, falls back to the site name - as the apply does.
                {"name": "edge-01", "site": "atl_colo.1", "site_slug": "not-a-slug"},
            ],
        )

        self.assertEqual(
            report["forward_site_id_by_device_pk"][str(old.pk)], self.new_site.pk
        )
        self.assertEqual(
            report["forward_site_id_by_device_pk"][str(other_old.pk)], self.old_site.pk
        )

    def test_names_are_matched_case_insensitively(self):
        old = self._device("CORE-SW-01", self.old_site)
        self._device("core-sw-01", self.new_site)
        report = self._report(
            [self._scope_row("core-sw-01", "")],
            [self._map_row("Core-Sw-01", self.new_site)],
        )

        self.assertEqual(
            report["forward_site_id_by_device_pk"][str(old.pk)], self.new_site.pk
        )

    def test_only_duplicated_names_are_recorded(self):
        self._device("solo-01", self.old_site)
        old = self._device("dup-01", self.old_site)
        new = self._device("dup-01", self.new_site)
        report = self._report(
            [self._scope_row("solo-01", ""), self._scope_row("dup-01", "")],
            [
                self._map_row("solo-01", self.old_site),
                self._map_row("dup-01", self.new_site),
            ],
        )

        self.assertEqual(
            set(report["forward_site_id_by_device_pk"]), {str(old.pk), str(new.pk)}
        )

    def test_an_estate_without_duplicates_never_fetches_the_device_map(self):
        self._device("solo-01", self.old_site)
        report = self._report(
            [self._scope_row("solo-01", "")], fetch_error=AssertionError("fetched")
        )

        self.assertEqual(
            report["forward_site_source"]["error"], "no_duplicated_device_names"
        )

    def test_a_name_the_map_places_at_two_sites_is_ambiguous_not_guessed(self):
        old = self._device("core-sw-01", self.old_site)
        new = self._device("core-sw-01", self.new_site)
        report = self._report(
            [self._scope_row("core-sw-01", "")],
            [
                self._map_row("core-sw-01", self.old_site),
                self._map_row("core-sw-01", self.new_site),
            ],
        )

        self.assertEqual(report["forward_site_id_by_device_pk"], {})
        self.assertEqual(
            report["forward_site_ambiguous_device_ids"], sorted([old.pk, new.pk])
        )

    def test_a_failed_device_map_fetch_leaves_scope_untouched_and_says_so(self):
        device = self._device("sw-1", self.old_site)
        self._claim(device)
        self._device("sw-1", self.new_site)
        report = self._report(
            [self._scope_row("sw-1", "")], fetch_error=RuntimeError("403")
        )

        self.assertEqual(report["_out_of_scope_pks"], [])
        self.assertEqual(report["forward_site_id_by_device_pk"], {})
        self.assertEqual(
            report["forward_site_source"],
            {"available": False, "error": "RuntimeError", "rows": 0},
        )

    def test_the_source_reports_row_count_when_available(self):
        self._device("sw-1", self.old_site)
        self._device("sw-1", self.new_site)
        report = self._report(
            [self._scope_row("sw-1", "")], [self._map_row("sw-1", self.old_site)]
        )

        self.assertEqual(
            report["forward_site_source"], {"available": True, "error": "", "rows": 1}
        )

    # -- empty sites come from the same answer -----------------------------

    def test_empty_orphan_sites_use_the_device_map_sites_not_derived_slugs(self):
        empty = Site.objects.create(name="empty-site", slug="empty-site")
        # Mapped by the device map, empty in NetBox, and not what slugify of
        # the Forward location would produce: still a live site.
        mapped = Site.objects.create(name="mapped_site.2", slug="mapped-site-2")
        self._device("sw-1", self.old_site)
        self._device("sw-1", self.new_site)
        report = self._report(
            [self._scope_row("sw-1", "mapped_site.2")],
            [self._map_row("sw-1", mapped)],
        )

        self.assertIn(empty.name, report["empty_orphan_site_sample"])
        self.assertNotIn(mapped.name, report["empty_orphan_site_sample"])

    # -- the prune never picks a side --------------------------------------

    def test_an_unmerged_pair_is_excluded_from_the_orphan_prune(self):
        from forward_netbox.utilities.scope_reconciliation import (
            prune_orphan_devices,
        )

        # Forward no longer tags the name at all, so both copies are orphans -
        # but they are a duplicated name, and which one to keep is the
        # operator's call through the merge, not the prune's.
        old = self._device("core-sw-01", self.old_site)
        new = self._device("core-sw-01", self.new_site)
        self._claim(old)
        self._claim(new)
        report = self._report(
            [self._scope_row("other", "")],
            [],
            census=({"core-sw-01": "absent"}, {}),
        )
        prune_orphan_devices(self.sync, report=report)

        self.assertTrue(Device.objects.filter(pk=old.pk).exists())
        self.assertTrue(Device.objects.filter(pk=new.pk).exists())
