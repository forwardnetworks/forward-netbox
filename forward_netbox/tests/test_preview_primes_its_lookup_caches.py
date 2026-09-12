"""The preview must resolve parents in bulk, as the apply already does.

A deployment's drift report priced its own comparison for the first time:
1,452,552 ms for 558,380 rows, of which `dcim.interface` alone was 842,445 ms
across 357,864 rows - 58% of a 24-minute preview. The estate it was measuring
had 387 rows of real interface drift, so essentially every one of those rows
was unchanged.

The comparison was not slow because comparing is expensive. `apply_model_rows`
calls `prime_dependency_lookup_caches` before handing rows to the
classification, so the real apply reads every parent device, interface and tag
in a handful of bulk queries. `compare_model_rows` called the classification
directly and skipped that step, so the identical code then resolved each parent
one row at a time against caches that started empty and only ever filled from
their own misses.

Two properties have to hold together, and only one is about speed:

  - the counts must not move. A comparison that is cheaper but answers
    differently is worse than the slow one, because the drift figure is the
    product and an operator acts on it.
  - a row that matches something already in NetBox must cost no queries at all.
    That is the shape of a converged estate, which is the shape every routine
    preview has.

What was deliberately NOT claimed here was the create path, on the reasoning
that a row with no counterpart is instantiated, that NetBox charges two queries
per instance for content type and custom field defaults, and that the only way
to avoid them is to stop routing the preview through the real apply.

**The middle step of that was wrong, and measurement says so.** The per-row
charge came from `_validate_interface` - `full_clean` - not from
`Interface(**defaults)`. The preview now skips the validation and still
instantiates, still resolves, still classifies through the same code, and the
query count went from 9,000 to 36 across 16,000 first-sync rows (29,139 ms to
1,467 ms). Instantiation is cheap; validating is not.

So the create path is no longer charged per row, and that is NOT the divergence
this module warned about - the preview still routes through the real apply. One
narrower divergence was bought knowingly: a row `full_clean` would reject counts
as a create rather than failed, which overstates drift and never understates it,
pinned by `test_an_invalid_interface_row_counts_as_a_create_under_preview` in
`test_drift_comparison`.

The canary below is kept, with its assertion moved off query count - which no
longer varies - and onto the property it was really protecting: that each row
is still classified individually.
"""

from dcim.models import Device
from dcim.models import DeviceRole
from dcim.models import DeviceType
from dcim.models import Interface
from dcim.models import Manufacturer
from dcim.models import Site
from django.db import connection
from django.test import TestCase
from django.test.utils import CaptureQueriesContext
from ipam.models import VLAN

from forward_netbox.utilities import sync_primitives
from forward_netbox.utilities.drift_comparison import compare_model_rows


class PreviewPrimesItsLookupCachesTest(TestCase):
    """Same answers, in bulk."""

    def setUp(self):
        site = Site.objects.create(name="P Site", slug="p-site")
        mfr = Manufacturer.objects.create(name="P Mfr", slug="p-mfr")
        dtype = DeviceType.objects.create(manufacturer=mfr, model="P DT", slug="p-dt")
        role = DeviceRole.objects.create(name="P Role", slug="p-role")
        self.devices = [
            Device.objects.create(
                name=f"prime-dev-{index}", site=site, device_type=dtype, role=role
            )
            for index in range(3)
        ]
        for device in self.devices:
            for port in range(8):
                Interface.objects.create(
                    device=device, name=f"Ethernet{port}", type="1000base-t"
                )

    def _rows(self, names):
        return [
            {
                "device": device.name,
                "name": name,
                "type": "1000base-t",
                "enabled": True,
            }
            for device in self.devices
            for name in names
        ]

    def _existing(self, count):
        return self._rows([f"Ethernet{port}" for port in range(count)])

    def _absent(self, count):
        return self._rows([f"NewPort{port}" for port in range(count)])

    def _compare_without_priming(self, model_string, rows):
        """Run the comparison as it behaved before priming was added."""
        real = sync_primitives.prime_dependency_lookup_caches
        sync_primitives.prime_dependency_lookup_caches = (
            lambda runner, model, rows: None
        )
        try:
            return compare_model_rows(None, model_string, rows)
        finally:
            sync_primitives.prime_dependency_lookup_caches = real

    def test_priming_does_not_change_the_counts(self):
        rows = self._existing(4) + self._absent(2)

        primed = compare_model_rows(None, "dcim.interface", rows)
        cold = self._compare_without_priming("dcim.interface", rows)

        self.assertEqual(
            primed,
            cold,
            "priming is a cost change only; the comparison must return the "
            "same creates/updates/unchanged/rejected either way",
        )
        # And the counts are the ones the fixture describes, so a comparison
        # that returned zeros consistently could not pass by agreeing with
        # itself.
        self.assertEqual(primed["unchanged"], 12)
        self.assertEqual(primed["creates"], 6)
        self.assertEqual(primed["updates"], 0)

    def test_matching_rows_cost_a_fixed_number_of_queries(self):
        few = self._existing(2)
        many = self._existing(8)
        self.assertEqual(len(many), len(few) * 4)

        with CaptureQueriesContext(connection) as few_queries:
            compare_model_rows(None, "dcim.interface", few)
        with CaptureQueriesContext(connection) as many_queries:
            compare_model_rows(None, "dcim.interface", many)

        self.assertEqual(
            len(many_queries),
            len(few_queries),
            "rows that match an existing object must be classified from the "
            "primed caches alone, so quadrupling them must not cost one extra "
            "query - this is the converged estate every routine preview reads",
        )

    def test_priming_is_what_removes_the_per_row_queries(self):
        rows = self._existing(8)

        with CaptureQueriesContext(connection) as primed_queries:
            compare_model_rows(None, "dcim.interface", rows)
        with CaptureQueriesContext(connection) as cold_queries:
            self._compare_without_priming("dcim.interface", rows)

        self.assertLess(
            len(primed_queries),
            len(cold_queries),
            "the primed comparison must issue strictly fewer queries than the "
            "cold one it replaces",
        )

    def test_the_create_path_still_classifies_every_row(self):
        """The canary this module has always carried, re-aimed.

        It used to assert that the create path costs MORE QUERIES for more
        rows, as proof the preview was still going through the real apply. That
        proxy died when the preview stopped validating: queries are flat now
        (36 for 16,000 rows) while the classification is unchanged. Asserting
        the dead proxy would have forced the cost back to keep a test happy.

        What it was really protecting is that each row is still put through the
        classification rather than short-circuited in bulk - so that is what it
        asserts now, on the counts themselves, which is also the thing an
        operator reads.
        """
        few = self._absent(2)
        many = self._absent(8)

        few_result = compare_model_rows(None, "dcim.interface", few)
        many_result = compare_model_rows(None, "dcim.interface", many)

        # Counted against the row lists themselves rather than hard-coded, so
        # the assertion survives `_absent` changing how many devices it spans.
        self.assertEqual(few_result["creates"], len(few))
        self.assertEqual(many_result["creates"], len(many))
        self.assertEqual(few_result["unchanged"], 0)
        self.assertEqual(many_result["unchanged"], 0)
        self.assertGreater(len(many), len(few))


class SiteBearingModelsArePrimedTest(TestCase):
    """A converged `ipam.vlan` comparison must not cost a query per row.

    A deployment's drift page priced it: 102,334 queries and 239,755 ms for
    7,902 `ipam.vlan` rows across ~95 sites - 31% of the whole comparison's
    766 seconds - on a model the same page reported as In sync: Yes. Thirteen
    queries per row to confirm nothing had changed.

    The cause was not the priming 2.8.7 added (this path does its own bulk
    fetch of existing rows). It was that fetch loading each VLAN WITHOUT its
    site while the lookup key then read `vlan.site` - one query per existing
    row, per lookup field. The fetch now joins the FKs its keys read.

    Both properties, as in the class above: the counts must not move, and a row
    matching something already in NetBox must cost no queries of its own.
    """

    def setUp(self):
        self.sites = [
            Site.objects.create(name=f"V Site {index}", slug=f"v-site-{index}")
            for index in range(4)
        ]
        for site in self.sites:
            for vid in range(1, 6):
                VLAN.objects.create(vid=vid, name=f"vlan-{vid}", site=site)

    def _rows(self, vids):
        return [
            {
                "vid": vid,
                "name": f"vlan-{vid}",
                "status": "active",
                "site": site.name,
                "site_slug": site.slug,
            }
            for site in self.sites
            for vid in vids
        ]

    def _compare_without_priming(self, rows):
        real = sync_primitives.prime_dependency_lookup_caches
        sync_primitives.prime_dependency_lookup_caches = (
            lambda runner, model, rows: None
        )
        try:
            return compare_model_rows(None, "ipam.vlan", rows)
        finally:
            sync_primitives.prime_dependency_lookup_caches = real

    def test_priming_does_not_change_the_counts(self):
        rows = self._rows([1, 2, 3]) + self._rows([90, 91])
        primed = compare_model_rows(None, "ipam.vlan", rows)
        cold = self._compare_without_priming(rows)
        self.assertEqual(
            primed,
            cold,
            "priming changed the comparison's answer, which is worse than it "
            "being slow - the drift figure is what an operator acts on",
        )

    def test_matching_rows_do_not_scale_the_query_count(self):
        # The property that matters: doubling converged rows must not double
        # the queries. Asserting a flat count rather than a wall-clock number,
        # because the count is what the page reported and what regressed.
        few = self._rows([1])
        many = self._rows([1, 2, 3, 4, 5])

        with CaptureQueriesContext(connection) as few_queries:
            compare_model_rows(None, "ipam.vlan", few)
        with CaptureQueriesContext(connection) as many_queries:
            compare_model_rows(None, "ipam.vlan", many)

        self.assertEqual(
            len(few_queries),
            len(many_queries),
            "five times the converged rows cost more queries, so the site "
            f"lookup is still per-row ({len(few_queries)} -> "
            f"{len(many_queries)} for 4 -> 20 rows)",
        )

    def test_no_existing_row_reloads_its_site(self):
        # Pins the cause, not just the symptom: the per-row cost was a
        # `dcim_site ... WHERE id = N` for every existing VLAN, issued when the
        # lookup key read `vlan.site` on an object fetched without it.
        rows = self._rows([1, 2, 3, 4, 5])

        with CaptureQueriesContext(connection) as queries:
            compare_model_rows(None, "ipam.vlan", rows)

        by_id = [
            q["sql"]
            for q in queries.captured_queries
            if '"dcim_site"."id" =' in q["sql"]
        ]
        self.assertEqual(by_id, [], "an existing VLAN reloaded its site per row")
