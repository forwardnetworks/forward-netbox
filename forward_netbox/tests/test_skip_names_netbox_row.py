"""A protected-delete skip must name the NetBox row it is about.

A customer's 2.8.1 sync recorded five dependency skips:

    ipam.vrf         row processing skipped (...; still referenced by
                     ipam.ipaddress, ipam.prefix, netbox_routing.bgpscope).
    dcim.devicetype  row processing skipped (...; still referenced by dcim.device).
    dcim.site        row processing skipped (...; still referenced by dcim.device).
    dcim.site        row processing skipped (...; still referenced by dcim.device).
    dcim.site        row processing skipped (...; still referenced by dcim.device, ipam.vlan).

Everything about those sentences is correct and none of them is actionable. The
model is named, the direction is named, and the one fact needed to go look —
*which* site — is missing, because every value that would say so is a name or a
slug and `diagnostic_shape` reduces it to its key names before it persists. Two
of the three site rows are byte-identical to each other, so the panel cannot
even confirm they are different sites.

The merge recorder already solved this: it appends `Affected NetBox row: pk N.`
A pk is an internal identifier, not customer data, so it survives redaction.
This is that fix on the sync recorder.
"""

from types import SimpleNamespace

from django.db.models.deletion import ProtectedError
from django.test import SimpleTestCase
from django.test import TestCase

from forward_netbox.exceptions import ForwardDependencySkipError
from forward_netbox.utilities.sync_reporting import record_issue


class SkipCarriesTheNetboxPkTest(SimpleTestCase):
    def test_the_exception_carries_the_pk_it_was_given(self):
        exc = ForwardDependencySkipError("m", model_string="dcim.site", netbox_pk=42)
        self.assertEqual("42", exc.netbox_pk)

    def test_a_raiser_that_names_no_row_carries_none(self):
        # Most raisers have no object in hand — a missing parent is an absence.
        self.assertIsNone(ForwardDependencySkipError("m").netbox_pk)

    def test_a_uuid_pk_survives_as_text(self):
        # Not every NetBox pk is an integer, and nothing downstream does
        # arithmetic on it.
        exc = ForwardDependencySkipError(
            "m", netbox_pk="6f1c9a3e-0000-4000-8000-000000000001"
        )
        self.assertEqual("6f1c9a3e-0000-4000-8000-000000000001", exc.netbox_pk)

    def test_a_blank_pk_is_not_a_pk(self):
        # `str("").strip() or None` — a blank must not print as `pk `.
        self.assertIsNone(ForwardDependencySkipError("m", netbox_pk="  ").netbox_pk)

    def test_pk_zero_is_reported_not_swallowed(self):
        # Guarding on falsiness rather than `is None` is how a real identifier
        # disappears; 0 is a legal pk.
        self.assertEqual("0", ForwardDependencySkipError("m", netbox_pk=0).netbox_pk)


class _Recorded(TestCase):
    """Shared harness: record an issue and hand back the persisted row."""

    def _runner(self, ingestion):
        return SimpleNamespace(
            ingestion=ingestion,
            logger=SimpleNamespace(
                log_info=lambda *a, **k: None,
                log_failure=lambda *a, **k: None,
                log_warning=lambda *a, **k: None,
            ),
            _recorded_issue_ids=set(),
            _dependency_skip_issue_counts={},
            _dependency_skip_issue_samples={},
            DEPENDENCY_SKIP_ISSUE_DETAIL_LIMIT=10,
        )

    def _ingestion(self):
        from forward_netbox.models import ForwardIngestion
        from forward_netbox.models import ForwardSource
        from forward_netbox.models import ForwardSync

        source = ForwardSource.objects.create(
            name="skip-identity-source", url="https://fwd.example.invalid"
        )
        sync = ForwardSync.objects.create(name="skip-identity-sync", source=source)
        return ForwardIngestion.objects.create(sync=sync)

    def _skip(self, runner, model_string, *, netbox_pk, dependency, context=None):
        return record_issue(
            runner,
            model_string,
            "ignored",
            {"slug": "a-slug"},
            exception=ForwardDependencySkipError(
                f"Skipping delete for `{model_string}`.",
                model_string=model_string,
                dependency=dependency,
                dependency_is_protecting=True,
                netbox_pk=netbox_pk,
            ),
            context=context or {"slug": "a-slug"},
            log_level="info",
        )


class SkipMessageNamesTheRowTest(_Recorded):
    def test_the_message_names_the_blocked_row(self):
        runner = self._runner(self._ingestion())
        issue = self._skip(runner, "dcim.site", netbox_pk=884, dependency="dcim.device")
        self.assertEqual(
            "dcim.site row processing skipped (ForwardDependencySkipError; "
            "still referenced by dcim.device). Affected NetBox row: pk 884.",
            issue.message,
        )

    def test_the_pk_is_also_stored_structurally(self):
        # So an operator reading issues over the API does not have to parse
        # English out of `message`.
        runner = self._runner(self._ingestion())
        issue = self._skip(runner, "ipam.vrf", netbox_pk=17, dependency="ipam.prefix")
        self.assertEqual("17", issue.raw_data["netbox_pk"])

    def test_the_row_values_are_still_never_recorded(self):
        # The whole reason the pk is needed: the slug that would identify the
        # site is redacted, and must stay redacted.
        runner = self._runner(self._ingestion())
        issue = self._skip(
            runner,
            "dcim.site",
            netbox_pk=884,
            dependency="dcim.device",
            context={"slug": "example-site-slug"},
        )
        persisted = f"{issue.message} {issue.coalesce_fields} {issue.raw_data}"
        self.assertNotIn("example-site-slug", persisted)
        self.assertIn("884", issue.message)

    def test_two_blocked_sites_no_longer_read_identically(self):
        # The customer's exact complaint: two of three `dcim.site` rows were
        # byte-identical, so the panel could not say they were different sites.
        runner = self._runner(self._ingestion())
        first = self._skip(runner, "dcim.site", netbox_pk=884, dependency="dcim.device")
        second = self._skip(
            runner, "dcim.site", netbox_pk=885, dependency="dcim.device"
        )
        self.assertNotEqual(first.message, second.message)
        self.assertNotEqual(first.pk, second.pk)

    def test_a_skip_with_no_row_in_hand_is_unchanged(self):
        # The missing-parent case is most of the raisers and must keep the
        # sentence it always had, byte for byte.
        runner = self._runner(self._ingestion())
        issue = record_issue(
            runner,
            "netbox_dlm.softwareversion",
            "ignored",
            {"device": "x"},
            exception=ForwardDependencySkipError(
                "Skipping.",
                model_string="netbox_dlm.softwareversion",
                dependency="dcim.device",
            ),
            log_level="info",
        )
        self.assertEqual(
            "netbox_dlm.softwareversion row processing skipped "
            "(ForwardDependencySkipError; waiting on dcim.device).",
            issue.message,
        )
        self.assertNotIn("netbox_pk", issue.raw_data)

    def test_a_non_skip_failure_with_no_pk_is_unchanged(self):
        runner = self._runner(self._ingestion())
        issue = record_issue(
            runner,
            "dcim.module",
            "ignored",
            {"device": "x"},
            exception=ValueError("boom"),
        )
        self.assertEqual(
            "dcim.module row processing failed (ValueError).", issue.message
        )


class DeleteByCoalesceNamesTheRowTest(TestCase):
    """The raiser itself must supply the pk; the recorder cannot invent one."""

    def test_a_protected_delete_reports_the_object_it_could_not_delete(self):
        from dcim.models import Site

        from forward_netbox.utilities import sync_primitives

        site = Site.objects.create(name="Blocked Site", slug="blocked-site")

        class _Blocked:
            pk = site.pk

            def delete(self):
                raise ProtectedError("protected", [site])

        runner = SimpleNamespace()
        blocked = _Blocked()

        with self.assertRaises(ForwardDependencySkipError) as caught:
            with _patched_get_unique(sync_primitives, blocked):
                sync_primitives.delete_by_coalesce(
                    runner, Site, [{"slug": "blocked-site"}]
                )

        self.assertEqual(str(site.pk), caught.exception.netbox_pk)
        self.assertTrue(caught.exception.dependency_is_protecting)
        self.assertIn("dcim.site", caught.exception.dependency)

    def test_an_object_without_a_pk_still_raises_the_skip(self):
        # A diagnostic on an error path must never replace the error it is
        # describing. Reading `obj.pk` directly turned the whole skip into an
        # AttributeError for any object that has none.
        from dcim.models import Site

        from forward_netbox.utilities import sync_primitives

        class _NoPk:
            def delete(self):
                raise ProtectedError("protected", set())

        runner = SimpleNamespace()

        with self.assertRaises(ForwardDependencySkipError) as caught:
            with _patched_get_unique(sync_primitives, _NoPk()):
                sync_primitives.delete_by_coalesce(runner, Site, [{"slug": "x"}])

        self.assertIsNone(caught.exception.netbox_pk)
        self.assertTrue(caught.exception.dependency_is_protecting)

    def test_a_successful_delete_still_returns_true(self):
        from dcim.models import Site

        from forward_netbox.utilities import sync_primitives

        deleted = []

        class _Deletable:
            pk = 5

            def delete(self):
                deleted.append(True)

        runner = SimpleNamespace()
        with _patched_get_unique(sync_primitives, _Deletable()):
            with _patched_forget(sync_primitives):
                result = sync_primitives.delete_by_coalesce(
                    runner, Site, [{"slug": "gone"}]
                )

        self.assertTrue(result)
        self.assertEqual([True], deleted)


class _patched_get_unique:
    def __init__(self, module, obj):
        self._module = module
        self._obj = obj

    def __enter__(self):
        self._original = self._module.get_unique_or_raise
        self._module.get_unique_or_raise = lambda *a, **k: self._obj
        return self

    def __exit__(self, *exc):
        self._module.get_unique_or_raise = self._original
        return False


class _patched_forget:
    def __init__(self, module):
        self._module = module

    def __enter__(self):
        self._original = self._module.forget_lookup_object
        self._module.forget_lookup_object = lambda *a, **k: None
        return self

    def __exit__(self, *exc):
        self._module.forget_lookup_object = self._original
        return False
