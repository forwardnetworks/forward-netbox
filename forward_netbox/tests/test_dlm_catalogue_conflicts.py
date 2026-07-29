"""A shared DLM catalogue row must never be created twice in one branch.

`netbox_dlm.cve` is a global catalogue keyed by `cve_id`, created from two
paths: the CVE map applies the rich catalogue row, and the vulnerability path
ensures-if-missing. Without `reuse_on_unique_conflict` a create that loses the
race raises IntegrityError rather than reusing the existing row, so a branch can
end up holding two rows for one `cve_id`.

That cannot be repaired at merge. `Vulnerability.cve` is an inbound FK, so
converging the duplicate by natural key would strand every branch-side
vulnerability pointing at the discarded pk — which is exactly why
`_resume_existing_create` resolves by pk only. Preventing the duplicate at
staging is the one place the fix is safe.

Observed live: four `netbox_dlm.cve` IntegrityErrors at merge blocked a
customer's baseline, and because the merge is retryable but deterministic, every
retry failed identically.
"""

from unittest.mock import Mock
from unittest.mock import patch

from django.apps import apps
from django.test import SimpleTestCase
from django.test import TestCase

from forward_netbox.models import ForwardSource
from forward_netbox.models import ForwardSync
from forward_netbox.utilities.sync import ForwardSyncRunner
from forward_netbox.utilities.sync_primitives import UNIQUE_LOOKUP_CACHE_FIELD_SETS


class DLMCatalogueConflictPolicyTest(SimpleTestCase):
    def _policy(self, model_string):
        return ForwardSyncRunner.MODEL_CONFLICT_POLICIES.get(model_string, "strict")

    def test_cve_reuses_an_existing_row_on_a_unique_conflict(self):
        self.assertEqual(self._policy("netbox_dlm.cve"), "reuse_on_unique_conflict")

    def test_software_version_reuses_an_existing_row_on_a_unique_conflict(self):
        # Same shape and the same dual-path creation as CVE; the ensure-if-
        # missing safety net is named in `ensure_dlm_cve`'s own docstring.
        self.assertEqual(
            self._policy("netbox_dlm.softwareversion"), "reuse_on_unique_conflict"
        )

    def test_every_shared_catalogue_model_shares_the_policy(self):
        # The bug was an inconsistency, not a missing concept: every comparable
        # natural-key catalogue already had this policy and CVE did not.
        for model_string in (
            "dcim.site",
            "dcim.manufacturer",
            "dcim.platform",
            "dcim.devicetype",
            "netbox_dlm.cve",
            "netbox_dlm.softwareversion",
        ):
            self.assertEqual(
                self._policy(model_string),
                "reuse_on_unique_conflict",
                f"{model_string} is a shared natural-key catalogue and must "
                "reuse rather than raise on a unique conflict",
            )

    def test_a_device_scoped_model_is_still_strict(self):
        # The policy must not leak to models where a unique conflict is a real
        # defect rather than a benign race.
        self.assertEqual(self._policy("dcim.device"), "strict")
        self.assertEqual(self._policy("dcim.interface"), "strict")

    def test_cve_is_cached_by_its_natural_key(self):
        # Its siblings were all cache-eligible and CVE was not, so every CVE
        # lookup bypassed the identity cache.
        self.assertEqual(
            UNIQUE_LOOKUP_CACHE_FIELD_SETS.get("netbox_dlm.cve"), (("cve_id",),)
        )

    def test_the_cve_cache_key_matches_the_coalesce_contract(self):
        from forward_netbox.utilities.sync_contracts import MODEL_SYNC_CONTRACTS

        contract = MODEL_SYNC_CONTRACTS["netbox_dlm.cve"]
        self.assertEqual(contract.default_coalesce_fields, (("cve_id",),))
        self.assertIn(
            contract.default_coalesce_fields[0],
            UNIQUE_LOOKUP_CACHE_FIELD_SETS["netbox_dlm.cve"],
            "the cached identity must be the same key the sync coalesces on, "
            "or the cache answers a different question than the lookup asks",
        )


class DLMCatalogueConflictBehaviourTest(TestCase):
    """The policy is only worth anything if a real conflict actually reuses."""

    def setUp(self):
        self.source = ForwardSource.objects.create(
            name="dlm-conflict-src",
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
            name="dlm-conflict-sync",
            source=self.source,
            parameters={"snapshot_id": "latestProcessed"},
        )

    def _runner(self):
        return ForwardSyncRunner(
            sync=self.sync, ingestion=None, client=None, logger_=Mock()
        )

    def test_a_second_ensure_reuses_the_first_row(self):
        from forward_netbox.utilities.sync_dlm import ensure_dlm_cve

        CVE = apps.get_model("netbox_dlm", "CVE")
        first = ensure_dlm_cve(self._runner(), {"cve_id": "CVE-2026-00001"})
        # A fresh runner, as every plan item gets: no warm identity cache.
        second = ensure_dlm_cve(self._runner(), {"cve_id": "CVE-2026-00001"})

        self.assertEqual(first.pk, second.pk)
        self.assertEqual(CVE.objects.filter(cve_id="CVE-2026-00001").count(), 1)

    def test_a_create_losing_the_race_reuses_instead_of_raising(self):
        """The actual failure: the lookup misses, then the insert collides.

        Simulated by making the pre-create lookup blind so the create is
        attempted against a row that already exists — which is what produced
        four IntegrityErrors at merge and blocked a customer's baseline.
        """
        from forward_netbox.utilities import sync_primitives
        from forward_netbox.utilities.sync_dlm import ensure_dlm_cve

        CVE = apps.get_model("netbox_dlm", "CVE")
        CVE.objects.create(cve_id="CVE-2026-00002")

        with patch.object(
            sync_primitives, "get_unique_or_raise", side_effect=[None, CVE.objects.get(cve_id="CVE-2026-00002")]
        ):
            reused = ensure_dlm_cve(self._runner(), {"cve_id": "CVE-2026-00002"})

        self.assertIsNotNone(reused)
        self.assertEqual(CVE.objects.filter(cve_id="CVE-2026-00002").count(), 1)

    def test_the_reused_row_is_returned_so_foreign_keys_point_at_it(self):
        # Vulnerability.cve is an inbound FK. If a reuse returned None or a
        # detached object, the caller would attach a vulnerability to the wrong
        # CVE — the exact corruption that makes a merge-time fix unsafe.
        from forward_netbox.utilities.sync_dlm import ensure_dlm_cve

        CVE = apps.get_model("netbox_dlm", "CVE")
        existing = CVE.objects.create(cve_id="CVE-2026-00003")
        returned = ensure_dlm_cve(self._runner(), {"cve_id": "CVE-2026-00003"})
        self.assertEqual(returned.pk, existing.pk)
