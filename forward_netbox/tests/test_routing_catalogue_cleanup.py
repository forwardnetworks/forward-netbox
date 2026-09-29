"""The operator cleanup for routing policy no in-scope device holds.

Deleting shared policy is exactly the kind of path that must name what it
touches and refuse everything else. These pin the negative space as hard as
the happy path: an entry this sync did not create, a definition a sync still
asserts, a list a staying route map matches on, an empty in-scope result, a
disabled model and an implausibly large share of the catalogue are all left
alone, and nothing outside the six catalogue models is ever deleted.
"""

import uuid
from unittest.mock import Mock
from unittest.mock import patch

from core.models import ObjectChange
from dcim.models import Device
from dcim.models import DeviceRole
from dcim.models import DeviceType
from dcim.models import Manufacturer
from dcim.models import Site
from django.apps import apps
from django.contrib.contenttypes.models import ContentType
from django.test import TestCase

from forward_netbox.models import ForwardIngestion
from forward_netbox.models import ForwardSource
from forward_netbox.models import ForwardSync
from forward_netbox.utilities.routing_catalogue_cleanup import (
    CATALOGUE_CLEANUP_MODELS,
)
from forward_netbox.utilities.routing_catalogue_cleanup import (
    HOLD_ASSERTED_BY_A_SYNC,
)
from forward_netbox.utilities.routing_catalogue_cleanup import HOLD_NO_CREATE_RECORD
from forward_netbox.utilities.routing_catalogue_cleanup import (
    HOLD_ROUTE_MAP_REFERENCE,
)
from forward_netbox.utilities.routing_catalogue_cleanup import (
    prune_out_of_scope_catalogue,
)
from forward_netbox.utilities.routing_catalogue_cleanup import REFUSED_EMPTY_SCOPE
from forward_netbox.utilities.routing_catalogue_cleanup import REFUSED_FRACTION
from forward_netbox.utilities.routing_catalogue_cleanup import REFUSED_NOT_ENABLED
from forward_netbox.utilities.sync import ForwardSyncRunner
from forward_netbox.utilities.sync_routing_policy import (
    apply_netbox_routing_prefixlistentry,
)
from forward_netbox.utilities.sync_routing_policy import (
    apply_netbox_routing_routemapentry,
)

PL = "netbox_routing.prefixlistentry"
CL = "netbox_routing.communitylistentry"
RM = "netbox_routing.routemapentry"


def _routing(name):
    return apps.get_model("netbox_routing", name)


class CatalogueCleanupTest(TestCase):
    def setUp(self):
        self.source = ForwardSource.objects.create(
            name="cat-source",
            type="saas",
            url="https://forward.example.com",
            status="ready",
            parameters={"network_id": "net-1"},
        )
        self.sync = ForwardSync.objects.create(
            name="cat-sync",
            source=self.source,
            parameters={"snapshot_id": "latestProcessed", PL: True, CL: True, RM: True},
        )
        self.request_id = uuid.uuid4()
        ForwardIngestion.objects.create(
            sync=self.sync, change_request_id=self.request_id
        )
        site = Site.objects.create(name="Cat Site", slug="cat-site")
        mfr = Manufacturer.objects.create(name="Cat Mfr", slug="cat-mfr")
        dtype = DeviceType.objects.create(
            manufacturer=mfr, model="Cat DT", slug="cat-dt"
        )
        role = DeviceRole.objects.create(name="Cat Role", slug="cat-role")
        self.device = Device.objects.create(
            name="pol-a", site=site, device_type=dtype, role=role, status="active"
        )

    def _runner(self):
        return ForwardSyncRunner(
            sync=self.sync, ingestion=None, client=None, logger_=Mock()
        )

    def _prefix_entry(self, name, sequence, *, created_by_sync=True):
        entry = apply_netbox_routing_prefixlistentry(
            self._runner(),
            {
                "name": name,
                "device": "pol-a",
                "device_count": 1,
                "list_name": name,
                "family": 4,
                "sequence": sequence,
                "action": "permit",
                "prefix": "10.0.0.0/8",
                "ge": None,
                "le": None,
                "eq": None,
                "os": "OS.IOS_XE",
            },
        )
        if created_by_sync:
            self._created(entry)
        return entry

    def _created(self, obj, request_id=None):
        ObjectChange.objects.create(
            request_id=request_id or self.request_id,
            user_name="forward",
            changed_object_type=ContentType.objects.get_for_model(type(obj)),
            changed_object_id=obj.pk,
            object_repr=str(obj),
            action="create",
        )

    def _scope(
        self,
        *,
        pl_in=("PL-KEEP",),
        pl_out=(),
        cl_in=("CL-X",),
        rm_in=("RM-X",),
        rm_out=()
    ):
        return {
            PL: {"in_scope": set(pl_in), "out_of_scope": set(pl_out)},
            CL: {"in_scope": set(cl_in), "out_of_scope": set()},
            RM: {"in_scope": set(rm_in), "out_of_scope": set(rm_out)},
        }

    def _run(self, scope):
        with patch(
            "forward_netbox.utilities.routing_catalogue_cleanup._asserted_definition_names",
            return_value=set(),
        ):
            return prune_out_of_scope_catalogue(self.sync, scope_names=scope)

    # -- the happy path ----------------------------------------------------

    def test_out_of_scope_entries_this_sync_created_are_removed_with_their_list(self):
        self._prefix_entry("PL-GONE", 10)
        self._prefix_entry("PL-GONE", 20)
        keep = self._prefix_entry("PL-KEEP", 10)

        result = self._run(self._scope(pl_out=("PL-GONE",)))

        self.assertEqual(result[PL]["deleted_entries"], 2)
        self.assertEqual(result[PL]["deleted_lists"], 1)
        self.assertEqual(
            list(_routing("PrefixList").objects.values_list("name", flat=True)),
            ["PL-KEEP"],
        )
        self.assertTrue(_routing("PrefixListEntry").objects.filter(pk=keep.pk).exists())

    def test_a_list_stored_under_another_spelling_is_matched(self):
        self._prefix_entry("PL-GONE", 10)
        _routing("PrefixList").objects.filter(name="PL-GONE").update(name="pl-gone")

        result = self._run(self._scope(pl_out=("PL-GONE",)))

        self.assertEqual(result[PL]["deleted_entries"], 1)

    # -- the negative space ------------------------------------------------

    def test_an_entry_this_sync_did_not_create_is_held(self):
        self._prefix_entry("PL-GONE", 10, created_by_sync=False)
        other = self._prefix_entry("PL-GONE", 20, created_by_sync=False)
        self._created(other, request_id=uuid.uuid4())  # another sync's ingestion

        result = self._run(self._scope(pl_out=("PL-GONE",)))

        self.assertEqual(result[PL]["deleted_entries"], 0)
        self.assertEqual(result[PL]["held"], {HOLD_NO_CREATE_RECORD: 2})
        self.assertEqual(_routing("PrefixListEntry").objects.count(), 2)

    def test_a_definition_a_sync_still_asserts_is_held(self):
        self._prefix_entry("PL-GONE", 10)
        with patch(
            "forward_netbox.utilities.routing_catalogue_cleanup._asserted_definition_names",
            return_value={"pl-gone"},
        ):
            result = prune_out_of_scope_catalogue(
                self.sync, scope_names=self._scope(pl_out=("PL-GONE",))
            )

        self.assertEqual(result[PL]["held"], {HOLD_ASSERTED_BY_A_SYNC: 1})
        self.assertEqual(_routing("PrefixListEntry").objects.count(), 1)

    def test_a_list_a_staying_route_map_matches_on_is_held(self):
        entry = self._prefix_entry("PL-GONE", 10)
        route_map_entry = apply_netbox_routing_routemapentry(
            self._runner(),
            {
                "name": "RM-X",
                "device": "pol-a",
                "device_count": 1,
                "map_name": "RM-X",
                "sequence": 10,
                "action": "permit",
                "clauses": [],
                "os": "OS.IOS_XE",
            },
        )
        route_map_entry.match_prefix_list.add(entry.prefix_list)

        result = self._run(self._scope(pl_out=("PL-GONE",)))

        self.assertEqual(result[PL]["held"], {HOLD_ROUTE_MAP_REFERENCE: 1})
        self.assertEqual(_routing("PrefixListEntry").objects.count(), 1)

    def test_an_empty_in_scope_result_refuses_the_model(self):
        self._prefix_entry("PL-GONE", 10)

        result = self._run(self._scope(pl_in=(), pl_out=("PL-GONE",)))

        self.assertEqual(result[PL]["refused"], REFUSED_EMPTY_SCOPE)
        self.assertEqual(_routing("PrefixListEntry").objects.count(), 1)

    def test_a_disabled_model_is_never_touched(self):
        self.sync.parameters = {**self.sync.parameters, PL: False}
        self.sync.save()
        self._prefix_entry("PL-GONE", 10)

        result = self._run(self._scope(pl_out=("PL-GONE",)))

        self.assertEqual(result[PL]["refused"], REFUSED_NOT_ENABLED)
        self.assertEqual(_routing("PrefixListEntry").objects.count(), 1)

    def test_too_large_a_share_of_the_catalogue_is_refused(self):
        for sequence in range(1, 31):
            self._prefix_entry("PL-GONE", sequence * 10)
        self._prefix_entry("PL-KEEP", 10)

        result = self._run(self._scope(pl_out=("PL-GONE",)))

        self.assertEqual(result[PL]["refused"], REFUSED_FRACTION)
        self.assertEqual(_routing("PrefixListEntry").objects.count(), 31)

    def test_nothing_outside_the_catalogue_models_is_deleted(self):
        self._prefix_entry("PL-GONE", 10)
        devices = Device.objects.count()

        self._run(self._scope(pl_out=("PL-GONE",)))

        self.assertEqual(Device.objects.count(), devices)
        self.assertEqual(
            CATALOGUE_CLEANUP_MODELS,
            frozenset(
                {
                    "netbox_routing.prefixlist",
                    "netbox_routing.prefixlistentry",
                    "netbox_routing.communitylist",
                    "netbox_routing.communitylistentry",
                    "netbox_routing.routemap",
                    "netbox_routing.routemapentry",
                }
            ),
        )

    def test_no_device_tag_scope_refuses_before_any_read(self):
        from forward_netbox.utilities.routing_catalogue_cleanup import (
            CatalogueCleanupRefused,
        )

        context = Mock(scoped_device_names=set())
        fetcher = Mock(resolve_context=Mock(return_value=context))
        with patch.object(ForwardSource, "get_client", return_value=Mock()), patch(
            "forward_netbox.utilities.query_fetch.ForwardQueryFetcher",
            return_value=fetcher,
        ):
            with self.assertRaises(CatalogueCleanupRefused):
                prune_out_of_scope_catalogue(self.sync)
        fetcher.fetch_workloads.assert_not_called()
