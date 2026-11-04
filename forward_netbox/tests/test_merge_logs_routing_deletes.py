# A customer's run staged 156 deletes and the change log held only the 23 prefix
# deletes: 133 routing rows were staged and no change-log row named them. This
# stages a routing delete in a real branch, merges it, and reads the main change
# log by the ingestion's request id, the way the bundle does.
import uuid

from core.models import ObjectChange
from django.apps import apps
from django.contrib.auth import get_user_model
from django.contrib.contenttypes.models import ContentType
from django.test import RequestFactory
from django.test import TransactionTestCase
from django.urls import reverse
from netbox.context_managers import event_tracking
from netbox_branching.utilities import activate_branch

from forward_netbox.models import ForwardIngestion
from forward_netbox.models import ForwardSource
from forward_netbox.models import ForwardSync
from forward_netbox.tests.test_primary_ip_integration import provision_branch
from forward_netbox.utilities.merge import merge_branch


class MergeLogsRoutingDeletesTest(TransactionTestCase):
    def setUp(self):
        self.user = get_user_model().objects.create_user(username="mrd-user")
        self.request = RequestFactory().get(reverse("home"))
        self.request.user = self.user
        self.PrefixList = apps.get_model("netbox_routing", "PrefixList")
        self.source = ForwardSource.objects.create(
            name="mrd-src",
            type="saas",
            url="https://fwd.app",
            parameters={"network_id": "net-1"},
        )
        self.sync = ForwardSync.objects.create(
            name="mrd-sync", source=self.source, parameters={}
        )

    def test_a_staged_routing_delete_is_in_the_main_change_log_by_request_id(self):
        doomed = self.PrefixList.objects.create(name="PL-GONE", family=4)
        keep = self.PrefixList.objects.create(name="PL-KEEP", family=4)
        branch = provision_branch(user=self.user, name="MRD Branch")
        with activate_branch(branch), event_tracking(self.request):
            self.request.id = uuid.uuid4()
            self.PrefixList.objects.get(pk=doomed.pk).delete()
        ingestion = ForwardIngestion.objects.create(
            sync=self.sync,
            snapshot_selector="latestProcessed",
            snapshot_id="snapshot",
            branch=branch,
        )

        merge_branch(ingestion, user=self.user)

        self.assertFalse(self.PrefixList.objects.filter(pk=doomed.pk).exists())
        self.assertTrue(self.PrefixList.objects.filter(pk=keep.pk).exists())
        logged = ObjectChange.objects.filter(
            request_id=ingestion.change_request_id,
            action="delete",
            changed_object_type=ContentType.objects.get_for_model(self.PrefixList),
        ).count()
        anywhere = list(
            ObjectChange.objects.filter(
                changed_object_type=ContentType.objects.get_for_model(self.PrefixList),
                changed_object_id=doomed.pk,
            ).values_list("action", "request_id", "user_name")
        )
        self.assertEqual(
            logged,
            1,
            f"not in the log by request id; rows for this object anywhere: {anywhere}",
        )
