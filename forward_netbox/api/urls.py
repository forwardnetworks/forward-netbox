# api/urls.py
from netbox.api.routers import NetBoxRouter

from forward_netbox.api.views import ForwardIngestionIssueViewSet
from forward_netbox.api.views import ForwardIngestionViewSet
from forward_netbox.api.views import ForwardNQEQueryViewSet
from forward_netbox.api.views import ForwardSnapshotViewSet
from forward_netbox.api.views import ForwardSourceViewSet
from forward_netbox.api.views import ForwardSyncViewSet


router = NetBoxRouter()
router.register("source", ForwardSourceViewSet)
router.register("snapshot", ForwardSnapshotViewSet)
router.register("nqe-map", ForwardNQEQueryViewSet)
router.register("sync", ForwardSyncViewSet)
router.register("ingestion", ForwardIngestionViewSet)
router.register("ingestion-issues", ForwardIngestionIssueViewSet)
urlpatterns = router.urls
