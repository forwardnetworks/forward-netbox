# api/urls.py
from netbox.api.routers import NetBoxRouter

from forward_netbox.api.views import ForwardIngestionIssueViewSet
from forward_netbox.api.views import ForwardIngestionViewSet
from forward_netbox.api.views import ForwardRelationshipFieldViewSet
from forward_netbox.api.views import ForwardSnapshotViewSet
from forward_netbox.api.views import ForwardSourceViewSet
from forward_netbox.api.views import ForwardSyncViewSet
from forward_netbox.api.views import ForwardTransformFieldViewSet
from forward_netbox.api.views import ForwardTransformMapGroupViewSet
from forward_netbox.api.views import ForwardTransformMapViewSet


router = NetBoxRouter()
router.register("source", ForwardSourceViewSet)
router.register("snapshot", ForwardSnapshotViewSet)
router.register("transform-map-group", ForwardTransformMapGroupViewSet)
router.register("transform-map", ForwardTransformMapViewSet)
router.register("sync", ForwardSyncViewSet)
router.register("ingestion", ForwardIngestionViewSet)
router.register("ingestion-issues", ForwardIngestionIssueViewSet)
router.register("transform-field", ForwardTransformFieldViewSet)
router.register("relationship-field", ForwardRelationshipFieldViewSet)
urlpatterns = router.urls
