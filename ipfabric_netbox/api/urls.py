# api/urls.py
from netbox.api.routers import NetBoxRouter

from ipfabric_netbox.api.views import IPFabricIngestionIssueViewSet
from ipfabric_netbox.api.views import IPFabricIngestionViewSet
from ipfabric_netbox.api.views import IPFabricRelationshipFieldViewSet
from ipfabric_netbox.api.views import IPFabricSnapshotViewSet
from ipfabric_netbox.api.views import IPFabricSourceViewSet
from ipfabric_netbox.api.views import IPFabricSyncViewSet
from ipfabric_netbox.api.views import IPFabricTransformFieldViewSet
from ipfabric_netbox.api.views import IPFabricTransformMapGroupViewSet
from ipfabric_netbox.api.views import IPFabricTransformMapViewSet


router = NetBoxRouter()
router.register("source", IPFabricSourceViewSet)
router.register("snapshot", IPFabricSnapshotViewSet)
router.register("transform-map-group", IPFabricTransformMapGroupViewSet)
router.register("transform-map", IPFabricTransformMapViewSet)
router.register("sync", IPFabricSyncViewSet)
router.register("ingestion", IPFabricIngestionViewSet)
router.register("ingestion-issues", IPFabricIngestionIssueViewSet)
router.register("transform-field", IPFabricTransformFieldViewSet)
router.register("relationship-field", IPFabricRelationshipFieldViewSet)
urlpatterns = router.urls
