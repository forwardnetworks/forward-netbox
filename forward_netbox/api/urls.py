# forward_netbox/api/urls.py

from netbox.api.routers import NetBoxRouter
from forward_netbox.api.views import (
    ForwardSourceViewSet,
    ForwardSnapshotViewSet,
    ForwardSyncViewSet,
    ForwardNQEMapViewSet,
)

router = NetBoxRouter()
router.register("sources", ForwardSourceViewSet)
router.register("snapshots", ForwardSnapshotViewSet)
router.register("syncs", ForwardSyncViewSet)
router.register("nqe-maps", ForwardNQEMapViewSet)

urlpatterns = router.urls
