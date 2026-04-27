from netbox.api.routers import NetBoxRouter

from forward_netbox.api.views import ForwardDriftPolicyViewSet
from forward_netbox.api.views import ForwardIngestionIssueViewSet
from forward_netbox.api.views import ForwardIngestionViewSet
from forward_netbox.api.views import ForwardNQEMapViewSet
from forward_netbox.api.views import ForwardSourceViewSet
from forward_netbox.api.views import ForwardSyncViewSet
from forward_netbox.api.views import ForwardValidationRunViewSet


router = NetBoxRouter()
router.register("source", ForwardSourceViewSet)
router.register("sync", ForwardSyncViewSet)
router.register("ingestion", ForwardIngestionViewSet)
router.register("ingestion-issues", ForwardIngestionIssueViewSet)
router.register("nqe-map", ForwardNQEMapViewSet)
router.register("drift-policy", ForwardDriftPolicyViewSet)
router.register("validation-run", ForwardValidationRunViewSet)
urlpatterns = router.urls
