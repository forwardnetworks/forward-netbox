from netbox.api.routers import NetBoxRouter

from forward_netbox.api.views import ForwardDeviceAnalysisViewSet
from forward_netbox.api.views import ForwardDriftPolicyViewSet
from forward_netbox.api.views import ForwardExecutionRunViewSet
from forward_netbox.api.views import ForwardExecutionStepViewSet
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
router.register("execution-run", ForwardExecutionRunViewSet)
router.register("execution-step", ForwardExecutionStepViewSet)
router.register("nqe-map", ForwardNQEMapViewSet)
router.register("device-analysis", ForwardDeviceAnalysisViewSet)
router.register("drift-policy", ForwardDriftPolicyViewSet)
router.register("validation-run", ForwardValidationRunViewSet)
urlpatterns = router.urls
