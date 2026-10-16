from netbox.api.routers import NetBoxRouter

from forward_netbox.api.change_views import ForwardChangeApprovalRuleViewSet
from forward_netbox.api.change_views import ForwardChangeCommentViewSet
from forward_netbox.api.change_views import ForwardChangeCriterionViewSet
from forward_netbox.api.change_views import ForwardChangeEvidenceViewSet
from forward_netbox.api.change_views import ForwardChangePolicyRuleViewSet
from forward_netbox.api.change_views import ForwardChangePolicyViewSet
from forward_netbox.api.change_views import ForwardChangeReviewViewSet
from forward_netbox.api.change_views import ForwardChangeViewSet
from forward_netbox.api.views import ForwardDeviceAnalysisViewSet
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
router.register("device-analysis", ForwardDeviceAnalysisViewSet)
router.register("drift-policy", ForwardDriftPolicyViewSet)
router.register("validation-run", ForwardValidationRunViewSet)
router.register("changes", ForwardChangeViewSet)
router.register("change-policy", ForwardChangePolicyViewSet)
router.register("change-policy-rule", ForwardChangePolicyRuleViewSet)
router.register("change-approval-rule", ForwardChangeApprovalRuleViewSet)
router.register("change-criterion", ForwardChangeCriterionViewSet)
router.register("change-evidence", ForwardChangeEvidenceViewSet)
router.register("change-review", ForwardChangeReviewViewSet)
router.register("change-comment", ForwardChangeCommentViewSet)
urlpatterns = router.urls
