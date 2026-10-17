import strawberry
import strawberry_django

from forward_netbox.graphql.types import ForwardChangeApprovalRuleType
from forward_netbox.graphql.types import ForwardChangeCommentType
from forward_netbox.graphql.types import ForwardChangeCriterionType
from forward_netbox.graphql.types import ForwardChangePolicyType
from forward_netbox.graphql.types import ForwardChangeReviewType
from forward_netbox.graphql.types import ForwardChangeType
from forward_netbox.graphql.types import ForwardChangeValidationPolicyType
from forward_netbox.graphql.types import ForwardChangeValidationResultType
from forward_netbox.graphql.types import ForwardChangeValidationRuleType
from forward_netbox.graphql.types import ForwardChangeValidationRunType


@strawberry.type(name="Query")
class ForwardChangeQuery:
    forward_change: ForwardChangeType = strawberry_django.field()
    forward_change_list: list[ForwardChangeType] = strawberry_django.field()

    forward_change_policy: ForwardChangePolicyType = strawberry_django.field()
    forward_change_policy_list: list[ForwardChangePolicyType] = (
        strawberry_django.field()
    )

    forward_change_approval_rule: ForwardChangeApprovalRuleType = (
        strawberry_django.field()
    )
    forward_change_approval_rule_list: list[ForwardChangeApprovalRuleType] = (
        strawberry_django.field()
    )

    forward_change_criterion: ForwardChangeCriterionType = strawberry_django.field()
    forward_change_criterion_list: list[ForwardChangeCriterionType] = (
        strawberry_django.field()
    )

    forward_change_review: ForwardChangeReviewType = strawberry_django.field()
    forward_change_review_list: list[ForwardChangeReviewType] = (
        strawberry_django.field()
    )

    forward_change_comment: ForwardChangeCommentType = strawberry_django.field()
    forward_change_comment_list: list[ForwardChangeCommentType] = (
        strawberry_django.field()
    )


@strawberry.type(name="Query")
class ForwardChangeValidationQuery:
    forward_change_validation_policy: ForwardChangeValidationPolicyType = (
        strawberry_django.field()
    )
    forward_change_validation_policy_list: list[ForwardChangeValidationPolicyType] = (
        strawberry_django.field()
    )

    forward_change_validation_rule: ForwardChangeValidationRuleType = (
        strawberry_django.field()
    )
    forward_change_validation_rule_list: list[ForwardChangeValidationRuleType] = (
        strawberry_django.field()
    )

    forward_change_validation_run: ForwardChangeValidationRunType = (
        strawberry_django.field()
    )
    forward_change_validation_run_list: list[ForwardChangeValidationRunType] = (
        strawberry_django.field()
    )

    forward_change_validation_result: ForwardChangeValidationResultType = (
        strawberry_django.field()
    )
    forward_change_validation_result_list: list[ForwardChangeValidationResultType] = (
        strawberry_django.field()
    )


schema = [ForwardChangeQuery, ForwardChangeValidationQuery]
