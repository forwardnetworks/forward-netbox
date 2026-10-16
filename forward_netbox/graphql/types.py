# GraphQL types for change control and validation. Scalars and the relations between
# these models only: the sync-side models are not exposed here, and a change's
# evidence rows, which hold query shapes, are left out on purpose.
from typing import Annotated

import strawberry
import strawberry_django
from netbox.graphql.types import BaseObjectType
from netbox.graphql.types import ObjectType
from netbox.graphql.types import PrimaryObjectType

from forward_netbox import models
from forward_netbox.graphql import filters

__all__ = (
    "ForwardChangeApprovalRuleType",
    "ForwardChangeCommentType",
    "ForwardChangeCriterionType",
    "ForwardChangePolicyType",
    "ForwardChangeReviewType",
    "ForwardChangeType",
    "ForwardChangeValidationPolicyType",
    "ForwardChangeValidationResultType",
    "ForwardChangeValidationRuleType",
    "ForwardChangeValidationRunType",
)

_CHANGE = Annotated[
    "ForwardChangeType", strawberry.lazy("forward_netbox.graphql.types")
]
_POLICY = Annotated[
    "ForwardChangePolicyType", strawberry.lazy("forward_netbox.graphql.types")
]
_VALIDATION_POLICY = Annotated[
    "ForwardChangeValidationPolicyType",
    strawberry.lazy("forward_netbox.graphql.types"),
]
_RUN = Annotated[
    "ForwardChangeValidationRunType", strawberry.lazy("forward_netbox.graphql.types")
]


@strawberry_django.type(
    models.ForwardChange,
    fields=[
        "id",
        "ref",
        "title",
        "state",
        "verdict",
        "priority",
        "summary",
        "branch_name",
        "before_snapshot_id",
        "after_snapshot_id",
        "workspace_network_id",
        "workspace_origin",
        "window_start",
        "window_end",
        "applied_at",
        "applied_ref",
        "predict_status",
        "predict_reason",
        "description",
        "comments",
        "created",
        "last_updated",
    ],
    filters=filters.ForwardChangeFilter,
    pagination=True,
)
class ForwardChangeType(PrimaryObjectType):
    pass


@strawberry_django.type(
    models.ForwardChangePolicy,
    fields=[
        "id",
        "name",
        "enabled",
        "min_pre_approvals",
        "min_post_approvals",
        "description",
        "comments",
        "created",
        "last_updated",
    ],
    filters=filters.ForwardChangePolicyFilter,
    pagination=True,
)
class ForwardChangePolicyType(PrimaryObjectType):
    pass


@strawberry_django.type(
    models.ForwardChangeApprovalRule,
    fields=["id", "name", "min_reviews", "any_reviewer", "policy"],
    filters=filters.ForwardChangeApprovalRuleFilter,
    pagination=True,
)
class ForwardChangeApprovalRuleType(ObjectType):
    policy: _POLICY


@strawberry_django.type(
    models.ForwardChangeCriterion,
    fields=[
        "id",
        "name",
        "family",
        "expectation",
        "blocking",
        "query_path",
        "query_id",
        "commit_id",
        "change",
    ],
    filters=filters.ForwardChangeCriterionFilter,
    pagination=True,
)
class ForwardChangeCriterionType(ObjectType):
    change: _CHANGE


@strawberry_django.type(
    models.ForwardChangeReview,
    fields=["id", "phase", "approved", "comment", "change", "created"],
    filters=filters.ForwardChangeReviewFilter,
    pagination=True,
)
class ForwardChangeReviewType(ObjectType):
    change: _CHANGE


@strawberry_django.type(
    models.ForwardChangeComment,
    fields=["id", "body", "resolved", "resolved_at", "change", "created"],
    filters=filters.ForwardChangeCommentFilter,
    pagination=True,
)
class ForwardChangeCommentType(ObjectType):
    change: _CHANGE


@strawberry_django.type(
    models.ForwardChangeValidationPolicy,
    fields=[
        "id",
        "name",
        "enabled",
        "min_score",
        "require_passed_run",
        "run_on_submission",
        "description",
        "comments",
        "created",
        "last_updated",
    ],
    filters=filters.ForwardChangeValidationPolicyFilter,
    pagination=True,
)
class ForwardChangeValidationPolicyType(PrimaryObjectType):
    pass


@strawberry_django.type(
    models.ForwardChangeValidationRule,
    fields=[
        "id",
        "name",
        "engine",
        "category",
        "severity",
        "enabled",
        "parameters",
        "policy",
    ],
    filters=filters.ForwardChangeValidationRuleFilter,
    pagination=True,
)
class ForwardChangeValidationRuleType(ObjectType):
    policy: _VALIDATION_POLICY


@strawberry_django.type(
    models.ForwardChangeValidationRun,
    fields=[
        "id",
        "trigger",
        "status",
        "started_at",
        "completed_at",
        "snapshot_id",
        "score",
        "summary",
        "error",
        "change",
        "policy",
        "created",
    ],
    filters=filters.ForwardChangeValidationRunFilter,
    pagination=True,
)
class ForwardChangeValidationRunType(ObjectType):
    change: _CHANGE
    policy: _VALIDATION_POLICY | None


@strawberry_django.type(
    models.ForwardChangeValidationResult,
    fields=[
        "id",
        "engine",
        "category",
        "check_name",
        "device_name",
        "status",
        "severity",
        "message",
        "row_count",
        "run",
    ],
    pagination=True,
)
class ForwardChangeValidationResultType(BaseObjectType):
    run: _RUN
