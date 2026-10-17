import strawberry_django
from netbox.graphql.filters import ChangeLoggedModelFilter
from netbox.graphql.filters import PrimaryModelFilter
from strawberry_django import BaseFilterLookup
from strawberry_django import StrFilterLookup

from forward_netbox import models

__all__ = (
    "ForwardChangeApprovalRuleFilter",
    "ForwardChangeCommentFilter",
    "ForwardChangeCriterionFilter",
    "ForwardChangeFilter",
    "ForwardChangePolicyFilter",
    "ForwardChangeReviewFilter",
    "ForwardChangeValidationPolicyFilter",
    "ForwardChangeValidationRuleFilter",
    "ForwardChangeValidationRunFilter",
)


@strawberry_django.filter(models.ForwardChange, lookups=True)
class ForwardChangeFilter(PrimaryModelFilter):
    ref: StrFilterLookup | None = strawberry_django.filter_field()
    title: StrFilterLookup | None = strawberry_django.filter_field()
    state: BaseFilterLookup[str] | None = strawberry_django.filter_field()
    verdict: BaseFilterLookup[str] | None = strawberry_django.filter_field()
    priority: BaseFilterLookup[int] | None = strawberry_django.filter_field()


@strawberry_django.filter(models.ForwardChangePolicy, lookups=True)
class ForwardChangePolicyFilter(PrimaryModelFilter):
    name: StrFilterLookup | None = strawberry_django.filter_field()
    enabled: BaseFilterLookup[bool] | None = strawberry_django.filter_field()


@strawberry_django.filter(models.ForwardChangeApprovalRule, lookups=True)
class ForwardChangeApprovalRuleFilter(ChangeLoggedModelFilter):
    name: StrFilterLookup | None = strawberry_django.filter_field()
    any_reviewer: BaseFilterLookup[bool] | None = strawberry_django.filter_field()


@strawberry_django.filter(models.ForwardChangeCriterion, lookups=True)
class ForwardChangeCriterionFilter(ChangeLoggedModelFilter):
    name: StrFilterLookup | None = strawberry_django.filter_field()
    family: BaseFilterLookup[str] | None = strawberry_django.filter_field()
    blocking: BaseFilterLookup[bool] | None = strawberry_django.filter_field()


@strawberry_django.filter(models.ForwardChangeReview, lookups=True)
class ForwardChangeReviewFilter(ChangeLoggedModelFilter):
    phase: BaseFilterLookup[str] | None = strawberry_django.filter_field()
    approved: BaseFilterLookup[bool] | None = strawberry_django.filter_field()


@strawberry_django.filter(models.ForwardChangeComment, lookups=True)
class ForwardChangeCommentFilter(ChangeLoggedModelFilter):
    resolved: BaseFilterLookup[bool] | None = strawberry_django.filter_field()


@strawberry_django.filter(models.ForwardChangeValidationPolicy, lookups=True)
class ForwardChangeValidationPolicyFilter(PrimaryModelFilter):
    name: StrFilterLookup | None = strawberry_django.filter_field()
    enabled: BaseFilterLookup[bool] | None = strawberry_django.filter_field()


@strawberry_django.filter(models.ForwardChangeValidationRule, lookups=True)
class ForwardChangeValidationRuleFilter(ChangeLoggedModelFilter):
    name: StrFilterLookup | None = strawberry_django.filter_field()
    engine: BaseFilterLookup[str] | None = strawberry_django.filter_field()
    severity: BaseFilterLookup[str] | None = strawberry_django.filter_field()
    enabled: BaseFilterLookup[bool] | None = strawberry_django.filter_field()


@strawberry_django.filter(models.ForwardChangeValidationRun, lookups=True)
class ForwardChangeValidationRunFilter(ChangeLoggedModelFilter):
    status: BaseFilterLookup[str] | None = strawberry_django.filter_field()
    trigger: BaseFilterLookup[str] | None = strawberry_django.filter_field()
