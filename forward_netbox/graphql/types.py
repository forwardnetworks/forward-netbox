from typing import Annotated

import strawberry
import strawberry_django
from core.graphql.mixins import ChangelogMixin
from core.graphql.types import ContentType as ContentTypeType
from core.models import Job
from extras.graphql.mixins import TagsMixin
from netbox.graphql.types import BaseObjectType
from netbox.graphql.types import OrganizationalObjectType
from netbox_branching.models import Branch
from strawberry.scalars import JSON
from users.graphql.types import UserType

from .filters import BranchFilter
from .filters import ForwardNQEQueryFilter
from .filters import ForwardDataFilter
from .filters import ForwardIngestionFilter
from .filters import ForwardIngestionIssueFilter
from .filters import ForwardSnapshotFilter
from .filters import ForwardSourceFilter
from .filters import ForwardSyncFilter
from .filters import JobFilter
from forward_netbox import models


__all__ = (
    "ForwardNQEQueryType",
    "ForwardSourceType",
    "ForwardSnapshotType",
    "ForwardSyncType",
    "ForwardIngestionType",
    "ForwardIngestionIssueType",
    "ForwardDataType",
)


@strawberry_django.type(
    models.ForwardNQEQuery, fields="__all__", filters=ForwardNQEQueryFilter
)
class ForwardNQEQueryType(BaseObjectType):
    content_type: ContentTypeType
    enabled: bool


@strawberry_django.type(
    models.ForwardSource, fields="__all__", filters=ForwardSourceFilter
)
class ForwardSourceType(OrganizationalObjectType):
    name: str
    type: str
    url: str
    status: str
    parameters: JSON
    last_synced: str


@strawberry_django.type(
    models.ForwardSnapshot, fields="__all__", filters=ForwardSnapshotFilter
)
class ForwardSnapshotType(ChangelogMixin, TagsMixin, BaseObjectType):
    source: (
        Annotated[
            "ForwardSourceType", strawberry.lazy("forward_netbox.graphql.types")
        ]
        | None
    )
    name: str
    snapshot_id: str
    data: JSON
    status: str


@strawberry_django.type(
    models.ForwardSync, fields="__all__", filters=ForwardSyncFilter
)
class ForwardSyncType(ChangelogMixin, TagsMixin, BaseObjectType):
    name: str
    snapshot_data: (
        Annotated[
            "ForwardSnapshotType", strawberry.lazy("forward_netbox.graphql.types")
        ]
        | None
    )
    status: str
    parameters: JSON
    auto_merge: bool
    last_synced: str | None
    scheduled: str | None
    interval: int | None
    user: Annotated["UserType", strawberry.lazy("users.graphql.types")] | None


@strawberry_django.type(Branch, fields="__all__", filters=BranchFilter)
class BranchType(OrganizationalObjectType):
    name: str
    description: str | None
    owner: Annotated["UserType", strawberry.lazy("users.graphql.types")]
    merged_by: Annotated["UserType", strawberry.lazy("users.graphql.types")]


@strawberry_django.type(Job, fields="__all__", filters=JobFilter)
class JobType(BaseObjectType):
    name: str
    user: Annotated["UserType", strawberry.lazy("users.graphql.types")]


@strawberry_django.type(
    models.ForwardIngestion, fields="__all__", filters=ForwardIngestionFilter
)
class ForwardIngestionType(BaseObjectType):
    sync: (
        Annotated["ForwardSyncType", strawberry.lazy("forward_netbox.graphql.types")]
        | None
    )
    job: Annotated["JobType", strawberry.lazy("forward_netbox.graphql.types")] | None
    branch: (
        Annotated["BranchType", strawberry.lazy("forward_netbox.graphql.types")] | None
    )


@strawberry_django.type(
    models.ForwardIngestionIssue,
    fields="__all__",
    filters=ForwardIngestionIssueFilter,
)
class ForwardIngestionIssueType(BaseObjectType):
    ingestion: (
        Annotated[
            "ForwardIngestionType", strawberry.lazy("forward_netbox.graphql.types")
        ]
        | None
    )
    timestamp: str
    model: str | None
    message: str
    raw_data: str
    coalesce_fields: str
    defaults: str
    exception: str


@strawberry_django.type(
    models.ForwardData, fields="__all__", filters=ForwardDataFilter
)
class ForwardDataType(BaseObjectType):
    snapshot_data: Annotated[
        "ForwardSnapshotType", strawberry.lazy("forward_netbox.graphql.types")
    ]
