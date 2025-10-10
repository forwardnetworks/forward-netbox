from datetime import datetime
from typing import Annotated

import strawberry
import strawberry_django
from core.graphql.filter_mixins import BaseFilterMixin
from core.graphql.filters import ContentTypeFilter
from core.models import Job
from netbox.graphql.filter_lookups import IntegerLookup
from netbox.graphql.filter_lookups import JSONFilter
from netbox.graphql.filter_lookups import StringArrayLookup
from netbox.graphql.filter_mixins import ChangeLogFilterMixin
from netbox.graphql.filter_mixins import NetBoxModelFilterMixin
from netbox.graphql.filter_mixins import PrimaryModelFilterMixin
from netbox.graphql.filter_mixins import TagsFilterMixin
from netbox_branching.models import Branch
from strawberry.scalars import ID
from strawberry_django import DatetimeFilterLookup
from strawberry_django import FilterLookup
from users.graphql.filters import UserFilter

from forward_netbox import models
from forward_netbox.graphql.enums import BranchStatusEnum
from forward_netbox.graphql.enums import DataSourceStatusEnum
from forward_netbox.graphql.enums import ForwardRawDataTypeEnum
from forward_netbox.graphql.enums import ForwardSnapshotStatusModelEnum
from forward_netbox.graphql.enums import ForwardSourceTypeEnum
from forward_netbox.graphql.enums import JobStatusEnum

__all__ = (
    "ForwardSourceFilter",
    "ForwardSnapshotFilter",
    "ForwardSyncFilter",
    "ForwardIngestionFilter",
    "ForwardIngestionIssueFilter",
    "ForwardDataFilter",
    "BranchFilter",
    "JobFilter",
)


@strawberry_django.filter(models.ForwardSource, lookups=True)
class ForwardSourceFilter(PrimaryModelFilterMixin):
    id: ID | None = strawberry_django.filter_field()
    name: FilterLookup[str] | None = strawberry_django.filter_field()
    type: (
        Annotated[
            "ForwardSourceTypeEnum", strawberry.lazy("forward_netbox.graphql.enums")
        ]
        | None
    ) = strawberry_django.filter_field()
    url: FilterLookup[str] | None = strawberry_django.filter_field()
    status: (
        Annotated[
            "DataSourceStatusEnum", strawberry.lazy("forward_netbox.graphql.enums")
        ]
        | None
    ) = strawberry_django.filter_field()
    parameters: (
        Annotated["JSONFilter", strawberry.lazy("netbox.graphql.filter_lookups")] | None
    ) = strawberry_django.filter_field()
    last_synced: DatetimeFilterLookup[
        datetime
    ] | None = strawberry_django.filter_field()


@strawberry_django.filter(models.ForwardSnapshot, lookups=True)
class ForwardSnapshotFilter(TagsFilterMixin, ChangeLogFilterMixin, BaseFilterMixin):
    id: ID | None = strawberry_django.filter_field()
    created: DatetimeFilterLookup[datetime] | None = strawberry_django.filter_field()
    last_updated: DatetimeFilterLookup[
        datetime
    ] | None = strawberry_django.filter_field()
    source: (
        Annotated[
            "ForwardSourceFilter", strawberry.lazy("forward_netbox.graphql.filters")
        ]
        | None
    ) = strawberry_django.filter_field()
    name: FilterLookup[str] | None = strawberry_django.filter_field()
    snapshot_id: FilterLookup[str] | None = strawberry_django.filter_field()
    data: (
        Annotated["JSONFilter", strawberry.lazy("netbox.graphql.filter_lookups")] | None
    ) = strawberry_django.filter_field()
    date: DatetimeFilterLookup[datetime] | None = strawberry_django.filter_field()
    status: (
        Annotated[
            "ForwardSnapshotStatusModelEnum",
            strawberry.lazy("forward_netbox.graphql.enums"),
        ]
        | None
    ) = strawberry_django.filter_field()


@strawberry_django.filter(models.ForwardSync, lookups=True)
class ForwardSyncFilter(TagsFilterMixin, ChangeLogFilterMixin, BaseFilterMixin):
    id: ID | None = strawberry_django.filter_field()
    name: FilterLookup[str] | None = strawberry_django.filter_field()
    snapshot_data: (
        Annotated[
            "ForwardSnapshotFilter", strawberry.lazy("forward_netbox.graphql.filters")
        ]
        | None
    ) = strawberry_django.filter_field()
    status: (
        Annotated[
            "DataSourceStatusEnum", strawberry.lazy("forward_netbox.graphql.enums")
        ]
        | None
    ) = strawberry_django.filter_field()
    parameters: (
        Annotated["JSONFilter", strawberry.lazy("netbox.graphql.filter_lookups")] | None
    ) = strawberry_django.filter_field()
    auto_merge: FilterLookup[bool] | None = strawberry_django.filter_field()
    last_synced: DatetimeFilterLookup[
        datetime
    ] | None = strawberry_django.filter_field()
    scheduled: DatetimeFilterLookup[datetime] | None = strawberry_django.filter_field()
    interval: (
        Annotated["IntegerLookup", strawberry.lazy("netbox.graphql.filter_lookups")]
        | None
    ) = strawberry_django.filter_field()
    user: Annotated[
        "UserFilter", strawberry.lazy("users.graphql.filters")
    ] | None = strawberry_django.filter_field()


@strawberry_django.filter(models.ForwardIngestion, lookups=True)
class ForwardIngestionFilter(BaseFilterMixin):
    id: ID | None = strawberry_django.filter_field()
    sync: (
        Annotated[
            "ForwardSyncFilter", strawberry.lazy("forward_netbox.graphql.filters")
        ]
        | None
    ) = strawberry_django.filter_field()
    job: (
        Annotated["JobFilter", strawberry.lazy("forward_netbox.graphql.filters")]
        | None
    ) = strawberry_django.filter_field()
    branch: (
        Annotated["BranchFilter", strawberry.lazy("forward_netbox.graphql.filters")]
        | None
    ) = strawberry_django.filter_field()


@strawberry_django.filter(models.ForwardIngestionIssue, lookups=True)
class ForwardIngestionIssueFilter(BaseFilterMixin):
    id: ID | None = strawberry_django.filter_field()
    ingestion: (
        Annotated[
            "ForwardIngestionFilter",
            strawberry.lazy("forward_netbox.graphql.filters"),
        ]
        | None
    ) = strawberry_django.filter_field()
    timestamp: DatetimeFilterLookup[datetime] | None = strawberry_django.filter_field()
    model: FilterLookup[str] | None = strawberry_django.filter_field()
    message: FilterLookup[str] | None = strawberry_django.filter_field()
    raw_data: FilterLookup[str] | None = strawberry_django.filter_field()
    coalesce_fields: FilterLookup[str] | None = strawberry_django.filter_field()
    defaults: FilterLookup[str] | None = strawberry_django.filter_field()
    exception: FilterLookup[str] | None = strawberry_django.filter_field()


@strawberry_django.filter(models.ForwardData, lookups=True)
class ForwardDataFilter(BaseFilterMixin):
    id: ID | None = strawberry_django.filter_field()
    snapshot_data: (
        Annotated[
            "ForwardSnapshotFilter", strawberry.lazy("forward_netbox.graphql.filters")
        ]
        | None
    ) = strawberry_django.filter_field()
    data: (
        Annotated["JSONFilter", strawberry.lazy("netbox.graphql.filter_lookups")] | None
    ) = strawberry_django.filter_field()
    type: (
        Annotated[
            "ForwardRawDataTypeEnum", strawberry.lazy("forward_netbox.graphql.enums")
        ]
        | None
    ) = strawberry_django.filter_field()


# These filters are not defined in the libs, so need to define them here
@strawberry_django.filter(Branch, lookups=True)
class BranchFilter(PrimaryModelFilterMixin):
    id: ID | None = strawberry_django.filter_field()
    name: FilterLookup[str] | None = strawberry_django.filter_field()
    owner: Annotated[
        "UserFilter", strawberry.lazy("users.graphql.filters")
    ] | None = strawberry_django.filter_field()
    schema_id: FilterLookup[str] | None = strawberry_django.filter_field()
    status: (
        Annotated["BranchStatusEnum", strawberry.lazy("forward_netbox.graphql.enums")]
        | None
    ) = strawberry_django.filter_field()
    applied_migrations: (
        Annotated["StringArrayLookup", strawberry.lazy("netbox.graphql.filter_lookups")]
        | None
    ) = strawberry_django.filter_field()
    last_sync: DatetimeFilterLookup[datetime] | None = strawberry_django.filter_field()
    merged_time: DatetimeFilterLookup[
        datetime
    ] | None = strawberry_django.filter_field()
    merged_by: (
        Annotated["UserFilter", strawberry.lazy("users.graphql.filters")] | None
    ) = strawberry_django.filter_field()


@strawberry_django.filter(Job, lookups=True)
class JobFilter(BaseFilterMixin):
    id: ID | None = strawberry_django.filter_field()
    object_type: (
        Annotated["ContentTypeFilter", strawberry.lazy("core.graphql.filters")] | None
    ) = strawberry_django.filter_field()
    object_id: (
        Annotated["IntegerLookup", strawberry.lazy("netbox.graphql.filter_lookups")]
        | None
    ) = strawberry_django.filter_field()
    name: FilterLookup[str] | None = strawberry_django.filter_field()
    created: DatetimeFilterLookup[datetime] | None = strawberry_django.filter_field()
    scheduled: DatetimeFilterLookup[datetime] | None = strawberry_django.filter_field()
    interval: (
        Annotated["IntegerLookup", strawberry.lazy("netbox.graphql.filter_lookups")]
        | None
    ) = strawberry_django.filter_field()
    started: DatetimeFilterLookup[datetime] | None = strawberry_django.filter_field()
    completed: DatetimeFilterLookup[datetime] | None = strawberry_django.filter_field()
    user: Annotated[
        "UserFilter", strawberry.lazy("users.graphql.filters")
    ] | None = strawberry_django.filter_field()
    status: (
        Annotated["JobStatusEnum", strawberry.lazy("forward_netbox.graphql.enums")]
        | None
    ) = strawberry_django.filter_field()
    data: (
        Annotated["JSONFilter", strawberry.lazy("netbox.graphql.filter_lookups")] | None
    ) = strawberry_django.filter_field()
    error: FilterLookup[str] | None = strawberry_django.filter_field()
    job_id: FilterLookup[str] | None = strawberry_django.filter_field()
