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

from ipfabric_netbox import models
from ipfabric_netbox.graphql.enums import BranchStatusEnum
from ipfabric_netbox.graphql.enums import DataSourceStatusEnum
from ipfabric_netbox.graphql.enums import IPFabricRawDataTypeEnum
from ipfabric_netbox.graphql.enums import IPFabricSnapshotStatusModelEnum
from ipfabric_netbox.graphql.enums import IPFabricSourceTypeEnum
from ipfabric_netbox.graphql.enums import IPFabricTransformMapSourceModelEnum
from ipfabric_netbox.graphql.enums import JobStatusEnum

__all__ = (
    "IPFabricTransformMapGroupFilter",
    "IPFabricTransformMapFilter",
    "IPFabricTransformFieldFilter",
    "IPFabricRelationshipFieldFilter",
    "IPFabricSourceFilter",
    "IPFabricSnapshotFilter",
    "IPFabricSyncFilter",
    "IPFabricIngestionFilter",
    "IPFabricIngestionIssueFilter",
    "IPFabricDataFilter",
    "BranchFilter",
    "JobFilter",
)


@strawberry_django.filter(models.IPFabricTransformMapGroup, lookups=True)
class IPFabricTransformMapGroupFilter(NetBoxModelFilterMixin):
    id: ID | None = strawberry_django.filter_field()
    name: FilterLookup[str] | None = strawberry_django.filter_field()
    description: FilterLookup[str] | None = strawberry_django.filter_field()


@strawberry_django.filter(models.IPFabricTransformMap, lookups=True)
class IPFabricTransformMapFilter(NetBoxModelFilterMixin):
    id: ID | None = strawberry_django.filter_field()
    name: FilterLookup[str] | None = strawberry_django.filter_field()
    source_model: (
        Annotated[
            "IPFabricTransformMapSourceModelEnum",
            strawberry.lazy("ipfabric_netbox.graphql.enums"),
        ]
        | None
    ) = strawberry_django.filter_field()
    target_model: (
        Annotated["ContentTypeFilter", strawberry.lazy("core.graphql.filters")] | None
    ) = strawberry_django.filter_field()
    group: (
        Annotated[
            "IPFabricTransformMapGroupFilter",
            strawberry.lazy("ipfabric_netbox.graphql.filters"),
        ]
        | None
    ) = strawberry_django.filter_field()


@strawberry_django.filter(models.IPFabricTransformField, lookups=True)
class IPFabricTransformFieldFilter(BaseFilterMixin):
    id: ID | None = strawberry_django.filter_field()
    transform_map: (
        Annotated[
            "IPFabricTransformMapFilter",
            strawberry.lazy("ipfabric_netbox.graphql.filters"),
        ]
        | None
    ) = strawberry_django.filter_field()
    source_field: FilterLookup[str] | None = strawberry_django.filter_field()
    target_field: FilterLookup[str] | None = strawberry_django.filter_field()
    coalesce: (
        Annotated["IntegerLookup", strawberry.lazy("netbox.graphql.filter_lookups")]
        | None
    ) = strawberry_django.filter_field()
    template: FilterLookup[str] | None = strawberry_django.filter_field()


@strawberry_django.filter(models.IPFabricRelationshipField, lookups=True)
class IPFabricRelationshipFieldFilter(BaseFilterMixin):
    id: ID | None = strawberry_django.filter_field()
    transform_map: (
        Annotated[
            "IPFabricTransformMapFilter",
            strawberry.lazy("ipfabric_netbox.graphql.filters"),
        ]
        | None
    ) = strawberry_django.filter_field()
    source_model: (
        Annotated["ContentTypeFilter", strawberry.lazy("core.graphql.filters")] | None
    ) = strawberry_django.filter_field()
    target_field: FilterLookup[str] | None = strawberry_django.filter_field()
    coalesce: (
        Annotated["IntegerLookup", strawberry.lazy("netbox.graphql.filter_lookups")]
        | None
    ) = strawberry_django.filter_field()
    template: FilterLookup[str] | None = strawberry_django.filter_field()


@strawberry_django.filter(models.IPFabricSource, lookups=True)
class IPFabricSourceFilter(PrimaryModelFilterMixin):
    id: ID | None = strawberry_django.filter_field()
    name: FilterLookup[str] | None = strawberry_django.filter_field()
    type: (
        Annotated[
            "IPFabricSourceTypeEnum", strawberry.lazy("ipfabric_netbox.graphql.enums")
        ]
        | None
    ) = strawberry_django.filter_field()
    url: FilterLookup[str] | None = strawberry_django.filter_field()
    status: (
        Annotated[
            "DataSourceStatusEnum", strawberry.lazy("ipfabric_netbox.graphql.enums")
        ]
        | None
    ) = strawberry_django.filter_field()
    parameters: (
        Annotated["JSONFilter", strawberry.lazy("netbox.graphql.filter_lookups")] | None
    ) = strawberry_django.filter_field()
    last_synced: DatetimeFilterLookup[
        datetime
    ] | None = strawberry_django.filter_field()


@strawberry_django.filter(models.IPFabricSnapshot, lookups=True)
class IPFabricSnapshotFilter(TagsFilterMixin, ChangeLogFilterMixin, BaseFilterMixin):
    id: ID | None = strawberry_django.filter_field()
    created: DatetimeFilterLookup[datetime] | None = strawberry_django.filter_field()
    last_updated: DatetimeFilterLookup[
        datetime
    ] | None = strawberry_django.filter_field()
    source: (
        Annotated[
            "IPFabricSourceFilter", strawberry.lazy("ipfabric_netbox.graphql.filters")
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
            "IPFabricSnapshotStatusModelEnum",
            strawberry.lazy("ipfabric_netbox.graphql.enums"),
        ]
        | None
    ) = strawberry_django.filter_field()


@strawberry_django.filter(models.IPFabricSync, lookups=True)
class IPFabricSyncFilter(TagsFilterMixin, ChangeLogFilterMixin, BaseFilterMixin):
    id: ID | None = strawberry_django.filter_field()
    name: FilterLookup[str] | None = strawberry_django.filter_field()
    snapshot_data: (
        Annotated[
            "IPFabricSnapshotFilter", strawberry.lazy("ipfabric_netbox.graphql.filters")
        ]
        | None
    ) = strawberry_django.filter_field()
    status: (
        Annotated[
            "DataSourceStatusEnum", strawberry.lazy("ipfabric_netbox.graphql.enums")
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


@strawberry_django.filter(models.IPFabricIngestion, lookups=True)
class IPFabricIngestionFilter(BaseFilterMixin):
    id: ID | None = strawberry_django.filter_field()
    sync: (
        Annotated[
            "IPFabricSyncFilter", strawberry.lazy("ipfabric_netbox.graphql.filters")
        ]
        | None
    ) = strawberry_django.filter_field()
    job: (
        Annotated["JobFilter", strawberry.lazy("ipfabric_netbox.graphql.filters")]
        | None
    ) = strawberry_django.filter_field()
    branch: (
        Annotated["BranchFilter", strawberry.lazy("ipfabric_netbox.graphql.filters")]
        | None
    ) = strawberry_django.filter_field()


@strawberry_django.filter(models.IPFabricIngestionIssue, lookups=True)
class IPFabricIngestionIssueFilter(BaseFilterMixin):
    id: ID | None = strawberry_django.filter_field()
    ingestion: (
        Annotated[
            "IPFabricIngestionFilter",
            strawberry.lazy("ipfabric_netbox.graphql.filters"),
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


@strawberry_django.filter(models.IPFabricData, lookups=True)
class IPFabricDataFilter(BaseFilterMixin):
    id: ID | None = strawberry_django.filter_field()
    snapshot_data: (
        Annotated[
            "IPFabricSnapshotFilter", strawberry.lazy("ipfabric_netbox.graphql.filters")
        ]
        | None
    ) = strawberry_django.filter_field()
    data: (
        Annotated["JSONFilter", strawberry.lazy("netbox.graphql.filter_lookups")] | None
    ) = strawberry_django.filter_field()
    type: (
        Annotated[
            "IPFabricRawDataTypeEnum", strawberry.lazy("ipfabric_netbox.graphql.enums")
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
        Annotated["BranchStatusEnum", strawberry.lazy("ipfabric_netbox.graphql.enums")]
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
        Annotated["JobStatusEnum", strawberry.lazy("ipfabric_netbox.graphql.enums")]
        | None
    ) = strawberry_django.filter_field()
    data: (
        Annotated["JSONFilter", strawberry.lazy("netbox.graphql.filter_lookups")] | None
    ) = strawberry_django.filter_field()
    error: FilterLookup[str] | None = strawberry_django.filter_field()
    job_id: FilterLookup[str] | None = strawberry_django.filter_field()
