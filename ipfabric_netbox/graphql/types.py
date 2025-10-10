from typing import Annotated

import strawberry
import strawberry_django
from core.graphql.mixins import ChangelogMixin
from core.models import Job
from extras.graphql.mixins import TagsMixin
from netbox.graphql.types import BaseObjectType
from netbox.graphql.types import ContentTypeType
from netbox.graphql.types import NetBoxObjectType
from netbox.graphql.types import OrganizationalObjectType
from netbox_branching.models import Branch
from strawberry.scalars import JSON
from users.graphql.types import UserType

from .filters import BranchFilter
from .filters import IPFabricDataFilter
from .filters import IPFabricIngestionFilter
from .filters import IPFabricIngestionIssueFilter
from .filters import IPFabricRelationshipFieldFilter
from .filters import IPFabricSnapshotFilter
from .filters import IPFabricSourceFilter
from .filters import IPFabricSyncFilter
from .filters import IPFabricTransformFieldFilter
from .filters import IPFabricTransformMapFilter
from .filters import IPFabricTransformMapGroupFilter
from .filters import JobFilter
from ipfabric_netbox import models


__all__ = (
    "IPFabricTransformMapGroupType",
    "IPFabricTransformMapType",
    "IPFabricTransformFieldType",
    "IPFabricRelationshipFieldType",
    "IPFabricSourceType",
    "IPFabricSnapshotType",
    "IPFabricSyncType",
    "IPFabricIngestionType",
    "IPFabricIngestionIssueType",
    "IPFabricDataType",
)


@strawberry_django.type(
    models.IPFabricTransformMapGroup,
    fields="__all__",
    filters=IPFabricTransformMapGroupFilter,
)
class IPFabricTransformMapGroupType(NetBoxObjectType):
    name: str
    description: str | None


@strawberry_django.type(
    models.IPFabricTransformMap, fields="__all__", filters=IPFabricTransformMapFilter
)
class IPFabricTransformMapType(NetBoxObjectType):
    name: str
    source_model: str
    target_model: (
        Annotated[
            "ContentTypeType",
            strawberry.lazy("netbox.graphql.types"),
        ]
        | None
    )
    group: (
        Annotated[
            "IPFabricTransformMapGroupType",
            strawberry.lazy("ipfabric_netbox.graphql.types"),
        ]
        | None
    )


@strawberry_django.type(
    models.IPFabricTransformField,
    fields="__all__",
    filters=IPFabricTransformFieldFilter,
)
class IPFabricTransformFieldType(BaseObjectType):
    transform_map: (
        Annotated[
            "IPFabricTransformMapType", strawberry.lazy("ipfabric_netbox.graphql.types")
        ]
        | None
    )
    source_field: str
    target_field: str
    coalesce: bool
    template: str


@strawberry_django.type(
    models.IPFabricRelationshipField,
    fields="__all__",
    filters=IPFabricRelationshipFieldFilter,
)
class IPFabricRelationshipFieldType(BaseObjectType):
    transform_map: (
        Annotated[
            "IPFabricTransformMapType", strawberry.lazy("ipfabric_netbox.graphql.types")
        ]
        | None
    )
    source_model: Annotated["ContentTypeType", strawberry.lazy("netbox.graphql.types")]
    target_field: str
    coalesce: bool
    template: str


@strawberry_django.type(
    models.IPFabricSource, fields="__all__", filters=IPFabricSourceFilter
)
class IPFabricSourceType(OrganizationalObjectType):
    name: str
    type: str
    url: str
    status: str
    parameters: JSON
    last_synced: str


@strawberry_django.type(
    models.IPFabricSnapshot, fields="__all__", filters=IPFabricSnapshotFilter
)
class IPFabricSnapshotType(ChangelogMixin, TagsMixin, BaseObjectType):
    source: (
        Annotated[
            "IPFabricSourceType", strawberry.lazy("ipfabric_netbox.graphql.types")
        ]
        | None
    )
    name: str
    snapshot_id: str
    data: JSON
    status: str


@strawberry_django.type(
    models.IPFabricSync, fields="__all__", filters=IPFabricSyncFilter
)
class IPFabricSyncType(ChangelogMixin, TagsMixin, BaseObjectType):
    name: str
    snapshot_data: (
        Annotated[
            "IPFabricSnapshotType", strawberry.lazy("ipfabric_netbox.graphql.types")
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
    models.IPFabricIngestion, fields="__all__", filters=IPFabricIngestionFilter
)
class IPFabricIngestionType(BaseObjectType):
    sync: (
        Annotated["IPFabricSyncType", strawberry.lazy("ipfabric_netbox.graphql.types")]
        | None
    )
    job: Annotated["JobType", strawberry.lazy("ipfabric_netbox.graphql.types")] | None
    branch: (
        Annotated["BranchType", strawberry.lazy("ipfabric_netbox.graphql.types")] | None
    )


@strawberry_django.type(
    models.IPFabricIngestionIssue,
    fields="__all__",
    filters=IPFabricIngestionIssueFilter,
)
class IPFabricIngestionIssueType(BaseObjectType):
    ingestion: (
        Annotated[
            "IPFabricIngestionType", strawberry.lazy("ipfabric_netbox.graphql.types")
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
    models.IPFabricData, fields="__all__", filters=IPFabricDataFilter
)
class IPFabricDataType(BaseObjectType):
    snapshot_data: Annotated[
        "IPFabricSnapshotType", strawberry.lazy("ipfabric_netbox.graphql.types")
    ]
