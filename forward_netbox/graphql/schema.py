import strawberry
import strawberry_django

from .types import ForwardDataType
from .types import ForwardIngestionIssueType
from .types import ForwardIngestionType
from .types import ForwardRelationshipFieldType
from .types import ForwardSnapshotType
from .types import ForwardSourceType
from .types import ForwardSyncType
from .types import ForwardTransformFieldType
from .types import ForwardTransformMapGroupType
from .types import ForwardTransformMapType


__all__ = (
    "ForwardTransformMapGroupQuery",
    "ForwardTransformMapQuery",
    "ForwardSyncQuery",
    "ForwardTransformFieldQuery",
    "ForwardRelationshipFieldQuery",
    "ForwardSourceQuery",
    "ForwardSnapshotQuery",
    "ForwardIngestionQuery",
    "ForwardIngestionIssueQuery",
    "ForwardDataQuery",
)


@strawberry.type(name="Query")
class ForwardTransformMapGroupQuery:
    forward_transform_map_group: ForwardTransformMapGroupType = (
        strawberry_django.field()
    )
    forward_transform_map_group_list: list[
        ForwardTransformMapGroupType
    ] = strawberry_django.field()


@strawberry.type(name="Query")
class ForwardTransformMapQuery:
    forward_transform_map: ForwardTransformMapType = strawberry_django.field()
    forward_transform_map_list: list[
        ForwardTransformMapType
    ] = strawberry_django.field()


@strawberry.type(name="Query")
class ForwardTransformFieldQuery:
    forward_transform_field: ForwardTransformFieldType = strawberry_django.field()
    forward_transform_field_list: list[
        ForwardTransformFieldType
    ] = strawberry_django.field()


@strawberry.type(name="Query")
class ForwardRelationshipFieldQuery:
    forward_relationship_field: ForwardRelationshipFieldType = (
        strawberry_django.field()
    )
    forward_relationship_field_list: list[
        ForwardRelationshipFieldType
    ] = strawberry_django.field()


@strawberry.type(name="Query")
class ForwardSourceQuery:
    forward_source: ForwardSourceType = strawberry_django.field()
    forward_source_list: list[ForwardSourceType] = strawberry_django.field()


@strawberry.type(name="Query")
class ForwardSnapshotQuery:
    forward_snapshot: ForwardSnapshotType = strawberry_django.field()
    forward_snapshot_list: list[ForwardSnapshotType] = strawberry_django.field()


@strawberry.type(name="Query")
class ForwardSyncQuery:
    forward_sync: ForwardSyncType = strawberry_django.field()
    forward_sync_list: list[ForwardSyncType] = strawberry_django.field()


@strawberry.type(name="Query")
class ForwardIngestionQuery:
    forward_ingestion: ForwardIngestionType = strawberry_django.field()
    forward_ingestion_list: list[ForwardIngestionType] = strawberry_django.field()


@strawberry.type(name="Query")
class ForwardIngestionIssueQuery:
    forward_ingestion_issue: ForwardIngestionIssueType = strawberry_django.field()
    forward_ingestion_issue_list: list[
        ForwardIngestionIssueType
    ] = strawberry_django.field()


@strawberry.type(name="Query")
class ForwardDataQuery:
    forward_data: ForwardDataType = strawberry_django.field()
    forward_data_list: list[ForwardDataType] = strawberry_django.field()
