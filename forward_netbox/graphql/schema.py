import strawberry
import strawberry_django

from .types import ForwardDataType
from .types import ForwardIngestionIssueType
from .types import ForwardIngestionType
from .types import ForwardSnapshotType
from .types import ForwardSourceType
from .types import ForwardSyncType


__all__ = (
    "ForwardSyncQuery",
    "ForwardSourceQuery",
    "ForwardSnapshotQuery",
    "ForwardIngestionQuery",
    "ForwardIngestionIssueQuery",
    "ForwardDataQuery",
)


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
