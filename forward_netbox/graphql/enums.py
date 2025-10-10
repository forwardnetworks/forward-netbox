import strawberry
from core.choices import DataSourceStatusChoices
from core.choices import JobStatusChoices
from netbox_branching.choices import BranchStatusChoices

from forward_netbox.choices import ForwardRawDataTypeChoices
from forward_netbox.choices import ForwardSnapshotStatusModelChoices
from forward_netbox.choices import ForwardSourceTypeChoices
from forward_netbox.choices import ForwardTransformMapSourceModelChoices

__all__ = (
    "DataSourceStatusEnum",
    "ForwardTransformMapSourceModelEnum",
    "ForwardSourceTypeEnum",
    "ForwardSnapshotStatusModelEnum",
    "ForwardRawDataTypeEnum",
    "BranchStatusEnum",
    "JobStatusEnum",
)

DataSourceStatusEnum = strawberry.enum(DataSourceStatusChoices.as_enum(prefix="type"))
ForwardTransformMapSourceModelEnum = strawberry.enum(
    ForwardTransformMapSourceModelChoices.as_enum(prefix="type")
)
ForwardSourceTypeEnum = strawberry.enum(
    ForwardSourceTypeChoices.as_enum(prefix="type")
)
ForwardSnapshotStatusModelEnum = strawberry.enum(
    ForwardSnapshotStatusModelChoices.as_enum(prefix="type")
)
ForwardRawDataTypeEnum = strawberry.enum(
    ForwardRawDataTypeChoices.as_enum(prefix="type")
)
BranchStatusEnum = strawberry.enum(BranchStatusChoices.as_enum(prefix="type"))
JobStatusEnum = strawberry.enum(JobStatusChoices.as_enum(prefix="type"))
