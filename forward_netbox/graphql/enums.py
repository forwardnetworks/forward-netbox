import strawberry
from core.choices import DataSourceStatusChoices
from core.choices import JobStatusChoices
from netbox_branching.choices import BranchStatusChoices

from forward_netbox.choices import ForwardRawDataTypeChoices
from forward_netbox.choices import ForwardSnapshotStatusModelChoices

__all__ = (
    "DataSourceStatusEnum",
    "ForwardSnapshotStatusModelEnum",
    "ForwardRawDataTypeEnum",
    "BranchStatusEnum",
    "JobStatusEnum",
)

DataSourceStatusEnum = strawberry.enum(DataSourceStatusChoices.as_enum(prefix="type"))
ForwardSnapshotStatusModelEnum = strawberry.enum(
    ForwardSnapshotStatusModelChoices.as_enum(prefix="type")
)
ForwardRawDataTypeEnum = strawberry.enum(
    ForwardRawDataTypeChoices.as_enum(prefix="type")
)
BranchStatusEnum = strawberry.enum(BranchStatusChoices.as_enum(prefix="type"))
JobStatusEnum = strawberry.enum(JobStatusChoices.as_enum(prefix="type"))
