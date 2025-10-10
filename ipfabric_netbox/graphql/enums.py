import strawberry
from core.choices import DataSourceStatusChoices
from core.choices import JobStatusChoices
from netbox_branching.choices import BranchStatusChoices

from ipfabric_netbox.choices import IPFabricRawDataTypeChoices
from ipfabric_netbox.choices import IPFabricSnapshotStatusModelChoices
from ipfabric_netbox.choices import IPFabricSourceTypeChoices
from ipfabric_netbox.choices import IPFabricTransformMapSourceModelChoices

__all__ = (
    "DataSourceStatusEnum",
    "IPFabricTransformMapSourceModelEnum",
    "IPFabricSourceTypeEnum",
    "IPFabricSnapshotStatusModelEnum",
    "IPFabricRawDataTypeEnum",
    "BranchStatusEnum",
    "JobStatusEnum",
)

DataSourceStatusEnum = strawberry.enum(DataSourceStatusChoices.as_enum(prefix="type"))
IPFabricTransformMapSourceModelEnum = strawberry.enum(
    IPFabricTransformMapSourceModelChoices.as_enum(prefix="type")
)
IPFabricSourceTypeEnum = strawberry.enum(
    IPFabricSourceTypeChoices.as_enum(prefix="type")
)
IPFabricSnapshotStatusModelEnum = strawberry.enum(
    IPFabricSnapshotStatusModelChoices.as_enum(prefix="type")
)
IPFabricRawDataTypeEnum = strawberry.enum(
    IPFabricRawDataTypeChoices.as_enum(prefix="type")
)
BranchStatusEnum = strawberry.enum(BranchStatusChoices.as_enum(prefix="type"))
JobStatusEnum = strawberry.enum(JobStatusChoices.as_enum(prefix="type"))
