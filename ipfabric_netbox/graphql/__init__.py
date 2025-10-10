from .schema import IPFabricDataQuery
from .schema import IPFabricIngestionIssueQuery
from .schema import IPFabricIngestionQuery
from .schema import IPFabricRelationshipFieldQuery
from .schema import IPFabricSnapshotQuery
from .schema import IPFabricSourceQuery
from .schema import IPFabricSyncQuery
from .schema import IPFabricTransformFieldQuery
from .schema import IPFabricTransformMapGroupQuery
from .schema import IPFabricTransformMapQuery

schema = [
    IPFabricTransformMapGroupQuery,
    IPFabricTransformMapQuery,
    IPFabricTransformFieldQuery,
    IPFabricRelationshipFieldQuery,
    IPFabricSourceQuery,
    IPFabricSnapshotQuery,
    IPFabricSyncQuery,
    IPFabricIngestionQuery,
    IPFabricIngestionIssueQuery,
    IPFabricDataQuery,
]
