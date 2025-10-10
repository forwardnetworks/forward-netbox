from .schema import ForwardDataQuery
from .schema import ForwardIngestionIssueQuery
from .schema import ForwardIngestionQuery
from .schema import ForwardRelationshipFieldQuery
from .schema import ForwardSnapshotQuery
from .schema import ForwardSourceQuery
from .schema import ForwardSyncQuery
from .schema import ForwardTransformFieldQuery
from .schema import ForwardTransformMapGroupQuery
from .schema import ForwardTransformMapQuery

schema = [
    ForwardTransformMapGroupQuery,
    ForwardTransformMapQuery,
    ForwardTransformFieldQuery,
    ForwardRelationshipFieldQuery,
    ForwardSourceQuery,
    ForwardSnapshotQuery,
    ForwardSyncQuery,
    ForwardIngestionQuery,
    ForwardIngestionIssueQuery,
    ForwardDataQuery,
]
