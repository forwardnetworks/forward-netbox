from .schema import ForwardDataQuery
from .schema import ForwardIngestionIssueQuery
from .schema import ForwardIngestionQuery
from .schema import ForwardNQEQueryQuery
from .schema import ForwardSnapshotQuery
from .schema import ForwardSourceQuery
from .schema import ForwardSyncQuery

schema = [
    ForwardNQEQueryQuery,
    ForwardSourceQuery,
    ForwardSnapshotQuery,
    ForwardSyncQuery,
    ForwardIngestionQuery,
    ForwardIngestionIssueQuery,
    ForwardDataQuery,
]
