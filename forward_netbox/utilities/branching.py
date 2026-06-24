from uuid import uuid4

from utilities.request import NetBoxFakeRequest


def build_branch_name(*, sync, ingestion, item):
    sync_id = getattr(sync, "pk", None)
    ingestion_id = getattr(ingestion, "pk", None)
    item_index = getattr(item, "index", None)
    model_string = getattr(item, "model_string", "")
    return (
        f"Forward Sync {sync_id} - ingestion {ingestion_id} "
        f"- part {item_index} {model_string}"
    )


def build_branch_request(user):
    if user is None:
        # Branch staging records change-logging ObjectChanges, which require a
        # request user; without one NetBox fails deep in a per-object save with an
        # opaque `AttributeError: 'NoneType' has no attribute 'username'`. Fail
        # fast with an actionable message. Production runs always carry the job
        # user; this guards adhoc/programmatic calls that forget to set one.
        from core.exceptions import SyncError

        raise SyncError(
            "Forward sync requires a user for change attribution. Run it via a "
            "queued or scheduled job (which carries the job user), or set the "
            "sync's user."
        )
    return NetBoxFakeRequest(
        {
            "id": uuid4(),
            "user": user,
            "META": {},
            "COOKIES": {},
            "POST": {},
            "GET": {},
            "FILES": {},
            "method": "POST",
            "path": "",
        }
    )
