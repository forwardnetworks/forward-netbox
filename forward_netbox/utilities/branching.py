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
