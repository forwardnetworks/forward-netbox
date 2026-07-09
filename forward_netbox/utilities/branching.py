from uuid import uuid4

from utilities.request import NetBoxFakeRequest


def missing_branch_table_report():
    """Map app_label -> branch-replicated tables missing from the database.

    Django registers ContentTypes for every installed app even when that app's
    migrations were never applied, so netbox_branching's provision happily runs
    ``CREATE TABLE branch.T (LIKE public.T)`` for a table that does not exist
    and the sync dies mid-provision with an opaque ProgrammingError (field
    report: a plugin installed without running its migrations). Detect the gap
    up front using the exact table list provision will replicate.
    """
    from django.apps import apps
    from django.db import connection
    from netbox_branching.utilities import get_tables_to_replicate

    missing = set(get_tables_to_replicate()) - set(
        connection.introspection.table_names()
    )
    if not missing:
        return {}
    by_app: dict[str, set[str]] = {}
    for model in apps.get_models(include_auto_created=True):
        table = model._meta.db_table
        if table in missing:
            by_app.setdefault(model._meta.app_label, set()).add(table)
    mapped = set().union(*by_app.values()) if by_app else set()
    for table in missing - mapped:
        by_app.setdefault("unknown", set()).add(table)
    return {app: sorted(tables) for app, tables in sorted(by_app.items())}


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
