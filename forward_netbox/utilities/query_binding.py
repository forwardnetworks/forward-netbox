from dataclasses import dataclass

from django.db import transaction

from ..models import ForwardNQEMap
from .query_registry import BUILTIN_SEEDED_QUERY_MAPS
from .query_registry import read_builtin_query_source
from .query_registry import read_compiled_builtin_query_source


@dataclass(frozen=True)
class NQEMapBinding:
    model_string: str
    query_name: str
    query_filename: str
    query_path: str
    query_repository: str
    query_id: str = ""
    commit_id: str = ""
    map_id: int | None = None
    matched: bool = False
    skipped_reason: str = ""


def builtin_filename_to_query_default() -> dict[str, dict]:
    return {
        str(query_default["filename"]): query_default
        for query_default in BUILTIN_SEEDED_QUERY_MAPS
    }


def builtin_query_defaults_by_model() -> dict[str, list[dict]]:
    query_defaults_by_model: dict[str, list[dict]] = {}
    for query_default in BUILTIN_SEEDED_QUERY_MAPS:
        query_defaults_by_model.setdefault(
            str(query_default["model_string"]),
            [],
        ).append(query_default)
    return query_defaults_by_model


def query_filename_from_path(query_path: str) -> str:
    return f"{str(query_path).rstrip('/').rsplit('/', 1)[-1]}.nqe"


def query_path_from_filename(directory: str, filename: str) -> str:
    directory = str(directory or "/").strip() or "/"
    if not directory.startswith("/"):
        directory = f"/{directory}"
    directory = directory.rstrip("/")
    query_name = str(filename).removesuffix(".nqe")
    return f"{directory}/{query_name}" if directory else f"/{query_name}"


def normalize_query_source(query: str) -> str:
    return "\n".join(line.rstrip() for line in str(query).strip().splitlines()).strip()


def binding_matches_current_reference(
    query_map: ForwardNQEMap,
    binding: NQEMapBinding,
) -> bool:
    if query_map.query_path:
        return (
            query_map.query_path == binding.query_path
            or query_filename_from_path(query_map.query_path) == binding.query_filename
        )
    if query_map.query_id and query_map.query_id == binding.query_id:
        return True
    if query_map.query:
        return normalize_query_source(query_map.query) == normalize_query_source(
            read_builtin_query_source(binding.query_filename)
        )
    return False


def builtin_query_default_for_map(query_map: ForwardNQEMap) -> tuple[dict | None, str]:
    filename_to_query_default = builtin_filename_to_query_default()
    query_defaults_by_model = builtin_query_defaults_by_model()

    if query_map.query_path:
        query_filename = query_filename_from_path(query_map.query_path)
        query_default = filename_to_query_default.get(query_filename)
        if (
            query_default
            and str(query_default["model_string"]) == query_map.model_string
        ):
            return query_default, ""
        return None, "Current repository query path does not match a bundled map."

    if query_map.query:
        current_query = normalize_query_source(query_map.query)
        source_matches = [
            query_default
            for query_default in query_defaults_by_model.get(query_map.model_string, [])
            if normalize_query_source(
                read_builtin_query_source(str(query_default["filename"]))
            )
            == current_query
        ]
        if len(source_matches) == 1:
            return source_matches[0], ""

    name_matches = [
        query_default
        for query_default in query_defaults_by_model.get(query_map.model_string, [])
        if str(query_default["name"]) == query_map.name
    ]
    if len(name_matches) == 1:
        return name_matches[0], ""

    model_matches = query_defaults_by_model.get(query_map.model_string, [])
    if len(model_matches) == 1:
        return model_matches[0], ""

    if model_matches:
        return (
            None,
            "Multiple bundled queries target this NetBox model; restore it "
            "individually or bind it by repository path first.",
        )
    return None, "No bundled query targets this NetBox model."


def build_nqe_map_bindings(
    *,
    client,
    repository: str,
    directory: str,
    pin_commit: bool = False,
) -> list[NQEMapBinding]:
    filename_to_query_default = builtin_filename_to_query_default()
    bindings = []
    for query in client.get_nqe_repository_queries(
        repository=repository,
        directory=directory,
    ):
        query_path = str(query.get("path") or "").strip()
        query_id = str(query.get("queryId") or "").strip()
        if not query_path:
            continue
        query_filename = query_filename_from_path(query_path)
        query_default = filename_to_query_default.get(query_filename)
        if not query_default:
            continue
        bindings.append(
            NQEMapBinding(
                model_string=str(query_default["model_string"]),
                query_name=str(query_default["name"]),
                query_filename=query_filename,
                query_path=query_path,
                query_repository=repository,
                query_id=query_id,
                commit_id=(
                    str(query.get("lastCommitId") or "").strip() if pin_commit else ""
                ),
            )
        )
    return bindings


def _committed_query_by_path(client, query_path: str, existing_query: dict | None):
    if (
        existing_query
        and existing_query.get("queryId")
        and existing_query.get("lastCommitId")
    ):
        return existing_query
    try:
        query = client.get_committed_nqe_query(
            repository="org",
            query_path=query_path,
            commit_id="head",
        )
    except Exception:
        return existing_query or {}
    last_commit = query.get("lastCommit") or {}
    return {
        "queryId": str(query.get("queryId") or "").strip(),
        "lastCommitId": str(
            query.get("lastCommitId") or last_commit.get("id") or ""
        ).strip(),
        "path": str(query.get("path") or query_path).strip(),
    }


def publish_builtin_nqe_map_queries(
    *,
    client,
    directory: str,
    queryset=None,
    overwrite: bool = False,
    commit_message: str = "",
    pin_commit: bool = False,
) -> list[NQEMapBinding]:
    queryset = (
        queryset
        if queryset is not None
        else ForwardNQEMap.objects.select_related("netbox_model")
    )
    selected_maps = list(queryset.select_related("netbox_model"))
    map_query_paths = {}
    publish_filenames = []
    results = []
    for query_map in selected_maps:
        query_default, skipped_reason = builtin_query_default_for_map(query_map)
        if query_default is None:
            results.append(
                NQEMapBinding(
                    model_string=query_map.model_string,
                    query_name=query_map.name,
                    query_filename="",
                    query_path="",
                    query_repository="org",
                    map_id=query_map.pk,
                    skipped_reason=skipped_reason,
                )
            )
            continue
        filename = str(query_default["filename"])
        map_query_paths[query_map.pk] = query_path_from_filename(directory, filename)
        if filename not in publish_filenames:
            publish_filenames.append(filename)

    if not map_query_paths:
        return results

    existing_by_path = {
        str(query.get("path") or "").strip(): query
        for query in client.get_nqe_repository_queries(
            repository="org",
            directory=directory,
        )
    }
    changed_paths = []
    for filename in publish_filenames:
        query_path = query_path_from_filename(directory, filename)
        source_code = read_compiled_builtin_query_source(filename)
        existing_query = existing_by_path.get(query_path)
        if existing_query and not overwrite:
            continue
        if existing_query:
            committed_query = _committed_query_by_path(
                client,
                query_path,
                existing_query,
            )
            client.edit_org_nqe_query(
                query_path=query_path,
                source_code=source_code,
                query_id=committed_query.get("queryId"),
                commit_id=committed_query.get("lastCommitId"),
            )
        else:
            client.add_org_nqe_query(query_path=query_path, source_code=source_code)
        changed_paths.append(query_path)

    commit_id = ""
    if changed_paths:
        commit_id = client.commit_org_nqe_queries(
            query_paths=changed_paths,
            message=commit_message,
        )

    bindings = build_nqe_map_bindings(
        client=client,
        repository="org",
        directory=directory,
        pin_commit=pin_commit,
    )
    if commit_id and pin_commit:
        bindings = [
            NQEMapBinding(
                model_string=binding.model_string,
                query_name=binding.query_name,
                query_filename=binding.query_filename,
                query_path=binding.query_path,
                query_repository=binding.query_repository,
                query_id=binding.query_id,
                commit_id=binding.commit_id or commit_id,
            )
            for binding in bindings
        ]
    return [
        *results,
        *apply_explicit_nqe_map_bindings(
            bindings,
            query_path_by_map_id=map_query_paths,
            queryset=ForwardNQEMap.objects.filter(
                pk__in=map_query_paths.keys()
            ).select_related("netbox_model"),
        ),
    ]


@transaction.atomic
def apply_nqe_map_bindings(
    bindings: list[NQEMapBinding],
    *,
    queryset=None,
) -> list[NQEMapBinding]:
    queryset = (
        queryset
        if queryset is not None
        else ForwardNQEMap.objects.select_related("netbox_model")
    )
    bindings_by_model: dict[str, list[NQEMapBinding]] = {}
    for binding in bindings:
        bindings_by_model.setdefault(binding.model_string, []).append(binding)

    applied = []
    for query_map in queryset.select_related("netbox_model"):
        candidates = bindings_by_model.get(query_map.model_string, [])
        if not candidates:
            applied.append(
                NQEMapBinding(
                    model_string=query_map.model_string,
                    query_name=query_map.name,
                    query_filename="",
                    query_path="",
                    query_repository="",
                    map_id=query_map.pk,
                    skipped_reason="No repository query matched this NetBox model.",
                )
            )
            continue

        if len(candidates) == 1:
            binding = candidates[0]
        else:
            reference_matches = [
                candidate
                for candidate in candidates
                if binding_matches_current_reference(query_map, candidate)
            ]
            if len(reference_matches) != 1:
                applied.append(
                    NQEMapBinding(
                        model_string=query_map.model_string,
                        query_name=query_map.name,
                        query_filename="",
                        query_path="",
                        query_repository="",
                        map_id=query_map.pk,
                        skipped_reason=(
                            "Multiple repository queries matched this NetBox model; "
                            "edit the map individually or pre-bind it by path."
                        ),
                    )
                )
                continue
            binding = reference_matches[0]

        query_map.query_id = ""
        query_map.query_repository = binding.query_repository
        query_map.query_path = binding.query_path
        query_map.query = ""
        query_map.commit_id = binding.commit_id
        query_map.full_clean()
        query_map.save(
            update_fields=[
                "query_id",
                "query_repository",
                "query_path",
                "query",
                "commit_id",
            ]
        )
        applied.append(
            NQEMapBinding(
                model_string=binding.model_string,
                query_name=binding.query_name,
                query_filename=binding.query_filename,
                query_path=binding.query_path,
                query_repository=binding.query_repository,
                query_id=binding.query_id,
                commit_id=binding.commit_id,
                map_id=query_map.pk,
                matched=True,
            )
        )
    return applied


@transaction.atomic
def restore_builtin_raw_query_bindings(*, queryset=None) -> list[NQEMapBinding]:
    queryset = (
        queryset
        if queryset is not None
        else ForwardNQEMap.objects.select_related("netbox_model")
    )
    restored = []
    for query_map in queryset.select_related("netbox_model"):
        query_default, skipped_reason = builtin_query_default_for_map(query_map)
        if query_default is None:
            restored.append(
                NQEMapBinding(
                    model_string=query_map.model_string,
                    query_name=query_map.name,
                    query_filename="",
                    query_path="",
                    query_repository="",
                    map_id=query_map.pk,
                    skipped_reason=skipped_reason,
                )
            )
            continue

        query_filename = str(query_default["filename"])
        query_map.query_id = ""
        query_map.query_repository = ""
        query_map.query_path = ""
        query_map.query = read_builtin_query_source(query_filename)
        query_map.commit_id = ""
        query_map.full_clean()
        query_map.save(
            update_fields=[
                "query_id",
                "query_repository",
                "query_path",
                "query",
                "commit_id",
            ]
        )
        restored.append(
            NQEMapBinding(
                model_string=str(query_default["model_string"]),
                query_name=str(query_default["name"]),
                query_filename=query_filename,
                query_path="",
                query_repository="",
                map_id=query_map.pk,
                matched=True,
            )
        )
    return restored


@transaction.atomic
def apply_explicit_nqe_map_bindings(
    bindings: list[NQEMapBinding],
    *,
    query_path_by_map_id: dict[int, str],
    queryset=None,
) -> list[NQEMapBinding]:
    queryset = (
        queryset
        if queryset is not None
        else ForwardNQEMap.objects.select_related("netbox_model")
    )
    bindings_by_path = {binding.query_path: binding for binding in bindings}
    applied = []
    for query_map in queryset.select_related("netbox_model"):
        query_path = (query_path_by_map_id.get(query_map.pk) or "").strip()
        if not query_path:
            applied.append(
                NQEMapBinding(
                    model_string=query_map.model_string,
                    query_name=query_map.name,
                    query_filename="",
                    query_path="",
                    query_repository="",
                    map_id=query_map.pk,
                    skipped_reason="No repository query path was selected.",
                )
            )
            continue

        binding = bindings_by_path.get(query_path)
        if binding is None:
            applied.append(
                NQEMapBinding(
                    model_string=query_map.model_string,
                    query_name=query_map.name,
                    query_filename="",
                    query_path=query_path,
                    query_repository="",
                    map_id=query_map.pk,
                    skipped_reason="Selected query path was not found in the repository folder.",
                )
            )
            continue

        if binding.model_string != query_map.model_string:
            applied.append(
                NQEMapBinding(
                    model_string=query_map.model_string,
                    query_name=query_map.name,
                    query_filename=binding.query_filename,
                    query_path=binding.query_path,
                    query_repository=binding.query_repository,
                    map_id=query_map.pk,
                    skipped_reason=(
                        f"Selected query targets {binding.model_string}, "
                        f"not {query_map.model_string}."
                    ),
                )
            )
            continue

        query_map.query_id = ""
        query_map.query_repository = binding.query_repository
        query_map.query_path = binding.query_path
        query_map.query = ""
        query_map.commit_id = binding.commit_id
        query_map.full_clean()
        query_map.save(
            update_fields=[
                "query_id",
                "query_repository",
                "query_path",
                "query",
                "commit_id",
            ]
        )
        applied.append(
            NQEMapBinding(
                model_string=binding.model_string,
                query_name=binding.query_name,
                query_filename=binding.query_filename,
                query_path=binding.query_path,
                query_repository=binding.query_repository,
                query_id=binding.query_id,
                commit_id=binding.commit_id,
                map_id=query_map.pk,
                matched=True,
            )
        )
    return applied
