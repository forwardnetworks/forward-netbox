import hashlib
import json
import re
from collections import Counter
from concurrent.futures import Future
from dataclasses import dataclass
from dataclasses import field
from threading import Lock
from types import MappingProxyType
from typing import Any
from typing import Callable
from typing import Mapping


QUERY_CONTRACT_VERSION = 1
NORMALIZATION_VERSION = 1
IDENTITY_CONTRACT_VERSION = 1
REDUCER_VERSION = 1
SUPPORTED_DIFF_OWNERSHIP_MODES = {
    "global",
    "device",
    "cable_either_endpoint",
    "contributor_relation",
}

_QUERY_SIGNATURE_RE = re.compile(
    r"@query\s+(?:[A-Za-z_][A-Za-z0-9_]*\s*)?\((.*?)\)\s*=",
    flags=re.DOTALL,
)


def _canonical_value(value):
    if isinstance(value, Mapping):
        return {
            str(key): _canonical_value(item)
            for key, item in sorted(value.items(), key=lambda pair: str(pair[0]))
        }
    if isinstance(value, (set, frozenset)):
        return sorted((_canonical_value(item) for item in value), key=_sort_key)
    if isinstance(value, (list, tuple)):
        return [_canonical_value(item) for item in value]
    if value is None or isinstance(value, (bool, int, float, str)):
        return value
    return str(value)


def _sort_key(value):
    return json.dumps(value, sort_keys=True, separators=(",", ":"), default=str)


def canonical_sha256(value) -> str:
    encoded = json.dumps(
        _canonical_value(value),
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
        default=str,
    ).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def normalized_row_multiset(rows) -> Counter[str]:
    """Return an order-independent, duplicate-sensitive canonical row fixture."""

    return Counter(
        json.dumps(
            _canonical_value(row),
            sort_keys=True,
            separators=(",", ":"),
            ensure_ascii=False,
            default=str,
        )
        for row in rows
    )


def normalized_row_sets_match(expected_rows, actual_rows) -> bool:
    return normalized_row_multiset(expected_rows) == normalized_row_multiset(
        actual_rows
    )


def normalize_query_source(source: str | None) -> str:
    return str(source or "").replace("\r\n", "\n").replace("\r", "\n").strip()


def query_source_sha256(source: str | None) -> str:
    normalized = normalize_query_source(source)
    return hashlib.sha256(normalized.encode("utf-8")).hexdigest() if normalized else ""


@dataclass(frozen=True, order=True)
class DeclaredQueryParameter:
    name: str
    nqe_type: str


def declared_query_parameters(
    source: str | None,
) -> tuple[DeclaredQueryParameter, ...] | None:
    """Return the exact runnable-query declaration, or ``None`` if unverified."""

    normalized = normalize_query_source(source)
    if not normalized:
        return None
    matches = list(_QUERY_SIGNATURE_RE.finditer(normalized))
    if not matches:
        if re.search(r"@query\b", normalized):
            return None
        # Inline NQE without an explicit @query declaration is the implicit
        # parameterless runnable query. Helper functions may declare their own
        # arguments, but the external runtime contract still accepts none.
        # Multiple primary-key annotations are ambiguous/invalid provenance and
        # remain unverified rather than being treated as a runnable contract.
        primary_keys = re.findall(r"(?m)^\s*@primaryKey\s*\(", normalized)
        return () if len(primary_keys) <= 1 else None
    if len(matches) != 1:
        return None
    declaration = matches[0].group(1).strip()
    if not declaration:
        return ()

    parts = []
    start = 0
    depth = 0
    for index, character in enumerate(declaration):
        if character in "<([{":
            depth += 1
        elif character in ">)]}":
            depth = max(0, depth - 1)
        elif character == "," and depth == 0:
            parts.append(declaration[start:index])
            start = index + 1
    parts.append(declaration[start:])

    parameters = []
    for part in parts:
        if ":" not in part:
            return None
        name, nqe_type = part.split(":", 1)
        normalized_name = name.strip()
        normalized_type = " ".join(nqe_type.split())
        if not normalized_name or not normalized_type:
            return None
        parameters.append(
            DeclaredQueryParameter(
                name=normalized_name,
                nqe_type=normalized_type,
            )
        )
    if len({parameter.name for parameter in parameters}) != len(parameters):
        return None
    return tuple(parameters)


@dataclass(frozen=True)
class ResolvedQueryRevision:
    role: str
    query_id: str
    repository: str
    query_path: str
    commit_id: str
    source_sha256: str
    declared_parameters: tuple[DeclaredQueryParameter, ...] | None
    source_verified: bool

    @property
    def declarations_verified(self) -> bool:
        return self.declared_parameters is not None

    @property
    def fingerprint(self) -> str:
        return canonical_sha256(
            {
                "role": self.role,
                "query_id": self.query_id,
                "repository": self.repository,
                "query_path": self.query_path,
                "commit_id": self.commit_id,
                "source_sha256": self.source_sha256,
                "declared_parameters": (
                    [
                        {"name": parameter.name, "type": parameter.nqe_type}
                        for parameter in self.declared_parameters
                    ]
                    if self.declared_parameters is not None
                    else None
                ),
                "source_verified": self.source_verified,
            }
        )


@dataclass(frozen=True)
class ResolvedExecutionContract:
    spec: Any = field(compare=False, repr=False)
    model_string: str
    map_id: int | None
    map_weight: int
    contract_key: str
    query_name: str
    full_revision: ResolvedQueryRevision
    full_effective_parameters: Mapping[str, Any]
    diff_revision: ResolvedQueryRevision | None
    full_eligible: bool
    full_reason_code: str
    diff_reason_code: str
    coalesce_fields: tuple[tuple[str, ...], ...]
    variant: str
    data_file_hashes: tuple[tuple[str, str], ...]
    reducer_id: str
    reducer_version: int
    diff_ownership_mode: str
    normalization_version: int
    identity_version: int
    query_contract_version: int
    diff_eligible: bool
    reason_code: str

    @property
    def fingerprint(self) -> str:
        return canonical_sha256(
            {
                "model": self.model_string,
                "map_id": self.map_id,
                "map_weight": self.map_weight,
                "contract_key": self.contract_key,
                "query_name": self.query_name,
                "full_revision": self.full_revision.fingerprint,
                "full_effective_parameters": dict(self.full_effective_parameters),
                "diff_revision": (
                    self.diff_revision.fingerprint if self.diff_revision else None
                ),
                "coalesce_fields": self.coalesce_fields,
                "variant": self.variant,
                "data_file_hashes": self.data_file_hashes,
                "reducer_id": self.reducer_id,
                "reducer_version": self.reducer_version,
                "diff_ownership_mode": self.diff_ownership_mode,
                "normalization_version": self.normalization_version,
                "identity_version": self.identity_version,
                "query_contract_version": self.query_contract_version,
            }
        )


@dataclass(frozen=True)
class ResolvedModelExecutionContract:
    model_string: str
    maps: tuple[ResolvedExecutionContract, ...]
    map_set_fingerprint: str
    scope_config_fingerprint: str
    after_scope_membership_fingerprint: str
    diff_eligible: bool
    reason_code: str

    @property
    def execution_contract_fingerprint(self) -> str:
        return canonical_sha256(
            {
                "model": self.model_string,
                "map_set": self.map_set_fingerprint,
                "scope_config": self.scope_config_fingerprint,
                "normalization_versions": sorted(
                    {contract.normalization_version for contract in self.maps}
                ),
                "identity_versions": sorted(
                    {contract.identity_version for contract in self.maps}
                ),
                "reducer_versions": sorted(
                    {
                        (contract.reducer_id, contract.reducer_version)
                        for contract in self.maps
                    }
                ),
                "query_contract_versions": sorted(
                    {contract.query_contract_version for contract in self.maps}
                ),
            }
        )


def _expected_source_hash(spec, attribute: str) -> str:
    return str(getattr(spec, attribute, "") or "").strip().lower()


def _revision(
    *,
    spec,
    role: str,
    commit_id: str,
    source: str | None,
    expected_source_hash: str,
) -> ResolvedQueryRevision:
    source_hash = query_source_sha256(source)
    return ResolvedQueryRevision(
        role=role,
        query_id=str(getattr(spec, "run_query_id", "") or "").strip(),
        repository=str(getattr(spec, "query_repository", "") or "").strip(),
        query_path=str(
            getattr(spec, "resolved_query_path", "")
            or getattr(spec, "query_path", "")
            or ""
        ).strip(),
        commit_id=str(commit_id or "").strip(),
        source_sha256=source_hash or expected_source_hash,
        declared_parameters=declared_query_parameters(source),
        source_verified=bool(
            source_hash and expected_source_hash and source_hash == expected_source_hash
        ),
    )


def resolve_execution_contract(
    spec,
    *,
    effective_parameters: Mapping[str, Any] | None,
) -> ResolvedExecutionContract:
    full_source = getattr(spec, "full_query_source", None) or getattr(
        spec, "query", None
    )
    full_revision = _revision(
        spec=spec,
        role="full",
        commit_id=getattr(spec, "commit_id", "") or "",
        source=full_source,
        expected_source_hash=_expected_source_hash(spec, "full_source_sha256"),
    )
    diff_commit_id = str(getattr(spec, "diff_commit_id", "") or "").strip()
    diff_source = getattr(spec, "diff_query_source", None)
    diff_revision = (
        _revision(
            spec=spec,
            role="diff",
            commit_id=diff_commit_id,
            source=diff_source,
            expected_source_hash=_expected_source_hash(spec, "diff_source_sha256"),
        )
        if diff_commit_id
        else None
    )
    parameters = dict(effective_parameters or {})
    full_declarations = full_revision.declared_parameters
    declared_names = (
        {parameter.name for parameter in full_declarations}
        if full_declarations is not None
        else set()
    )
    raw_required_data_files = getattr(spec, "required_data_files", ())
    if not isinstance(raw_required_data_files, (list, tuple, set, frozenset)):
        raw_required_data_files = ()
    required_data_files = tuple(
        sorted(str(name) for name in raw_required_data_files if name)
    )
    raw_data_file_hashes = getattr(spec, "data_file_hashes", {})
    if not isinstance(raw_data_file_hashes, Mapping):
        raw_data_file_hashes = {}
    data_file_hashes = tuple(
        sorted(
            (
                str(name),
                str(content_hash),
            )
            for name, content_hash in raw_data_file_hashes.items()
            if name and content_hash
        )
    )
    data_hash_names = {name for name, _ in data_file_hashes}
    diff_ownership_mode = str(
        getattr(spec, "diff_ownership_mode", "") or "global"
    ).strip()

    is_raw_query = getattr(spec, "query", None) is not None and not getattr(
        spec, "run_query_id", None
    )
    full_reason_code = "eligible"
    if is_raw_query:
        if full_declarations is None:
            full_reason_code = "unverified_full_declarations"
        elif set(parameters) != declared_names:
            full_reason_code = "unsupported_full_parameters"
        else:
            full_reason_code = "raw_query"
    elif not full_revision.query_id:
        full_reason_code = "unresolved_query_id"
    elif not full_revision.commit_id or full_revision.commit_id == "head":
        full_reason_code = "unresolved_full_commit"
    elif not full_revision.source_verified:
        full_reason_code = "unverified_full_source"
    elif full_declarations is None:
        full_reason_code = "unverified_full_declarations"
    elif set(parameters) != declared_names:
        full_reason_code = "unsupported_full_parameters"
    elif any(name not in data_hash_names for name in required_data_files):
        full_reason_code = "missing_variant_data_hash"

    diff_reason_code = "eligible"
    if getattr(spec, "query", None) is not None and not getattr(
        spec, "run_query_id", None
    ):
        diff_reason_code = "raw_query"
    elif diff_ownership_mode not in SUPPORTED_DIFF_OWNERSHIP_MODES:
        diff_reason_code = "unsupported_diff_ownership"
    elif diff_revision is None:
        diff_reason_code = "missing_diff_commit"
    elif not diff_revision.query_id:
        diff_reason_code = "unresolved_query_id"
    elif diff_revision.commit_id == "head":
        diff_reason_code = "unresolved_diff_commit"
    elif full_revision.commit_id == diff_revision.commit_id and full_revision.commit_id:
        diff_reason_code = "identical_full_diff_commit"
    elif not diff_revision.source_verified:
        diff_reason_code = "unverified_diff_source"
    elif diff_revision.declared_parameters is None:
        diff_reason_code = "unverified_diff_declarations"
    elif diff_revision.declared_parameters:
        diff_reason_code = "nonempty_diff_declarations"

    return ResolvedExecutionContract(
        spec=spec,
        model_string=str(getattr(spec, "model_string", "") or ""),
        map_id=getattr(spec, "map_id", None),
        map_weight=int(getattr(spec, "map_weight", 100) or 100),
        contract_key=str(getattr(spec, "contract_key", "") or ""),
        query_name=str(getattr(spec, "query_name", "") or ""),
        full_revision=full_revision,
        full_effective_parameters=MappingProxyType(parameters),
        full_eligible=full_reason_code
        in {
            "eligible",
            "raw_query",
            "missing_variant_data_hash",
        },
        full_reason_code=full_reason_code,
        diff_reason_code=diff_reason_code,
        diff_revision=diff_revision,
        coalesce_fields=tuple(
            tuple(field_set) for field_set in getattr(spec, "coalesce_fields", ())
        ),
        variant=str(getattr(spec, "variant", "") or "base"),
        data_file_hashes=data_file_hashes,
        reducer_id=str(getattr(spec, "reducer_id", "") or "direct_rows"),
        reducer_version=int(
            getattr(spec, "reducer_version", REDUCER_VERSION) or REDUCER_VERSION
        ),
        diff_ownership_mode=diff_ownership_mode,
        normalization_version=int(
            getattr(spec, "normalization_version", NORMALIZATION_VERSION)
            or NORMALIZATION_VERSION
        ),
        identity_version=int(
            getattr(spec, "identity_version", IDENTITY_CONTRACT_VERSION)
            or IDENTITY_CONTRACT_VERSION
        ),
        query_contract_version=int(
            getattr(spec, "query_contract_version", QUERY_CONTRACT_VERSION)
            or QUERY_CONTRACT_VERSION
        ),
        diff_eligible=(
            diff_reason_code == "eligible" and full_reason_code == "eligible"
        ),
        reason_code=(
            full_reason_code if full_reason_code != "eligible" else diff_reason_code
        ),
    )


def scope_config_fingerprint(context) -> str:
    return canonical_sha256(
        {
            "network_id": str(getattr(context, "network_id", "") or ""),
            "include_tags": sorted(
                str(value)
                for value in getattr(context, "device_tag_include_tags", ()) or ()
            ),
            "exclude_tags": sorted(
                str(value)
                for value in getattr(context, "device_tag_exclude_tags", ()) or ()
            ),
            "include_match": str(
                getattr(context, "device_tag_include_match", "any") or "any"
            ),
            "prune_out_of_scope": bool(
                getattr(context, "device_tag_prune_out_of_scope", False)
            ),
            "sync_device_tags": sorted(
                str(value) for value in getattr(context, "sync_device_tags", ()) or ()
            ),
            "apply_device_scope_tags": bool(
                getattr(context, "apply_device_scope_tags", False)
            ),
            "sync_endpoints": bool(getattr(context, "sync_endpoints", False)),
            "sync_generic_endpoints": bool(
                getattr(context, "sync_generic_endpoints", False)
            ),
            "scope_endpoints_by_include_tags": bool(
                getattr(context, "scope_endpoints_by_include_tags", False)
            ),
            "endpoint_contract_version": 1,
            "device_tag_contract_version": 1,
            "prune_contract_version": 1,
        }
    )


def scope_membership_fingerprint(context, *, snapshot_id: str | None = None) -> str:
    matched_tags = getattr(context, "scoped_matched_tags", {}) or {}
    return canonical_sha256(
        {
            "snapshot_id": str(
                snapshot_id
                if snapshot_id is not None
                else getattr(context, "snapshot_id", "") or ""
            ),
            "device_names": sorted(
                str(value)
                for value in getattr(context, "scoped_device_names", ()) or ()
            ),
            "site_names": sorted(
                str(value) for value in getattr(context, "scoped_site_names", ()) or ()
            ),
            "matched_tags": {
                str(name): sorted(str(tag) for tag in tags or ())
                for name, tags in sorted(
                    matched_tags.items(), key=lambda pair: str(pair[0])
                )
            },
        }
    )


def resolve_model_execution_contract(
    model_string: str,
    contracts,
    *,
    context,
) -> ResolvedModelExecutionContract:
    ordered = tuple(
        sorted(
            contracts,
            key=lambda contract: (
                contract.map_weight,
                contract.contract_key,
                contract.query_name,
                contract.fingerprint,
            ),
        )
    )
    map_set_fingerprint = canonical_sha256(
        {
            "model": model_string,
            "enabled_maps": [
                {
                    "map_id": contract.map_id,
                    "weight": contract.map_weight,
                    "contract_key": contract.contract_key,
                    "fingerprint": contract.fingerprint,
                }
                for contract in ordered
            ],
        }
    )
    first_failure = next(
        (
            contract.reason_code
            for contract in ordered
            if not contract.full_eligible or not contract.diff_eligible
        ),
        "",
    )
    return ResolvedModelExecutionContract(
        model_string=model_string,
        maps=ordered,
        map_set_fingerprint=map_set_fingerprint,
        scope_config_fingerprint=scope_config_fingerprint(context),
        after_scope_membership_fingerprint=scope_membership_fingerprint(context),
        diff_eligible=bool(ordered) and not first_failure,
        reason_code=first_failure or ("eligible" if ordered else "no_maps"),
    )


@dataclass(frozen=True)
class BaselineContractEvidence:
    scope_membership_fingerprint: str


def compatible_baseline_evidence(
    baseline,
    model_contract: ResolvedModelExecutionContract,
) -> BaselineContractEvidence | None:
    rows = [
        row
        for row in list(getattr(baseline, "model_results", None) or [])
        if isinstance(row, dict)
        and str(row.get("model") or row.get("model_string") or "")
        == model_contract.model_string
    ]
    if not rows:
        return None
    required = (
        "execution_contract_fingerprint",
        "map_set_fingerprint",
        "scope_config_fingerprint",
        "scope_membership_fingerprint",
    )
    evidence = {key: {str(row.get(key) or "") for row in rows} for key in required}
    if any(len(values) != 1 or "" in values for values in evidence.values()):
        return None
    if evidence["execution_contract_fingerprint"] != {
        model_contract.execution_contract_fingerprint
    }:
        return None
    if evidence["map_set_fingerprint"] != {model_contract.map_set_fingerprint}:
        return None
    if evidence["scope_config_fingerprint"] != {
        model_contract.scope_config_fingerprint
    }:
        return None
    return BaselineContractEvidence(
        scope_membership_fingerprint=next(
            iter(evidence["scope_membership_fingerprint"])
        )
    )


@dataclass(frozen=True)
class DiffArtifactKey:
    model_string: str
    before_snapshot_id: str
    after_snapshot_id: str
    resolved_map_set_fingerprint: str
    scope_fingerprint: str

    @property
    def fingerprint(self) -> str:
        return canonical_sha256(
            {
                "model": self.model_string,
                "before_snapshot": self.before_snapshot_id,
                "after_snapshot": self.after_snapshot_id,
                "map_set": self.resolved_map_set_fingerprint,
                "scope": self.scope_fingerprint,
            }
        )


def diff_artifact_key(
    model_contract: ResolvedModelExecutionContract,
    *,
    before_snapshot_id: str,
    after_snapshot_id: str,
    before_scope_membership_fingerprint: str,
) -> DiffArtifactKey:
    return DiffArtifactKey(
        model_string=model_contract.model_string,
        before_snapshot_id=str(before_snapshot_id or ""),
        after_snapshot_id=str(after_snapshot_id or ""),
        resolved_map_set_fingerprint=model_contract.map_set_fingerprint,
        scope_fingerprint=canonical_sha256(
            {
                "scope_config": model_contract.scope_config_fingerprint,
                "before_membership": before_scope_membership_fingerprint,
                "after_membership": (model_contract.after_scope_membership_fingerprint),
            }
        ),
    )


@dataclass(frozen=True)
class DiffArtifact:
    key: DiffArtifactKey
    map_results: tuple[Any, ...]


class DiffArtifactStore:
    """Deduplicate one logical model artifact build within one fetcher run."""

    def __init__(self):
        self._lock = Lock()
        self._futures: dict[DiffArtifactKey, Future] = {}

    def get_or_build(
        self,
        key: DiffArtifactKey,
        builder: Callable[[], DiffArtifact],
    ) -> DiffArtifact:
        with self._lock:
            future = self._futures.get(key)
            owner = future is None
            if owner:
                future = Future()
                self._futures[key] = future
        if owner:
            try:
                artifact = builder()
            except BaseException as exc:
                future.set_exception(exc)
            else:
                if artifact.key != key:
                    future.set_exception(
                        ValueError("Diff artifact builder returned the wrong key.")
                    )
                else:
                    future.set_result(artifact)
        return future.result()


def _model_contract_issue_rows(contract):
    issues = []
    if not getattr(contract, "full_eligible", False):
        issues.append(f"full:{contract.full_reason_code}")
    if not getattr(contract, "diff_eligible", False):
        issues.append(f"diff:{contract.diff_reason_code}")
    return issues
