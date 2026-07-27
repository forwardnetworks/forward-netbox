"""Merge-gated, chunked contributor relations for future Tier 3 diffs.

This module deliberately does not make any query map diff-eligible. It provides
the durable state, strict delta reconstruction, validation, and promotion
primitives that Tier 3 reducers must use before they can be enabled.
"""

import hashlib
import json
import os
import shutil
import sqlite3
import tempfile
import zlib
from collections.abc import Callable
from collections.abc import Iterable
from collections.abc import Iterator
from dataclasses import dataclass
from typing import Any

from django.db import DatabaseError
from django.db import IntegrityError
from django.db import transaction
from django.utils import timezone
from rq.timeouts import JobTimeoutException

from ..exceptions import ForwardQueryError
from .diagnostics import safe_operation_failure

CONTRIBUTOR_PAYLOAD_VERSION = 1
PROVENANCE_IDENTITY_VERSION = 1
SCOPE_PAYLOAD_VERSION = 1
DEFAULT_CHUNK_UNCOMPRESSED_BYTES = 16 * 1024 * 1024
DEFAULT_CONTRIBUTOR_CACHE_MAX_ROWS = 1_500_000
DEFAULT_CONTRIBUTOR_CACHE_MAX_COMPRESSED_BYTES = 300 * 1024 * 1024
ZERO_MACS = {
    "000000000000",
    "00:00:00:00:00:00",
    "00-00-00-00-00-00",
}


class ContributorBaselineUnavailable(ForwardQueryError):
    """The contributor cache cannot safely serve this execution."""


class ContributorBaselineCorrupt(ContributorBaselineUnavailable):
    """Persisted contributor bytes or metadata failed validation."""


class ContributorBaselinePromotionError(ContributorBaselineUnavailable):
    """A pending contributor generation cannot become current."""


def _canonicalize(value, *, field_name=""):
    if isinstance(value, dict):
        normalized = {}
        for key in sorted(value, key=str):
            normalized_key = str(key)
            normalized[normalized_key] = _canonicalize(
                value[key],
                field_name=normalized_key,
            )
        return normalized
    if isinstance(value, (list, tuple)):
        values = [_canonicalize(item) for item in value]
        if field_name == "contributor_tags":
            encoded = {_canonical_json(item): item for item in values}
            return [encoded[key] for key in sorted(encoded)]
        return values
    if value is None or isinstance(value, (str, int, bool)):
        return value
    if isinstance(value, float):
        if not (float("-inf") < value < float("inf")):
            raise ContributorBaselineUnavailable(
                "Contributor rows cannot contain non-finite numbers."
            )
        return value
    raise ContributorBaselineUnavailable(
        f"Unsupported contributor scalar type `{type(value).__name__}`."
    )


def _canonical_json(value) -> str:
    return json.dumps(
        value,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
        allow_nan=False,
    )


def canonical_contributor_row(row: dict[str, Any]) -> dict[str, Any]:
    if not isinstance(row, dict):
        raise ContributorBaselineUnavailable(
            "Contributor relation rows must be mappings."
        )
    return _canonicalize(row)


def canonical_contributor_identity(row: dict[str, Any]) -> str:
    encoded = _canonical_json(canonical_contributor_row(row)).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def _safe_target_key(value) -> str:
    if value is None:
        raise ContributorBaselineUnavailable(
            "Contributor relation target keys cannot be null."
        )
    if isinstance(value, (dict, list, tuple)):
        return _canonical_json(_canonicalize(value))
    return str(value)


@dataclass(frozen=True)
class ContributorRelationContract:
    model_string: str
    map_id: int | None
    contract_key: str
    query_path: str
    query_id: str
    full_commit_id: str
    full_source_sha256: str
    diff_query_id: str
    diff_commit_id: str
    diff_source_sha256: str
    contract_fingerprint: str
    reducer_id: str
    reducer_version: int
    normalization_version: int
    identity_version: int


@dataclass(frozen=True)
class ContributorRelationSeed:
    contract: ContributorRelationContract
    rows: Iterable[dict[str, Any]]
    target_key: Callable[[dict[str, Any]], Any]


@dataclass(frozen=True)
class ContributorBaselineExpectation:
    before_snapshot_id: str
    network_fingerprint: str
    map_set_fingerprint: str
    scope_config_fingerprint: str
    scope_membership_fingerprint: str
    contract: ContributorRelationContract


def _scope_payload(scope_state) -> tuple[bytes, str]:
    envelope = {
        "kind": "forward_contributor_scope",
        "payload_version": SCOPE_PAYLOAD_VERSION,
        "scope": _canonicalize(scope_state or {}),
    }
    payload = zlib.compress(_canonical_json(envelope).encode("utf-8"), level=6)
    return payload, hashlib.sha256(payload).hexdigest()


def contributor_cache_limits(sync) -> tuple[int, int]:
    parameters = dict(getattr(getattr(sync, "source", None), "parameters", {}) or {})
    try:
        max_rows = int(
            parameters.get(
                "contributor_cache_max_rows",
                DEFAULT_CONTRIBUTOR_CACHE_MAX_ROWS,
            )
        )
    except (TypeError, ValueError):
        max_rows = DEFAULT_CONTRIBUTOR_CACHE_MAX_ROWS
    try:
        max_compressed_bytes = int(
            parameters.get(
                "contributor_cache_max_compressed_bytes",
                DEFAULT_CONTRIBUTOR_CACHE_MAX_COMPRESSED_BYTES,
            )
        )
    except (TypeError, ValueError):
        max_compressed_bytes = DEFAULT_CONTRIBUTOR_CACHE_MAX_COMPRESSED_BYTES
    return max(1, max_rows), max(1, max_compressed_bytes)


def decode_scope_payload(baseline) -> dict[str, Any]:
    payload = bytes(baseline.scope_payload)
    if hashlib.sha256(payload).hexdigest() != baseline.scope_payload_checksum:
        raise ContributorBaselineCorrupt(
            "Contributor scope payload checksum validation failed."
        )
    try:
        envelope = json.loads(zlib.decompress(payload).decode("utf-8"))
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise ContributorBaselineCorrupt(
            "Contributor scope payload is invalid."
        ) from exc
    if (
        not isinstance(envelope, dict)
        or envelope.get("kind") != "forward_contributor_scope"
        or envelope.get("payload_version") != SCOPE_PAYLOAD_VERSION
        or baseline.scope_payload_version != SCOPE_PAYLOAD_VERSION
        or not isinstance(envelope.get("scope"), dict)
    ):
        raise ContributorBaselineCorrupt(
            "Contributor scope payload version/header validation failed."
        )
    return envelope["scope"]


def _relation_header(contract: ContributorRelationContract) -> dict[str, Any]:
    return {
        "kind": "forward_contributor_relation",
        "payload_version": CONTRIBUTOR_PAYLOAD_VERSION,
        "provenance_identity_version": PROVENANCE_IDENTITY_VERSION,
        "contract_fingerprint": contract.contract_fingerprint,
    }


def _write_relation_chunks(
    relation,
    seed: ContributorRelationSeed,
    *,
    max_rows: int,
    max_compressed_bytes: int,
) -> tuple[int, int, int, str]:
    from ..models import ForwardContributorRelationChunk

    relation_hasher = hashlib.sha256()
    buffer = bytearray()
    row_count = 0
    uncompressed_bytes = 0
    compressed_bytes = 0
    sequence = 0

    def flush():
        nonlocal buffer
        nonlocal compressed_bytes
        nonlocal sequence
        if not buffer:
            return
        payload = zlib.compress(bytes(buffer), level=6)
        compressed_bytes += len(payload)
        if compressed_bytes > max_compressed_bytes:
            raise ContributorBaselineUnavailable(
                "Contributor relation exceeded the configured compressed-byte budget."
            )
        ForwardContributorRelationChunk.objects.create(
            relation=relation,
            sequence=sequence,
            payload=payload,
            payload_checksum=hashlib.sha256(payload).hexdigest(),
            compressed_bytes=len(payload),
        )
        sequence += 1
        buffer = bytearray()

    header_line = (_canonical_json(_relation_header(seed.contract)) + "\n").encode(
        "utf-8"
    )
    relation_hasher.update(header_line)
    uncompressed_bytes += len(header_line)
    buffer.extend(header_line)

    for raw_row in seed.rows:
        row = canonical_contributor_row(raw_row)
        row_json = _canonical_json(row)
        identity = hashlib.sha256(row_json.encode("utf-8")).hexdigest()
        target_key = _safe_target_key(seed.target_key(row))
        line = (_canonical_json([identity, target_key, row]) + "\n").encode("utf-8")
        row_count += 1
        if row_count > max_rows:
            raise ContributorBaselineUnavailable(
                "Contributor relation exceeded the configured row budget."
            )
        if buffer and len(buffer) + len(line) > DEFAULT_CHUNK_UNCOMPRESSED_BYTES:
            flush()
        buffer.extend(line)
        relation_hasher.update(line)
        uncompressed_bytes += len(line)
    flush()
    return (
        row_count,
        uncompressed_bytes,
        compressed_bytes,
        relation_hasher.hexdigest(),
    )


def stage_contributor_baseline(
    ingestion,
    relation_seeds: Iterable[ContributorRelationSeed],
    *,
    network_fingerprint: str,
    map_set_fingerprint: str,
    scope_config_fingerprint: str,
    scope_membership_fingerprint: str,
    scope_state: dict[str, Any] | None = None,
    max_rows: int | None = None,
    max_compressed_bytes: int | None = None,
):
    """Atomically stage one immutable pending generation for an ingestion."""

    from ..models import (
        ForwardContributorBaseline,
        ForwardContributorRelation,
        ForwardSync,
    )

    seeds = tuple(relation_seeds)
    if not seeds:
        raise ContributorBaselineUnavailable(
            "A contributor baseline requires at least one complete relation."
        )
    scope_payload, scope_checksum = _scope_payload(scope_state or {})
    configured_max_rows, configured_max_compressed_bytes = contributor_cache_limits(
        ingestion.sync
    )
    max_rows = configured_max_rows if max_rows is None else max(1, int(max_rows))
    max_compressed_bytes = (
        configured_max_compressed_bytes
        if max_compressed_bytes is None
        else max(1, int(max_compressed_bytes))
    )
    with transaction.atomic():
        ForwardSync.objects.select_for_update().get(pk=ingestion.sync_id)
        if ForwardContributorBaseline.objects.filter(ingestion=ingestion).exists():
            raise ContributorBaselineUnavailable(
                "A pending contributor baseline already exists for this ingestion."
            )
        current = (
            ForwardContributorBaseline.objects.select_for_update()
            .filter(sync_id=ingestion.sync_id, is_current=True)
            .first()
        )
        baseline = ForwardContributorBaseline.objects.create(
            sync_id=ingestion.sync_id,
            ingestion=ingestion,
            parent_baseline=current,
            snapshot_id=str(ingestion.snapshot_id or ""),
            network_fingerprint=network_fingerprint,
            map_set_fingerprint=map_set_fingerprint,
            scope_config_fingerprint=scope_config_fingerprint,
            scope_membership_fingerprint=scope_membership_fingerprint,
            scope_payload_version=SCOPE_PAYLOAD_VERSION,
            scope_payload=scope_payload,
            scope_payload_checksum=scope_checksum,
            status=ForwardContributorBaseline.Status.PENDING,
            is_current=False,
        )
        total_rows = 0
        total_compressed = 0
        for seed in seeds:
            contract = seed.contract
            relation = ForwardContributorRelation.objects.create(
                baseline=baseline,
                query_map_id=contract.map_id,
                model_string=contract.model_string,
                contract_key=contract.contract_key,
                query_path=contract.query_path,
                query_id=contract.query_id,
                full_commit_id=contract.full_commit_id,
                full_source_sha256=contract.full_source_sha256,
                diff_query_id=contract.diff_query_id,
                diff_commit_id=contract.diff_commit_id,
                diff_source_sha256=contract.diff_source_sha256,
                contract_fingerprint=contract.contract_fingerprint,
                reducer_id=contract.reducer_id,
                reducer_version=contract.reducer_version,
                normalization_version=contract.normalization_version,
                identity_version=contract.identity_version,
                provenance_identity_version=PROVENANCE_IDENTITY_VERSION,
                payload_version=CONTRIBUTOR_PAYLOAD_VERSION,
                relation_checksum="",
            )
            (
                row_count,
                uncompressed_bytes,
                compressed_bytes,
                relation_checksum,
            ) = _write_relation_chunks(
                relation,
                seed,
                max_rows=max_rows - total_rows,
                max_compressed_bytes=max_compressed_bytes - total_compressed,
            )
            total_rows += row_count
            total_compressed += compressed_bytes
            relation.row_count = row_count
            relation.uncompressed_bytes = uncompressed_bytes
            relation.compressed_bytes = compressed_bytes
            relation.relation_checksum = relation_checksum
            relation.save(
                update_fields=[
                    "row_count",
                    "uncompressed_bytes",
                    "compressed_bytes",
                    "relation_checksum",
                ]
            )
        return baseline


def iter_relation_entries(relation) -> Iterator[tuple[str, str, dict[str, Any]]]:
    """Validate and stream relation entries without building a Python row map."""

    if (
        relation.payload_version != CONTRIBUTOR_PAYLOAD_VERSION
        or relation.provenance_identity_version != PROVENANCE_IDENTITY_VERSION
    ):
        raise ContributorBaselineCorrupt(
            "Contributor relation payload/identity version mismatch."
        )
    relation_hasher = hashlib.sha256()
    expected_sequence = 0
    header_seen = False
    row_count = 0
    compressed_bytes = 0
    uncompressed_bytes = 0
    for chunk in relation.chunks.order_by("sequence").iterator():
        if chunk.sequence != expected_sequence:
            raise ContributorBaselineCorrupt(
                "Contributor relation chunk sequence is incomplete."
            )
        expected_sequence += 1
        payload = bytes(chunk.payload)
        if (
            len(payload) != chunk.compressed_bytes
            or hashlib.sha256(payload).hexdigest() != chunk.payload_checksum
        ):
            raise ContributorBaselineCorrupt(
                "Contributor relation chunk checksum validation failed."
            )
        try:
            raw = zlib.decompress(payload)
        except zlib.error as exc:
            raise ContributorBaselineCorrupt(
                "Contributor relation chunk compression is invalid."
            ) from exc
        compressed_bytes += len(payload)
        uncompressed_bytes += len(raw)
        relation_hasher.update(raw)
        for line in raw.splitlines():
            try:
                value = json.loads(line)
            except (UnicodeDecodeError, json.JSONDecodeError) as exc:
                raise ContributorBaselineCorrupt(
                    "Contributor relation JSON is invalid."
                ) from exc
            if not header_seen:
                header_seen = True
                if (
                    not isinstance(value, dict)
                    or value.get("kind") != "forward_contributor_relation"
                    or value.get("payload_version") != CONTRIBUTOR_PAYLOAD_VERSION
                    or value.get("provenance_identity_version")
                    != PROVENANCE_IDENTITY_VERSION
                    or value.get("contract_fingerprint")
                    != relation.contract_fingerprint
                ):
                    raise ContributorBaselineCorrupt(
                        "Contributor relation header validation failed."
                    )
                continue
            if not isinstance(value, list) or len(value) != 3:
                raise ContributorBaselineCorrupt(
                    "Contributor relation record shape is invalid."
                )
            identity, target_key, row = value
            if (
                not isinstance(identity, str)
                or not isinstance(target_key, str)
                or not isinstance(row, dict)
            ):
                raise ContributorBaselineCorrupt(
                    "Contributor relation record types are invalid."
                )
            canonical = canonical_contributor_row(row)
            expected_identity = canonical_contributor_identity(canonical)
            if identity != expected_identity:
                raise ContributorBaselineCorrupt(
                    "Contributor provenance identity validation failed."
                )
            row_count += 1
            yield identity, target_key, canonical
    if not header_seen or expected_sequence == 0:
        raise ContributorBaselineCorrupt(
            "Contributor relation contains no payload header."
        )
    if row_count != relation.row_count:
        raise ContributorBaselineCorrupt(
            "Contributor relation row-count validation failed."
        )
    if uncompressed_bytes != relation.uncompressed_bytes:
        raise ContributorBaselineCorrupt(
            "Contributor relation uncompressed-byte validation failed."
        )
    if compressed_bytes != relation.compressed_bytes:
        raise ContributorBaselineCorrupt(
            "Contributor relation compressed-byte validation failed."
        )
    if relation_hasher.hexdigest() != relation.relation_checksum:
        raise ContributorBaselineCorrupt(
            "Contributor relation checksum validation failed."
        )


def _contract_mismatch_reason(relation, contract) -> str:
    commit_fields = (
        "full_commit_id",
        "diff_commit_id",
    )
    if any(
        getattr(relation, name) != getattr(contract, name) for name in commit_fields
    ):
        return "query_commit_changed"
    identity_fields = (
        "query_path",
        "query_id",
        "full_source_sha256",
        "diff_query_id",
        "diff_source_sha256",
    )
    if any(
        getattr(relation, name) != getattr(contract, name) for name in identity_fields
    ):
        return "query_identity_changed"
    contract_fields = (
        "model_string",
        "contract_key",
        "contract_fingerprint",
        "reducer_id",
        "reducer_version",
        "normalization_version",
        "identity_version",
    )
    if any(
        getattr(relation, name) != getattr(contract, name) for name in contract_fields
    ):
        return "contract_fingerprint_changed"
    if relation.query_map_id != contract.map_id:
        return "query_map_changed"
    return ""


def compatible_current_relation(sync, expectation: ContributorBaselineExpectation):
    """Return ``(relation, reason)``; every cache problem fails closed."""

    from ..models import ForwardContributorBaseline

    try:
        baseline = (
            ForwardContributorBaseline.objects.filter(sync=sync, is_current=True)
            .prefetch_related("relations__chunks")
            .first()
        )
        if baseline is None:
            return None, "cache_miss"
        if baseline.snapshot_id != expectation.before_snapshot_id:
            return None, "baseline_snapshot_changed"
        if baseline.network_fingerprint != expectation.network_fingerprint:
            return None, "network_scope_changed"
        if baseline.map_set_fingerprint != expectation.map_set_fingerprint:
            return None, "map_set_changed"
        if baseline.scope_config_fingerprint != expectation.scope_config_fingerprint:
            return None, "scope_config_changed"
        if (
            baseline.scope_membership_fingerprint
            != expectation.scope_membership_fingerprint
        ):
            return None, "scope_membership_changed"
        decode_scope_payload(baseline)
        relation = baseline.relations.filter(
            contract_key=expectation.contract.contract_key
        ).first()
        if relation is None:
            return None, "cache_miss"
        mismatch = _contract_mismatch_reason(relation, expectation.contract)
        if mismatch:
            return None, mismatch
        for _entry in iter_relation_entries(relation):
            pass
        return relation, ""
    except JobTimeoutException:
        raise
    except ContributorBaselineUnavailable:
        return None, "cache_corrupt"
    except DatabaseError:
        return None, "cache_database_error"


class ContributorWorkRelation:
    """Private disk-backed relation used for strict change reconstruction."""

    def __init__(self, relation):
        self.relation = relation
        self._directory = tempfile.mkdtemp(prefix="forward-contributors-")
        self.path = os.path.join(self._directory, "contributors.sqlite3")
        self.connection = None
        try:
            os.chmod(self._directory, 0o700)
            # Workload fetches run in worker threads, while immutable Django
            # staging runs on the coordinator after every worker has joined.
            # The relation is never accessed concurrently, but SQLite's default
            # creator-thread guard would reject that serialized handoff.
            self.connection = sqlite3.connect(self.path, check_same_thread=False)
            os.chmod(self.path, 0o600)
            self.connection.execute("PRAGMA journal_mode=MEMORY")
            self.connection.execute("PRAGMA synchronous=FULL")
            self.connection.execute(
                """
                CREATE TABLE contributors (
                    identity TEXT PRIMARY KEY,
                    target_key TEXT NOT NULL,
                    row_json BLOB NOT NULL
                ) WITHOUT ROWID
                """
            )
            self.connection.execute(
                "CREATE INDEX contributors_target_key ON contributors(target_key)"
            )
            batch = []
            for identity, target_key, row in iter_relation_entries(relation):
                batch.append(
                    (
                        identity,
                        target_key,
                        _canonical_json(row).encode("utf-8"),
                    )
                )
                if len(batch) >= 1000:
                    self.connection.executemany(
                        "INSERT INTO contributors VALUES (?, ?, ?)",
                        batch,
                    )
                    batch = []
            if batch:
                self.connection.executemany(
                    "INSERT INTO contributors VALUES (?, ?, ?)",
                    batch,
                )
            self.connection.commit()
        except sqlite3.IntegrityError as exc:
            self.close()
            raise ContributorBaselineCorrupt(
                "Contributor relation contains duplicate/conflicting identities."
            ) from exc
        except Exception:
            self.close()
            raise

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, traceback):
        self.close()

    def __del__(self):
        try:
            self.close()
        except (OSError, sqlite3.Error):
            # Best-effort process-exit safety net. Normal runtime paths close
            # explicitly after immutable staging or on fetch failure.
            pass

    def close(self):
        connection = getattr(self, "connection", None)
        directory = getattr(self, "_directory", "")
        try:
            if connection is not None:
                connection.close()
                self.connection = None
        finally:
            if directory:
                shutil.rmtree(directory)
                self._directory = ""

    def _delete_exact(self, row):
        identity = canonical_contributor_identity(row)
        cursor = self.connection.execute(
            "DELETE FROM contributors WHERE identity = ?",
            (identity,),
        )
        if cursor.rowcount != 1:
            raise ContributorBaselineUnavailable(
                "Contributor diff removed an identity absent from the baseline."
            )

    def _insert_exact(self, row, target_key):
        canonical = canonical_contributor_row(row)
        identity = canonical_contributor_identity(canonical)
        try:
            self.connection.execute(
                "INSERT INTO contributors VALUES (?, ?, ?)",
                (
                    identity,
                    _safe_target_key(target_key(canonical)),
                    _canonical_json(canonical).encode("utf-8"),
                ),
            )
        except sqlite3.IntegrityError as exc:
            raise ContributorBaselineUnavailable(
                "Contributor diff added a duplicate/conflicting identity."
            ) from exc

    def apply_diff(
        self,
        changes: Iterable[dict[str, Any]],
        *,
        target_key: Callable[[dict[str, Any]], Any],
    ):
        try:
            self.connection.execute("BEGIN IMMEDIATE")
            for change in changes:
                if not isinstance(change, dict):
                    raise ContributorBaselineUnavailable(
                        "Contributor diff rows must be mappings."
                    )
                change_type = str(change.get("type") or "").upper()
                before = change.get("before")
                after = change.get("after")
                if change_type == "DELETED" and isinstance(before, dict):
                    self._delete_exact(before)
                elif change_type == "ADDED" and isinstance(after, dict):
                    self._insert_exact(after, target_key)
                elif (
                    change_type == "MODIFIED"
                    and isinstance(before, dict)
                    and isinstance(after, dict)
                ):
                    self._delete_exact(before)
                    self._insert_exact(after, target_key)
                else:
                    raise ContributorBaselineUnavailable(
                        "Contributor diff change type/side shape is unsupported."
                    )
            self.connection.commit()
        except Exception:
            self.connection.rollback()
            raise
        return self

    def iter_rows(self) -> Iterator[dict[str, Any]]:
        cursor = self.connection.execute(
            "SELECT row_json FROM contributors ORDER BY identity"
        )
        for (row_json,) in cursor:
            yield json.loads(bytes(row_json))

    def reduce_mac_addresses(
        self,
        *,
        scoped_devices: set[str] | None = None,
    ) -> list[dict[str, Any]]:
        """Representative reducer used by the false-deletion safety fixture."""

        selected = []
        current_key = None
        candidates = []

        def flush():
            if not candidates:
                return
            chosen = min(
                candidates,
                key=lambda row: (
                    str(row.get("device") or ""),
                    str(row.get("interface") or ""),
                ),
            )
            mac = str(chosen.get("mac_address") or "")
            selected.append(
                {
                    "device": chosen.get("device"),
                    "interface": chosen.get("interface"),
                    "mac": mac,
                    "mac_address": mac,
                }
            )

        cursor = self.connection.execute(
            "SELECT target_key, row_json FROM contributors ORDER BY target_key"
        )
        for target_key, row_json in cursor:
            row = json.loads(bytes(row_json))
            if scoped_devices is not None and row.get("device") not in scoped_devices:
                continue
            mac = str(row.get("mac_address") or "")
            if mac.replace(".", "").lower() in ZERO_MACS or mac.lower() in ZERO_MACS:
                continue
            if current_key is not None and target_key != current_key:
                flush()
                candidates = []
            current_key = target_key
            candidates.append(row)
        flush()
        return selected


def promote_contributor_baselines_locked(ingestion) -> int:
    """CAS-promote an ingestion's complete pending generation.

    The caller must already be inside the merge/no-op finalization transaction.
    """

    from ..models import ForwardContributorBaseline

    if ingestion.merge_applied_at is None:
        raise ContributorBaselinePromotionError(
            "Contributor baseline promotion requires durable finalization evidence."
        )
    pending = (
        ForwardContributorBaseline.objects.select_for_update()
        .filter(ingestion=ingestion)
        .first()
    )
    if pending is None:
        return 0
    if (
        pending.status == ForwardContributorBaseline.Status.CURRENT
        and pending.is_current
    ):
        return 1
    if pending.status != ForwardContributorBaseline.Status.PENDING:
        raise ContributorBaselinePromotionError(
            "Contributor baseline promotion requires a pending generation."
        )
    current = (
        ForwardContributorBaseline.objects.select_for_update()
        .filter(sync_id=ingestion.sync_id, is_current=True)
        .exclude(pk=pending.pk)
        .first()
    )
    current_id = current.pk if current is not None else None
    if pending.parent_baseline_id != current_id:
        raise ContributorBaselinePromotionError(
            "Contributor baseline parent is stale; promotion refused."
        )
    decode_scope_payload(pending)
    relations = list(pending.relations.select_for_update().order_by("contract_key"))
    if not relations:
        raise ContributorBaselinePromotionError(
            "Contributor baseline promotion requires complete relations."
        )
    for relation in relations:
        for _entry in iter_relation_entries(relation):
            pass
    if current is not None:
        current.is_current = False
        current.status = ForwardContributorBaseline.Status.SUPERSEDED
        current.save(update_fields=["is_current", "status"])
    pending.is_current = True
    pending.status = ForwardContributorBaseline.Status.CURRENT
    pending.promoted_at = timezone.now()
    try:
        pending.save(update_fields=["is_current", "status", "promoted_at"])
    except IntegrityError as exc:
        raise ContributorBaselinePromotionError(
            "Concurrent contributor baseline promotion was refused."
        ) from exc
    if current is not None:
        current.relations.all().delete()
        current.scope_payload = b""
        current.save(update_fields=["scope_payload"])
    return 1


def promote_contributor_baselines_fail_closed(ingestion, *, logger=None) -> int:
    """Promote in a savepoint; cache failure must not strand a merged sync."""

    try:
        with transaction.atomic():
            return promote_contributor_baselines_locked(ingestion)
    except JobTimeoutException:
        raise
    except (ContributorBaselineUnavailable, DatabaseError) as exc:
        if logger is not None:
            logger.log_warning(
                safe_operation_failure("Contributor baseline promotion", exc),
                obj=ingestion,
            )
        return 0


def contributor_storage_summary(baseline) -> dict[str, int]:
    relations = list(
        baseline.relations.values(
            "row_count",
            "uncompressed_bytes",
            "compressed_bytes",
        )
    )
    return {
        "relation_count": len(relations),
        "row_count": sum(int(row["row_count"] or 0) for row in relations),
        "uncompressed_bytes": sum(
            int(row["uncompressed_bytes"] or 0) for row in relations
        ),
        "compressed_bytes": sum(int(row["compressed_bytes"] or 0) for row in relations),
        "scope_compressed_bytes": len(bytes(baseline.scope_payload or b"")),
    }
