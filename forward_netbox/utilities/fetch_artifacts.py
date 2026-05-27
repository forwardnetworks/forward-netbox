import hashlib
import json
import os
import shutil
import tempfile
import time
from pathlib import Path
from typing import Any


DEFAULT_FETCH_ARTIFACT_TTL_SECONDS = 24 * 60 * 60
DEFAULT_FETCH_ARTIFACT_MAX_BYTES = 50 * 1024 * 1024
FETCH_ARTIFACT_DIR_ENV = "FORWARD_NETBOX_FETCH_ARTIFACT_DIR"
FETCH_ARTIFACT_TTL_ENV = "FORWARD_NETBOX_FETCH_ARTIFACT_TTL_SECONDS"
FETCH_ARTIFACT_MAX_BYTES_ENV = "FORWARD_NETBOX_FETCH_ARTIFACT_MAX_BYTES"


def fetch_artifact_key(payload: dict[str, Any]) -> str:
    serialized = json.dumps(payload, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(serialized.encode("utf-8")).hexdigest()


def load_fetch_artifact(
    key: str,
    *,
    run_id: int | str,
    now: float | None = None,
) -> tuple[dict[str, Any] | None, dict[str, Any]]:
    metadata = _base_metadata(key, run_id)
    path = _artifact_path(key, run_id=run_id)
    if not path.exists():
        metadata["status"] = "miss"
        return None, metadata

    now = time.time() if now is None else now
    try:
        with path.open("r", encoding="utf-8") as handle:
            payload = json.load(handle)
    except (OSError, json.JSONDecodeError) as exc:
        _remove_path(path)
        metadata.update(
            {
                "status": "discarded_invalid",
                "reason": str(exc),
            }
        )
        return None, metadata

    expires_at = float(payload.get("expires_at") or 0)
    metadata["expires_at"] = expires_at
    if expires_at and expires_at <= now:
        _remove_path(path)
        metadata["status"] = "expired"
        return None, metadata

    rows = list(payload.get("rows") or [])
    delete_rows = list(payload.get("delete_rows") or [])
    metadata.update(
        {
            "status": "hit",
            "row_count": len(rows),
            "delete_count": len(delete_rows),
            "byte_size": path.stat().st_size if path.exists() else 0,
        }
    )
    return payload, metadata


def save_fetch_artifact(
    key: str,
    *,
    run_id: int | str,
    rows: list[dict[str, Any]],
    delete_rows: list[dict[str, Any]],
    sync_mode: str,
    fetch_meta: dict[str, Any],
    now: float | None = None,
) -> dict[str, Any]:
    now = time.time() if now is None else now
    ttl = _env_int(FETCH_ARTIFACT_TTL_ENV, DEFAULT_FETCH_ARTIFACT_TTL_SECONDS)
    max_bytes = _env_int(FETCH_ARTIFACT_MAX_BYTES_ENV, DEFAULT_FETCH_ARTIFACT_MAX_BYTES)
    expires_at = now + max(ttl, 1)
    metadata = _base_metadata(key, run_id)
    metadata.update(
        {
            "row_count": len(rows or []),
            "delete_count": len(delete_rows or []),
            "expires_at": expires_at,
        }
    )
    payload = {
        "version": 1,
        "key": key,
        "created_at": now,
        "expires_at": expires_at,
        "sync_mode": sync_mode,
        "rows": list(rows or []),
        "delete_rows": list(delete_rows or []),
        "fetch_meta": dict(fetch_meta or {}),
    }
    try:
        serialized = json.dumps(payload, sort_keys=True, separators=(",", ":"))
    except (TypeError, ValueError) as exc:
        metadata.update({"status": "skipped_unserializable", "reason": str(exc)})
        return metadata

    byte_size = len(serialized.encode("utf-8"))
    metadata["byte_size"] = byte_size
    if max_bytes > 0 and byte_size > max_bytes:
        metadata.update(
            {
                "status": "skipped_too_large",
                "max_bytes": max_bytes,
            }
        )
        return metadata

    path = _artifact_path(key, run_id=run_id)
    tmp_path = path.with_suffix(".tmp")
    try:
        path.parent.mkdir(parents=True, exist_ok=True)
        with tmp_path.open("w", encoding="utf-8") as handle:
            handle.write(serialized)
        os.replace(tmp_path, path)
    except OSError as exc:
        _remove_path(tmp_path)
        metadata.update({"status": "store_failed", "reason": str(exc)})
        return metadata

    metadata["status"] = "stored"
    return metadata


def prune_fetch_artifacts_for_run(run_id: int | str) -> bool:
    path = _run_dir(run_id)
    if not path.exists():
        return False
    shutil.rmtree(path, ignore_errors=True)
    return not path.exists()


def load_runtime_artifact(
    key: str,
    *,
    run_id: int | str,
    now: float | None = None,
) -> tuple[dict[str, Any] | None, dict[str, Any]]:
    metadata = _base_metadata(key, run_id)
    path = _artifact_path(key, run_id=run_id)
    if not path.exists():
        metadata["status"] = "miss"
        return None, metadata

    now = time.time() if now is None else now
    try:
        with path.open("r", encoding="utf-8") as handle:
            envelope = json.load(handle)
    except (OSError, json.JSONDecodeError) as exc:
        _remove_path(path)
        metadata.update(
            {
                "status": "discarded_invalid",
                "reason": str(exc),
            }
        )
        return None, metadata

    expires_at = float(envelope.get("expires_at") or 0)
    metadata["expires_at"] = expires_at
    if expires_at and expires_at <= now:
        _remove_path(path)
        metadata["status"] = "expired"
        return None, metadata

    payload = envelope.get("payload")
    if not isinstance(payload, dict):
        _remove_path(path)
        metadata["status"] = "discarded_invalid_payload"
        return None, metadata

    metadata.update(
        {
            "status": "hit",
            "byte_size": path.stat().st_size if path.exists() else 0,
        }
    )
    return payload, metadata


def save_runtime_artifact(
    key: str,
    *,
    run_id: int | str,
    payload: dict[str, Any],
    now: float | None = None,
) -> dict[str, Any]:
    now = time.time() if now is None else now
    ttl = _env_int(FETCH_ARTIFACT_TTL_ENV, DEFAULT_FETCH_ARTIFACT_TTL_SECONDS)
    max_bytes = _env_int(FETCH_ARTIFACT_MAX_BYTES_ENV, DEFAULT_FETCH_ARTIFACT_MAX_BYTES)
    expires_at = now + max(ttl, 1)
    metadata = _base_metadata(key, run_id)
    metadata["expires_at"] = expires_at
    envelope = {
        "version": 1,
        "key": key,
        "created_at": now,
        "expires_at": expires_at,
        "payload": dict(payload or {}),
    }
    try:
        serialized = json.dumps(envelope, sort_keys=True, separators=(",", ":"))
    except (TypeError, ValueError) as exc:
        metadata.update({"status": "skipped_unserializable", "reason": str(exc)})
        return metadata

    byte_size = len(serialized.encode("utf-8"))
    metadata["byte_size"] = byte_size
    if max_bytes > 0 and byte_size > max_bytes:
        metadata.update(
            {
                "status": "skipped_too_large",
                "max_bytes": max_bytes,
            }
        )
        return metadata

    path = _artifact_path(key, run_id=run_id)
    tmp_path = path.with_suffix(".tmp")
    try:
        path.parent.mkdir(parents=True, exist_ok=True)
        with tmp_path.open("w", encoding="utf-8") as handle:
            handle.write(serialized)
        os.replace(tmp_path, path)
    except OSError as exc:
        _remove_path(tmp_path)
        metadata.update({"status": "store_failed", "reason": str(exc)})
        return metadata

    metadata["status"] = "stored"
    return metadata


def sanitize_fetch_artifact_metadata(metadata: dict[str, Any] | None) -> dict[str, Any]:
    metadata = dict(metadata or {})
    allowed_keys = {
        "key",
        "run_id",
        "status",
        "row_count",
        "delete_count",
        "byte_size",
        "max_bytes",
        "expires_at",
        "reason",
    }
    return {key: metadata[key] for key in sorted(allowed_keys) if key in metadata}


def _artifact_path(key: str, *, run_id: int | str) -> Path:
    return _run_dir(run_id) / f"{key}.json"


def _run_dir(run_id: int | str) -> Path:
    return _base_dir() / f"run-{run_id}"


def _base_dir() -> Path:
    configured = os.environ.get(FETCH_ARTIFACT_DIR_ENV)
    if configured:
        return Path(configured)
    return Path(tempfile.gettempdir()) / "forward_netbox_fetch_artifacts"


def _base_metadata(key: str, run_id: int | str) -> dict[str, Any]:
    return {
        "key": key,
        "run_id": str(run_id),
    }


def _env_int(name: str, default: int) -> int:
    value = os.environ.get(name)
    if value in ("", None):
        return default
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


def _remove_path(path: Path) -> None:
    try:
        path.unlink(missing_ok=True)
    except OSError:
        pass
