from __future__ import annotations

import os
import secrets
import stat
import tempfile
from pathlib import Path
from typing import Mapping


REPO_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_SECRET_DIR = REPO_ROOT / "development" / "secrets"
SECRET_NAMES = (
    "api_token_pepper_1",
    "db_password",
    "redis_password",
    "secret_key",
)


def _validate_secret(path: Path) -> None:
    file_stat = path.lstat()
    if not stat.S_ISREG(file_stat.st_mode):
        raise ValueError(f"Development secret must be a regular file: {path}")
    if file_stat.st_uid != os.getuid():
        raise ValueError(
            f"Development secret must be owned by the current user: {path}"
        )
    if stat.S_IMODE(file_stat.st_mode) != 0o600:
        raise ValueError(f"Development secret permissions must be 0600: {path}")
    if not path.read_bytes().strip():
        raise ValueError(f"Development secret must not be empty: {path}")


def ensure_development_secrets(
    secret_dir: Path = DEFAULT_SECRET_DIR,
    *,
    values: Mapping[str, str] | None = None,
) -> tuple[Path, ...]:
    """Create missing per-clone development secrets without replacing existing ones."""
    secret_dir.mkdir(mode=0o700, parents=True, exist_ok=True)
    if secret_dir.is_symlink() or not secret_dir.is_dir():
        raise ValueError(
            f"Development secret directory must be a real directory: {secret_dir}"
        )
    os.chmod(secret_dir, 0o700)

    supplied = dict(values or {})
    unknown = sorted(set(supplied) - set(SECRET_NAMES))
    if unknown:
        raise ValueError(f"Unknown development secret names: {', '.join(unknown)}")

    paths = []
    for name in SECRET_NAMES:
        path = secret_dir / name
        try:
            _validate_secret(path)
        except FileNotFoundError:
            value = supplied[name] if name in supplied else secrets.token_urlsafe(48)
            if not value.strip():
                raise ValueError(f"Development secret must not be empty: {name}")
            descriptor, temporary_name = tempfile.mkstemp(
                dir=secret_dir,
                prefix=f".{name}.",
            )
            temporary_path = Path(temporary_name)
            try:
                with os.fdopen(descriptor, "w", encoding="utf-8") as secret_file:
                    secret_file.write(value)
                    secret_file.write("\n")
                    secret_file.flush()
                    os.fsync(secret_file.fileno())
                os.chmod(temporary_path, 0o600)
                try:
                    os.link(temporary_path, path)
                except FileExistsError:
                    pass
            finally:
                temporary_path.unlink(missing_ok=True)
            _validate_secret(path)
        paths.append(path)
    return tuple(paths)
