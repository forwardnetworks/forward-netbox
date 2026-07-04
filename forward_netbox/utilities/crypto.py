# At-rest encryption for stored Forward credentials.
#
# The Forward API password is kept in the ``ForwardSource.parameters`` JSONField.
# Values are encrypted with Fernet using a key derived from Django's ``SECRET_KEY``
# so a database dump/backup no longer contains a usable credential.
#
# Design constraints that keep this safe to roll out:
# - Idempotent encrypt: an already-encrypted value (``enc:v1:`` prefix) is returned
#   unchanged, so re-saving a source (which reuses the stored ciphertext when the
#   operator does not retype the password) never double-encrypts.
# - Plaintext passthrough on decrypt: a value without the prefix is returned as-is,
#   so a source created/edited with a fresh plaintext password (e.g. during a
#   pre-save connection test) still authenticates, and pre-migration rows keep
#   working until the data migration encrypts them.
# - Fail-open-to-raw on decrypt error: if the key changed (``SECRET_KEY`` rotation)
#   the token cannot be decrypted; we return the raw value and log rather than
#   crash. Auth then fails loudly at Forward. Rotating ``SECRET_KEY`` therefore
#   requires re-entering credentials (documented in SECURITY.md).
import base64
import hashlib
import logging

from django.conf import settings

logger = logging.getLogger("forward_netbox")

SECRET_PREFIX = "enc:v1:"


def _fernet():
    from cryptography.fernet import Fernet

    digest = hashlib.sha256(str(settings.SECRET_KEY).encode("utf-8")).digest()
    return Fernet(base64.urlsafe_b64encode(digest))


def is_encrypted(value) -> bool:
    return isinstance(value, str) and value.startswith(SECRET_PREFIX)


def encrypt_secret(value):
    """Return an encrypted ``enc:v1:`` token for a plaintext string (idempotent)."""
    if not value or not isinstance(value, str) or is_encrypted(value):
        return value
    try:
        token = _fernet().encrypt(value.encode("utf-8")).decode("ascii")
    except Exception:  # pragma: no cover - crypto misconfig; never lose the value
        logger.warning("forward_netbox: unable to encrypt a stored secret at rest")
        return value
    return SECRET_PREFIX + token


def decrypt_secret(value):
    """Return the plaintext for an ``enc:v1:`` token; passthrough otherwise."""
    if not is_encrypted(value):
        return value
    try:
        raw = _fernet().decrypt(value[len(SECRET_PREFIX) :].encode("ascii"))
        return raw.decode("utf-8")
    except Exception:
        logger.warning(
            "forward_netbox: could not decrypt a stored secret (SECRET_KEY change?); "
            "re-enter the Forward credential for this source."
        )
        return value
