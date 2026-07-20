# At-rest encryption for stored Forward credentials.
#
# The Forward API password is kept in the ``ForwardSource.parameters`` JSONField.
# Values are encrypted with Fernet using a key derived from Django's ``SECRET_KEY``
# so a database dump/backup no longer contains a usable credential.
#
# Design constraints:
# - Idempotent encrypt: an already-encrypted value (``enc:v1:`` prefix) is returned
#   unchanged, so re-saving a source (which reuses the stored ciphertext when the
#   operator does not retype the password) never double-encrypts.
# - Runtime decrypt is strict: nonempty plaintext and undecryptable values fail
#   before any Forward request, so credentials cannot silently bypass encryption.
import base64
import hashlib

from django.conf import settings
from rq.timeouts import JobTimeoutException

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
    except JobTimeoutException:
        raise
    except Exception as exc:  # pragma: no cover - crypto backend misconfiguration
        raise ValueError("Unable to encrypt the Forward credential at rest.") from exc
    return SECRET_PREFIX + token


def decrypt_secret(value):
    """Return plaintext only for a valid ``enc:v1:`` token."""
    if value in (None, ""):
        return value
    if not is_encrypted(value):
        raise ValueError(
            "Forward source credential is not encrypted; re-save the source."
        )
    try:
        raw = _fernet().decrypt(value[len(SECRET_PREFIX) :].encode("ascii"))
        return raw.decode("utf-8")
    except JobTimeoutException:
        raise
    except Exception as exc:
        raise ValueError(
            "Forward source credential cannot be decrypted; re-enter it after "
            "verifying Django SECRET_KEY."
        ) from exc
