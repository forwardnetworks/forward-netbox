# Encrypt the Forward credential at rest

**Date:** 2026-07-04

## Goal
Encrypt the stored Forward API password so a NetBox database dump/backup no longer
contains a usable credential (assessment Tier-1: password was plaintext in a
JSONField, masking display-only).

## Constraints
- Roll out over LIVE credentials without breaking auth for existing sources.
- One key, no new secret store: derive it from Django `SECRET_KEY`.
- Every password reader must keep working (there are ~8 across forms, validation,
  the client, and commands); only the client actually authenticates.

## Touched Surfaces
- `forward_netbox/utilities/crypto.py` (new) — `encrypt_secret` / `decrypt_secret`
  (Fernet, `enc:v1:` prefix). Idempotent encrypt; plaintext passthrough on decrypt;
  fail-open-to-raw + log on decrypt error (SECRET_KEY change).
- `forward_netbox/models.py` — `ForwardSource.save()` encrypts `parameters["password"]`
  (idempotent, so reusing stored ciphertext never double-encrypts).
- `forward_netbox/utilities/forward_api_impl.py` — the client decrypts the password
  at the single point it is used for HTTP auth.
- `forward_netbox/migrations/0031_encrypt_source_password.py` — data migration
  encrypting existing plaintext passwords (reversible: decrypts).
- `SECURITY.md`, `operations.md` — document the encryption + the `SECRET_KEY`
  rotation consequence.
- `forward_netbox/tests/test_crypto.py` — round-trip, idempotency, plaintext
  passthrough, save-encrypts, client-decrypts, no double-encrypt.

## Approach
Encrypt at the persistence choke point (`save()`) and decrypt at the sole
consumer (the client). Because presence checks only test truthiness and the
password field never renders its value, no other reader needs changes. Backward
compatibility is carried by (a) idempotent encrypt and (b) plaintext passthrough on
decrypt, so pre-migration rows and fresh form input both authenticate.

## Validation
Full suite 955 green (28 skip); `makemigrations --check` clean; new crypto tests;
form/model source tests unchanged.

## Rollback
Migration `0031` reverses (decrypts). Code paths passthrough plaintext, so a
downgrade after reverse leaves usable credentials.

## Decision Log
- Key derived from `SECRET_KEY` (no external KMS): zero new operational surface;
  the tradeoff — rotating `SECRET_KEY` requires re-entering credentials — is
  documented.
- Encrypt at `save()` (not `clean()`): `save()` runs on every persist including
  direct `objects.create`, whereas `clean()` does not.
- Fail-open-to-raw on decrypt error rather than crash: a wedged decrypt must not
  take the plugin down; auth then fails loudly at Forward.

## Bundled changes
- The Forward API password is now encrypted at rest; existing passwords are
  migrated; rotating `SECRET_KEY` requires re-entry (documented).
