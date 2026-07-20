from django.test import TestCase

from forward_netbox.exceptions import ForwardClientError
from forward_netbox.models import ForwardSource
from forward_netbox.utilities.crypto import decrypt_secret
from forward_netbox.utilities.crypto import encrypt_secret
from forward_netbox.utilities.crypto import is_encrypted
from forward_netbox.utilities.crypto import SECRET_PREFIX


class CryptoHelperTest(TestCase):
    def test_encrypt_then_decrypt_round_trips(self):
        token = encrypt_secret("s3cr3t")
        self.assertTrue(token.startswith(SECRET_PREFIX))
        self.assertNotIn("s3cr3t", token)
        self.assertEqual(decrypt_secret(token), "s3cr3t")

    def test_encrypt_is_idempotent(self):
        once = encrypt_secret("pw")
        twice = encrypt_secret(once)
        self.assertEqual(once, twice)
        self.assertEqual(decrypt_secret(twice), "pw")

    def test_decrypt_rejects_plaintext(self):
        with self.assertRaisesRegex(ValueError, "credential is not encrypted"):
            decrypt_secret("plain")
        self.assertFalse(is_encrypted("plain"))

    def test_empty_values_untouched(self):
        self.assertEqual(encrypt_secret(""), "")
        self.assertIsNone(encrypt_secret(None))
        self.assertEqual(decrypt_secret(""), "")


class SourceCredentialAtRestTest(TestCase):
    def _source(self, password="secret"):
        return ForwardSource.objects.create(
            name="enc-source",
            type="saas",
            url="https://fwd.app",
            parameters={
                "username": "user@example.com",
                "password": password,
                "network_id": "test-network",
            },
        )

    def test_save_encrypts_password_at_rest(self):
        source = self._source(password="my-password")
        source.refresh_from_db()
        stored = source.parameters["password"]
        self.assertTrue(is_encrypted(stored))
        self.assertNotIn("my-password", stored)

    def test_client_sees_decrypted_password(self):
        source = self._source(password="my-password")
        source.refresh_from_db()
        client = source.get_client()
        self.assertEqual(client.password, "my-password")

    def test_resave_does_not_double_encrypt(self):
        source = self._source(password="my-password")
        source.refresh_from_db()
        first = source.parameters["password"]
        # Re-save reusing the stored (already-encrypted) value.
        source.save()
        source.refresh_from_db()
        self.assertEqual(source.parameters["password"], first)
        self.assertEqual(source.get_client().password, "my-password")

    def test_client_rejects_plaintext_database_value(self):
        source = self._source(password="my-password")
        ForwardSource.objects.filter(pk=source.pk).update(
            parameters={
                **source.parameters,
                "password": "plaintext-bypass",
            }
        )
        source.refresh_from_db()

        with self.assertRaisesRegex(ForwardClientError, "credential is not encrypted"):
            source.get_client()
