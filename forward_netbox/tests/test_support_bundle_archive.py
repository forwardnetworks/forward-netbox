import io
import struct

import pyzipper
from django.test import SimpleTestCase

from forward_netbox.utilities.support_bundle_archive import support_bundle_zip_response


class SupportBundleArchiveTest(SimpleTestCase):
    def test_small_encrypted_entry_omits_crc_from_local_header(self):
        password = "test-password"
        response = support_bundle_zip_response(
            {},
            filename="support.zip",
            json_filename="bundle.json",
            password=password,
        )
        archive_bytes = bytes(response.content)

        self.assertEqual(archive_bytes[:4], b"PK\x03\x04")
        self.assertEqual(struct.unpack_from("<I", archive_bytes, 14)[0], 0)

        with pyzipper.AESZipFile(io.BytesIO(archive_bytes)) as archive:
            archive.setpassword(password.encode("utf-8"))
            self.assertEqual(archive.read("bundle.json"), b"{}")
