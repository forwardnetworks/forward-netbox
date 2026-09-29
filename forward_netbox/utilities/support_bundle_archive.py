import io
import json
import zipfile

import pyzipper
from django.http import HttpResponse

from .export_redaction import export_safe_payload
from .json_safe import json_safe_value


def support_bundle_zip_response(payload, *, filename, json_filename, password=""):
    # This path builds its own bytes rather than going through
    # `_download_json_response`, so it needs the export filter applied here
    # too - the zip is the form of the bundle most likely to be mailed on.
    bundle_bytes = json.dumps(
        json_safe_value(export_safe_payload(payload)),
        indent=2,
        ensure_ascii=False,
    ).encode("utf-8")
    buffer = io.BytesIO()
    archive_password = (password or "").strip()
    if archive_password:
        with pyzipper.AESZipFile(
            buffer,
            mode="w",
            compression=pyzipper.ZIP_DEFLATED,
            encryption=pyzipper.WZ_AES,
        ) as archive:
            archive.setpassword(archive_password.encode("utf-8"))
            archive.writestr(json_filename, bundle_bytes)
    else:
        with zipfile.ZipFile(
            buffer,
            mode="w",
            compression=zipfile.ZIP_DEFLATED,
            compresslevel=9,
        ) as archive:
            archive.writestr(json_filename, bundle_bytes)

    response = HttpResponse(buffer.getvalue(), content_type="application/zip")
    response["Content-Disposition"] = f'attachment; filename="{filename}"'
    return response
