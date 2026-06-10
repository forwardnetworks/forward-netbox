import io
import json
import zipfile

from django.http import HttpResponse

from .json_safe import json_safe_value

try:
    import pyzipper
except ImportError:  # pragma: no cover - dependency installed for release tests
    pyzipper = None


def support_bundle_zip_response(payload, *, filename, json_filename, password=""):
    bundle_bytes = json.dumps(
        json_safe_value(payload),
        indent=2,
        ensure_ascii=False,
    ).encode("utf-8")
    buffer = io.BytesIO()
    archive_password = (password or "").strip()
    if archive_password:
        if pyzipper is None:
            raise RuntimeError(
                "Password-protected support bundle downloads require pyzipper."
            )
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
