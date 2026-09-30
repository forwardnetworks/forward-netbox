"""Where in the repository the config backup writes a device's file.

The folder used to be the literal ``configs``. Validity binds a device to a file
through the data source's ``device_config_path`` template, so an operator whose
repository already has another layout had to reshape the repository or give up
the backup. This is that one setting: a relative folder path, default
``configs``, that may be nested (``net/configs``).

A repository path built from operator input is a place a mistake becomes
repository structure, so the rules are strict and live in one place, stdlib
only, shared by the form, the source validator, the backup itself and Health:

- relative and non-empty, no leading slash, no empty segment (``a//b``);
- no ``.`` or ``..`` segment, no backslash, no control character;
- no ``.git`` segment, which git refuses to hold;
- the first segment is not ``unmanaged``, the fixed folder for devices this
  sync does not manage, which would otherwise overlap it.
"""

DEFAULT_CONFIG_BACKUP_PATH_PREFIX = "configs"
UNMANAGED_FOLDER = "unmanaged"
MAX_SEGMENT_LENGTH = 100
MAX_PATH_LENGTH = 255
PARAMETER_NAME = "config_backup_path_prefix"


def normalize_config_backup_path_prefix(value):
    """Return the folder path in its canonical form, or raise ``ValueError``.

    A blank value means the default. One trailing slash is tolerated and
    removed, because it is what people type; anything else that is not a plain
    relative path is refused with a sentence that says which rule it broke.
    """
    if value is None:
        return DEFAULT_CONFIG_BACKUP_PATH_PREFIX
    if not isinstance(value, str):
        raise ValueError("the config backup folder must be text.")
    text = value.strip()
    if not text:
        return DEFAULT_CONFIG_BACKUP_PATH_PREFIX
    if text.startswith("/"):
        raise ValueError(
            "the config backup folder is relative to the repository root, so it "
            "must not start with `/`."
        )
    text = text.rstrip("/")
    if not text:
        raise ValueError("the config backup folder must not be only slashes.")
    if len(text) > MAX_PATH_LENGTH:
        raise ValueError(
            f"the config backup folder must be at most {MAX_PATH_LENGTH} characters."
        )
    segments = text.split("/")
    for segment in segments:
        if not segment:
            raise ValueError(
                "the config backup folder must not contain an empty segment " "(`//`)."
            )
        if segment in (".", ".."):
            raise ValueError(
                "the config backup folder must not contain `.` or `..` segments."
            )
        if "\\" in segment:
            raise ValueError(
                "the config backup folder must use `/` between folders, not `\\`."
            )
        if any(ord(char) < 32 or ord(char) == 127 for char in segment):
            raise ValueError(
                "the config backup folder must not contain control characters."
            )
        if segment.lower() == ".git":
            raise ValueError("the config backup folder must not contain `.git`.")
        if len(segment) > MAX_SEGMENT_LENGTH:
            raise ValueError(
                "each folder in the config backup path must be at most "
                f"{MAX_SEGMENT_LENGTH} characters."
            )
    if segments[0] == UNMANAGED_FOLDER:
        raise ValueError(
            f"`{UNMANAGED_FOLDER}` is the folder for devices this sync does not "
            "manage, so the config backup folder cannot start with it."
        )
    return "/".join(segments)


def config_backup_path_prefix(parameters):
    """The configured folder for these source parameters, validated."""
    return normalize_config_backup_path_prefix((parameters or {}).get(PARAMETER_NAME))


def path_segments(prefix):
    """The folder's segments as the bytes a git tree stores."""
    return [segment.encode("utf-8") for segment in prefix.split("/")]


def device_config_path_template(prefix):
    """The Validity ``device_config_path`` that matches this folder."""
    return prefix + "/{{device.name}}.cfg"
