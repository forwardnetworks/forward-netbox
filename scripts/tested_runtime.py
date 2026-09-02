"""The NetBox runtime this repository claims to be tested on.

One declared value. It is not the only place the version appears - the release
checks, the compose defaults and the artifact validators each carry their own
literal, and several of them must, because a YAML default cannot import Python
and a regex cannot be a variable without changing what it matches.

What this replaces is not the duplication. It is the possibility of a copy being
left behind. `check_harness.py` reads this value and asserts every known pin
agrees with it, so moving the tested runtime is: edit this file, run the
harness, and fix exactly what it names.

That matters because the last uplift missed a pin that no literal search could
find. `scripts/check_release_authorization.py` writes its copy as the escaped
regex `4\\.6\\.8`, which does not match a grep for `4.6.8`, so a sweep reported
itself complete while leaving behind a check that would have refused correct
evidence - after the tag existed. The harness check reads both forms.

Historical mentions are deliberately NOT swept. `min_version` is the declared
minimum, `UPGRADE_FROM_NETBOX_OVERRIDES` records which runtime a past release
required, and migration comments describe what specific versions did. Rewriting
those into the current version makes them false. They are allowed by name in
the harness rule rather than by being invisible to it.
"""

# The runtime every release gate runs on, and the version the compatibility
# tables claim. Bump here first; the harness names every other site.
TESTED_NETBOX_VERSION = "4.7.0"
TESTED_NETBOX_TAG = f"v{TESTED_NETBOX_VERSION}"
