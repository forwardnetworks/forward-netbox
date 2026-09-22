"""The last thing every downloaded diagnostic passes through.

Two tiers exist, and the difference is where the bytes end up.

Tier 1 is the deployment's own surfaces - the NetBox GUI, the REST API, the
server log. They are authenticated, they never leave the customer's network,
and they may show the values behind a failure: the device name that collided,
the address that would not parse. An operator troubleshooting their own estate
should not have to guess.

Tier 2 is anything that leaves as a file. A support bundle is exported
specifically to be sent to us, so it carries primary keys, constraint and
field names, counts and shapes - enough to say `dcim.Device pk=12345 already
holds this (name, site)`, which is actionable without ever naming a customer's
network. When we need the name behind a pk we ask for that one pk.

The guarantee is enforced where issues are WRITTEN (`record_issue` accepts only
value-free tokens into `raw_data`, with `OPERATOR_DETAIL_KEY` as the single
named exception). This module is the net under that: it runs on every export
path, so a value that reaches a payload some other way still does not leave.
"""

from __future__ import annotations

# The one key allowed to carry values. It is written for the GUI, and dropped
# here on the way out. Anything needing operator-only detail uses this key
# rather than inventing a second convention nothing knows to strip.
OPERATOR_DETAIL_KEY = "operator_detail"

# Keys whose values name things (devices, sites, interfaces) rather than count
# or classify them.
DROP_KEY_SUFFIXES = (
    "_sample",
    "_detail",
    "_names",
    "_by_name",
)

# Keys whose values are bulk id lists: useful as a magnitude, noise as a list.
COUNT_KEY_SUFFIXES = ("_device_ids", "_pks")

# Keys whose VALUES are operator-supplied but whose KEYS are the diagnostic.
# A query's parameters are the example: which parameters were sent is what a
# published query with a stale signature rejects, and it is the fact worth
# having - while the values are the operator's own tag names, which had been
# reaching bundles verbatim through the dependency preview's `model_results`.
NAME_KEY_SUFFIXES = ("_parameters",)


def export_safe_payload(value):
    """Drop operator-only detail and name-bearing keys from an export payload.

    Recursive and total: it is applied to whole payloads rather than to the
    handful of keys known to be risky, because the failure mode being prevented
    is precisely a key nobody remembered to list.
    """
    if isinstance(value, dict):
        cleaned = {}
        for key, item in value.items():
            if not isinstance(key, str):
                continue
            if key == OPERATOR_DETAIL_KEY:
                continue
            if key.endswith(NAME_KEY_SUFFIXES):
                cleaned[f"{key}_names"] = (
                    sorted(str(name) for name in item)
                    if isinstance(item, dict)
                    else None
                )
                continue
            if key.endswith(COUNT_KEY_SUFFIXES):
                cleaned[f"{key}_count"] = (
                    len(item) if isinstance(item, (list, tuple, set)) else None
                )
                continue
            if key.endswith(DROP_KEY_SUFFIXES):
                continue
            cleaned[key] = export_safe_payload(item)
        return cleaned
    if isinstance(value, (list, tuple)):
        return [export_safe_payload(item) for item in value]
    return value
