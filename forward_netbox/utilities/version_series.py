# Accept a release series rather than an exact version.
#
# The fast baseline, set-based merge and COPY/SQL engines each pinned NetBox and
# Branching to exact version strings and compared them for equality. Every patch
# release therefore switched the engines off silently — no error, no log an
# operator would see, just a first sync taking hours instead of minutes.
#
# A version pin is also the wrong instrument. It cannot distinguish a harmless
# patch from one that changes a contract an engine depends on, so it fails on
# every upgrade and catches nothing specific. What catches the real thing is a
# behavioural check against the live runtime: NetBox 4.6.6 added a third field
# to `MACAddressIndex`, and it was the set-based engine's search-index contract
# check — not its version pin — that found it.
#
# So: accept the series, and let the contract checks do the work they are for.


def series_matches(actual, series) -> bool:
    """True when `actual` is a release within `series` (e.g. "4.6" or "1.1")."""
    actual = str(actual or "")
    return actual == series or actual.startswith(f"{series}.")
