"""The one declaration of the runtime this release was validated against.

Three subsystems refuse to run unless the installed runtime exactly matches a
validated set - the COPY/SQL apply engine, the set-based merge, and the fast
baseline. Failing closed is right: their SQL is generated against a known
schema. But each of them used to carry its OWN copy of that set, and the fast
baseline carried two (the versions it EXPECTS and, separately, the
distributions it actually PROBES), with the test fixtures spelling the same
facts out a fifth time.

Five hand-maintained copies of one fact, and every divergence fails CLOSED and
SILENTLY: a plugin listed in four places and missed in the fifth disables the
fast path with no error anywhere, turning a first sync from minutes into hours.
Adding one optional integration required finding all five, and each was located
only by a different test going red.

So the set is declared once, here, and every consumer derives from it. A future
integration is added in one place or it is not added at all - and the probe
list can no longer disagree with the expected list, because it IS the expected
list.

What deliberately stays with each subsystem is the JUDGEMENT: which models it
will touch, what it does when the runtime does not match, and its own reason
codes. This module carries facts about the runtime, not policy about it.
"""

# NetBox and Branching are matched by series; a patch release inside a
# validated series is not a different runtime.
VALIDATED_NETBOX_SERIES = "4.7"
VALIDATED_BRANCHING_SERIES = "1.2"

# Plugin apps as they appear in `settings.PLUGINS`.
#
# On NetBox 4.7 this is forward_netbox and Branching, and nothing else. Every
# optional integration - netbox-dlm 0.9.1, netbox-cisco-aci 0.4.0,
# netbox-peering-manager 0.3.0, netbox-routing, netbox-validity 3.5.2 -
# declares `max_version = "4.6.99"`, and NetBox refuses to start with a plugin
# outside its declared range. They cannot be installed here, so listing them
# would be a claim about a runtime nobody can assemble.
#
# This set is an EXACT match that fails closed and silently: an app present in
# PLUGINS but absent here disables COPY/SQL, the set-based merge and the fast
# baseline with no error, turning a first sync from minutes into hours. So when
# an optional plugin raises its ceiling past 4.6.99, its app label goes back in
# here and its versions into VALIDATED_OPTIONAL_DISTRIBUTIONS below - a data
# edit in one file, which is the whole point of this module.
VALIDATED_PLUGIN_APPS = frozenset(
    {
        "forward_netbox",
        "netbox_branching",
    }
)

# Distribution name -> every version validated against these subsystems, not a
# single pin. An exact pin meant a customer upgrading one optional plugin
# silently lost the fast paths, because the whole tuple stopped matching.
# Empty on 4.7 for the reason above, not because the integrations were
# removed: their registry, models and sync paths are all still here and still
# report an absent plugin honestly. The 4.6 versions this set held are kept in
# the 2.9.x line, and the values to restore are recorded in
# `docs/03_Plans/active/2026-09-02-netbox-4.7-runtime.md` so regaining one is a
# lookup rather than an archaeology exercise.
VALIDATED_OPTIONAL_DISTRIBUTIONS: dict[str, frozenset[str]] = {}

# The distributions whose versions a runtime probe reports. Derived rather than
# repeated: a name expected but never probed reads as ABSENT and fails the
# match exactly as a wrong version would, which is precisely the divergence
# that made this module necessary.
VALIDATED_OPTIONAL_DISTRIBUTION_NAMES = tuple(sorted(VALIDATED_OPTIONAL_DISTRIBUTIONS))


def validated_plugin_apps_match(installed_apps) -> bool:
    """Does this installed app set match the validated one exactly?"""
    return frozenset(installed_apps or ()) == VALIDATED_PLUGIN_APPS


def unexpected_plugin_apps(installed_apps) -> list:
    """Installed apps this release has not validated."""
    return sorted(frozenset(installed_apps or ()) - VALIDATED_PLUGIN_APPS)


def missing_plugin_apps(installed_apps) -> list:
    """Validated apps that are not installed."""
    return sorted(VALIDATED_PLUGIN_APPS - frozenset(installed_apps or ()))
