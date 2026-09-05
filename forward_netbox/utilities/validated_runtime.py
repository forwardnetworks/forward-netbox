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
VALIDATED_NETBOX_SERIES = "4.6"
VALIDATED_BRANCHING_SERIES = "1.1"

# Plugin apps as they appear in `settings.PLUGINS`.
VALIDATED_PLUGIN_APPS = frozenset(
    {
        "forward_netbox",
        "netbox_branching",
        "netbox_cisco_aci",
        "netbox_dlm",
        "netbox_peering_manager",
        "netbox_routing",
        # netbox-validity is a CONSUMER integration: it reads configuration
        # files from a git data source and writes nothing the apply engines
        # touch. It is listed here anyway, because this set is an exact match
        # that fails closed - its mere presence in PLUGINS would otherwise
        # disable the fast paths entirely. This entry is a claim that they
        # were validated with it installed; the COPY/SQL paired-branch
        # equivalence tests are that validation.
        "validity",
    }
)

# Distribution name -> every version validated against these subsystems, not a
# single pin. An exact pin meant a customer upgrading one optional plugin
# silently lost the fast paths, because the whole tuple stopped matching.
VALIDATED_OPTIONAL_DISTRIBUTIONS = {
    "netbox-cisco-aci": frozenset({"0.4.0"}),
    "netbox-dlm": frozenset(
        {"0.4.1", "0.5.0", "0.6.0", "0.7.0", "0.8.0", "0.9.1", "0.10.0"}
    ),
    "netbox-peering-manager": frozenset({"0.3.0"}),
    "netbox-routing": frozenset({"0.4.3"}),
    "netbox-validity": frozenset({"3.5.2"}),
}

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
