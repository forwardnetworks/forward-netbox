"""The one declaration of the runtime this release was validated against.

Three subsystems refuse to run unless the installed runtime matches a
validated set - the COPY/SQL apply engine, the set-based merge, and the fast
baseline. NetBox and Branching match by series; plugins match as a subset
(no unvalidated app, the required ones present, each installed optional plugin
at a validated version). Failing closed is right: their SQL is generated against a known
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
# On NetBox 4.7 this is forward_netbox, Branching and four of the five optional
# integrations. netbox-dlm 0.10.0 raised its ceiling to 4.7.99 on 2026-09-03;
# netbox-validity 3.6.0 and netbox-peering-manager 0.3.1 followed in released
# versions, as did netbox-routing 0.5.0. netbox-cisco-aci
# 0.4.0 still declares `max_version = "4.6.99"`, and NetBox refuses to start
# with a plugin outside its declared range. It cannot be installed here, so
# listing it would be a claim about a runtime nobody can assemble.
#
# An app present in PLUGINS but absent here disables COPY/SQL, the set-based
# merge and the fast baseline with no error, turning a first sync from minutes
# into hours. A validated optional plugin that is merely NOT installed does
# not (see REQUIRED_PLUGIN_APPS). So when
# netbox-cisco-aci raises its ceiling past 4.6.99, its app label goes back in
# here and its version into VALIDATED_OPTIONAL_DISTRIBUTIONS below - a data
# edit in one file, which is the whole point of this module.
VALIDATED_PLUGIN_APPS = frozenset(
    {
        "forward_netbox",
        "netbox_branching",
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
# Only the versions that boot on 4.7: every earlier release of each caps at
# 4.6.99, and their 4.6 validations are evidence about a different runtime.
# netbox-routing 0.5.0 is its first release that boots on 4.7. netbox-cisco-aci is absent, not
# removed - its registry, models and sync paths are all still here and still
# report an absent plugin honestly; its 4.6 value (0.4.0) is recorded in
# `docs/03_Plans/active/2026-09-02-netbox-4.7-runtime.md` so regaining it is a
# lookup rather than an archaeology exercise.
VALIDATED_OPTIONAL_DISTRIBUTIONS: dict[str, frozenset[str]] = {
    "netbox-dlm": frozenset({"0.10.0", "0.10.1", "0.10.2"}),
    "netbox-peering-manager": frozenset({"0.3.1"}),
    "netbox-routing": frozenset({"0.5.0"}),
    "netbox-validity": frozenset({"3.6.0"}),
}

# The distributions whose versions a runtime probe reports. Derived rather than
# repeated: a name expected but never probed reads as ABSENT and fails the
# match exactly as a wrong version would, which is precisely the divergence
# that made this module necessary.
VALIDATED_OPTIONAL_DISTRIBUTION_NAMES = tuple(sorted(VALIDATED_OPTIONAL_DISTRIBUTIONS))


# The plugin apps a supported runtime must have. Every other validated app is
# optional: absent, it contributes no tables, signal receivers or triggers the
# fast paths could meet, so it cannot make their SQL wrong. An EXTRA app can -
# COPY/SQL has no receiver or trigger check of its own - which is why an
# unexpected app still refuses.
REQUIRED_PLUGIN_APPS = frozenset({"forward_netbox", "netbox_branching"})

# Optional plugin app -> its distribution, for the version half of the check.
# Only an INSTALLED optional plugin's version is judged.
OPTIONAL_PLUGIN_APP_DISTRIBUTIONS = {
    "netbox_cisco_aci": "netbox-cisco-aci",
    "netbox_dlm": "netbox-dlm",
    "netbox_peering_manager": "netbox-peering-manager",
    "netbox_routing": "netbox-routing",
    "validity": "netbox-validity",
}


def validated_plugin_apps_match(installed_apps) -> bool:
    """Is this installed app set a supported one?

    No app outside the validated set, and the required apps present. A
    validated optional plugin that is simply not installed no longer counts
    against the runtime: 2.9.9 required every one of them, so a deployment
    without ACI or Peering Manager ran every first sync on the slow path.
    """
    apps = frozenset(installed_apps or ())
    return apps <= VALIDATED_PLUGIN_APPS and REQUIRED_PLUGIN_APPS <= apps


def plugin_runtime_mismatch(installed_apps, optional_versions):
    """``None`` when the plugin half of the runtime is supported.

    Otherwise ``(reason, detail)`` with one of the gates' existing reason codes:
    `unsupported_plugin_app_tuple` for an unexpected app or a missing required
    one, `unsupported_optional_plugin_version` for an installed optional
    plugin at a version nobody validated. ``optional_versions`` maps (or pairs)
    distribution name to installed version, None when not installed.
    """
    apps = frozenset(installed_apps or ())
    unexpected = sorted(apps - VALIDATED_PLUGIN_APPS)
    missing = sorted(REQUIRED_PLUGIN_APPS - apps)
    if unexpected or missing:
        return (
            "unsupported_plugin_app_tuple",
            {
                "unexpected": unexpected,
                "missing_required": missing,
                "actual": sorted(apps),
            },
        )
    versions = dict(optional_versions or ())
    for app in sorted(apps & set(OPTIONAL_PLUGIN_APP_DISTRIBUTIONS)):
        distribution = OPTIONAL_PLUGIN_APP_DISTRIBUTIONS[app]
        # `.get`: a distribution this runtime has not validated (ACI on 4.7)
        # is an UNVALIDATED plugin - refuse it, do not raise KeyError out of a
        # decision function. Its app is already outside the validated set, so
        # this is a backstop.
        expected = VALIDATED_OPTIONAL_DISTRIBUTIONS.get(distribution, frozenset())
        actual = versions.get(distribution)
        if actual not in expected:
            return (
                "unsupported_optional_plugin_version",
                {"distribution": distribution, "expected": expected, "actual": actual},
            )
    return None


def unexpected_plugin_apps(installed_apps) -> list:
    """Installed apps this release has not validated."""
    return sorted(frozenset(installed_apps or ()) - VALIDATED_PLUGIN_APPS)


def missing_plugin_apps(installed_apps) -> list:
    """Validated apps that are not installed."""
    return sorted(VALIDATED_PLUGIN_APPS - frozenset(installed_apps or ()))
