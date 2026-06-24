# Interface-name abbreviation handling for the Mgmt_<iface> primary-IP feature.
#
# Forward device tags of the form ``Mgmt_<iface>`` (e.g. ``Mgmt_Vl211``) name the
# management interface whose IP should become the device's primary_ip. The tag
# uses abbreviated, vendor-style short names (``Vl211``) while the collected
# interface name is usually the expanded form (``Vlan211``) — and either side may
# be abbreviated. Matching is therefore done on a canonical (type, number) key so
# ``Vl211`` and ``Vlan211`` compare equal regardless of which form each carries.
import re

MGMT_TAG_PREFIX = "mgmt_"

# Lowercased leading-alpha token -> canonical interface type. Both the short and
# expanded forms map to the same canonical value so either side may be
# abbreviated. Standard Cisco-style abbreviations (Partner to flag non-standard).
INTERFACE_TYPE_ALIASES = {
    "lo": "loopback",
    "loopback": "loopback",
    "v": "vlan",
    "vl": "vlan",
    "vlan": "vlan",
    "po": "port-channel",
    "portchannel": "port-channel",
    "port-channel": "port-channel",
    "et": "ethernet",
    "eth": "ethernet",
    "ethernet": "ethernet",
    "fa": "fastethernet",
    "fastethernet": "fastethernet",
    "g": "gigabitethernet",
    "gi": "gigabitethernet",
    "gig": "gigabitethernet",
    "gige": "gigabitethernet",
    "gigabitethernet": "gigabitethernet",
    "te": "tengige",
    "tengig": "tengige",
    "tengige": "tengige",
    "tengigabitethernet": "tengige",
    "twe": "twentyfivegige",
    "twentyfivegige": "twentyfivegige",
    "fo": "fortygige",
    "fortygige": "fortygige",
    "fortygigabitethernet": "fortygige",
    "fi": "fiftygige",
    "fiftygige": "fiftygige",
    "hu": "hundredgige",
    "hundredgige": "hundredgige",
    "tu": "tunnel",
    "tunnel": "tunnel",
    # Management interfaces appear as mgmt0 / Management1 / Ma0 — unify so the
    # Mgmt_Ma0 tag resolves to the real mgmt0 interface.
    "ma": "mgmt",
    "mg": "mgmt",
    "mgmt": "mgmt",
    "mgmteth": "mgmt",
    "management": "mgmt",
    "se": "serial",
    "serial": "serial",
    "bdi": "bdi",
    "nve": "nve",
}

# Leading alpha (letters, may contain a hyphen e.g. Port-channel) then the
# numeric/slot remainder (digit-led: 0, 0/1, 1/0/2, 211, 1.100).
_NAME_RE = re.compile(r"^\s*([A-Za-z][A-Za-z\-]*?)\s*([0-9][0-9/.:]*)\s*$")


def canonical_interface_key(name):
    """Return a ``(canonical_type, number)`` key for an interface name.

    ``Vl211`` and ``Vlan211`` both yield ``("vlan", "211")``. Returns ``None``
    when the name has no recognizable ``<type><number>`` shape.
    """
    if not name:
        return None
    match = _NAME_RE.match(str(name))
    if not match:
        return None
    prefix, number = match.group(1), match.group(2)
    lookup = prefix.lower().replace("-", "")
    canonical = INTERFACE_TYPE_ALIASES.get(
        lookup,
        INTERFACE_TYPE_ALIASES.get(prefix.lower(), prefix.lower()),
    )
    return (canonical, number)


def interface_names_match(a, b):
    """True if two interface names refer to the same interface modulo abbreviation."""
    if a is None or b is None:
        return False
    if str(a).strip().lower() == str(b).strip().lower():
        return True
    key_a = canonical_interface_key(a)
    key_b = canonical_interface_key(b)
    return key_a is not None and key_a == key_b


def parse_mgmt_tag(tag):
    """Extract the interface token from a ``Mgmt_<iface>`` tag (case-insensitive).

    ``Mgmt_Vl211`` -> ``"Vl211"``. Returns ``None`` for tags that are not a
    management tag or carry no interface suffix.
    """
    if not tag:
        return None
    text = str(tag).strip()
    if not text.lower().startswith(MGMT_TAG_PREFIX):
        return None
    suffix = text[len(MGMT_TAG_PREFIX) :].strip()
    return suffix or None


def resolve_mgmt_interface_name(tag, interface_names):
    """Given a ``Mgmt_<iface>`` tag, return the matching name from ``interface_names``.

    Returns the actual interface name (as it appears in ``interface_names``) that
    the tag points at, or ``None`` if the tag is not a management tag or nothing
    matches.
    """
    token = parse_mgmt_tag(tag)
    if token is None:
        return None
    for name in interface_names:
        if interface_names_match(token, name):
            return name
    return None
