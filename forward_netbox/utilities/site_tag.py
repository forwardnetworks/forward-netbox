# The NetBox site a Forward device belongs to, and the one rule that decides it.
#
# Forward knows a device by its location. NetBox sites are usually coarser: many
# Forward locations can make up one data centre. A device tagged
# `NetBox_Site_<name>` is placed at the site `<name>`, whatever its location; an
# untagged device keeps its location. Site names are lowercased like every site
# name the sync creates.
#
# The shipped NQE maps get this from the shared `deviceSiteName` helper in
# `queries/netbox_utilities.nqe`. A few ad-hoc queries the plugin builds in Python
# cannot import that module, so they embed the same rule from here, and
# `test_site_tag_override` keeps the two spellings in step. `site_name_for` is the
# plain-Python statement of the rule, used as the reference in tests and docs.

SITE_TAG_PREFIX = "NetBox_Site_"


def site_name_for(tag_names, location_name, *, fallback="unknown"):
    """The lowercased site name for a device's tags and location name.

    More than one site tag takes the first alphabetically, so the answer never
    depends on tag order. A tag with nothing after the prefix is not a site tag.
    """
    tagged = sorted(
        str(tag)[len(SITE_TAG_PREFIX) :]
        for tag in tag_names or ()
        if str(tag).startswith(SITE_TAG_PREFIX) and len(str(tag)) > len(SITE_TAG_PREFIX)
    )
    if tagged:
        return tagged[0].lower()
    location = str(location_name or "")
    return location.lower() if location else fallback


def site_tag_nqe_let(variable="device", name="site_tag"):
    """The NQE `let` line that finds a device's site tag (null when it has none)."""
    return (
        f"let {name} = min(foreach tag in {variable}.tagNames "
        f'where matches(tag, "{SITE_TAG_PREFIX}*") '
        f'where length(replace(tag, "{SITE_TAG_PREFIX}", "")) > 0 '
        f'select replace(tag, "{SITE_TAG_PREFIX}", ""))'
    )


def site_name_nqe(variable="device", name="site_tag", fallback='"unknown"'):
    """The NQE expression for the site name, after `site_tag_nqe_let`."""
    return (
        f"if isPresent({name}) then toLowerCase({name}) "
        f"else if isPresent({variable}.locationName) "
        f"&& length({variable}.locationName) > 0 "
        f"then toLowerCase({variable}.locationName) else {fallback}"
    )
