# Site Tag Override

> To map locations or tags to sites from the Forward source settings, without tagging
> devices in Forward, see [Site Mapping Rules](site-mapping-rules.md). The tag
> convention below still works, and wins over a rule for the device that carries it.

Forward knows a device by its **location**. NetBox sites are often coarser: one
data centre can be dozens of Forward locations (halls, fabrics, zones). By default
the sync creates one NetBox site per Forward location. Tag devices to group
several locations into one NetBox site instead.

## The convention

Give a device a Forward device tag named `NetBox_Site_<name>`:

| Device | Forward location | Tag | NetBox site |
| --- | --- | --- | --- |
| core-1 | `HQ_Core` | none | `hq_core` |
| leaf-7 | `HQ_Fab1` | `NetBox_Site_HQ-Campus` | `hq-campus` |
| leaf-9 | `HQ_Fab2` | `NetBox_Site_HQ-Campus` | `hq-campus` |

- A device with no site tag keeps its location, exactly as before. Nothing changes
  until a tag is applied.
- The tag wins over the location. Several locations can share one tag value.
- The site name is lowercased, like every site name the sync creates, and the slug
  is derived from it. The prefix is case sensitive: `netbox_site_x` is not a site
  tag, and a tag with nothing after the prefix is ignored.
- A device with more than one site tag takes the first alphabetically, so the
  result never depends on tag order. Give each device one.
- A device with no location and no site tag goes to the site `unknown`, as before.
- SNMP endpoints follow the same rule.

## Placing devices at a site another system created

If another system (an inventory import, a person) already created the site in
NetBox, tag the devices with that site's **slug**, for example
`NetBox_Site_dc1`. The sync finds a site by slug first, then by name, and uses an
existing site exactly as it is:

- it never renames a site, or changes its slug, once the site exists;
- it only deletes a site once nothing NetBox would cascade into through a foreign
  key still references it (otherwise the delete would take prefixes, clusters and
  change-policy rules scoped to the site with it). Many-to-many links, such as a
  config context scoped only to the site, and generic links such as contacts or
  journal entries are not checked: deleting the site removes them;
- it never deletes a site that NetBox's change log shows another actor created,
  even after it stops returning it (a warning names the site instead, on each run
  until the site is removed or returned again). That includes a site another
  Forward sync created.

The change log is the proof, so once a create record ages out of NetBox's change-log
retention the guard no longer sees it. **Prune orphans** removes empty sites without
this guard.

A site the sync created itself is still removed once Forward stops returning it and
nothing references it. One consequence of never renaming: if a Forward location is
renamed in a way that keeps the same slug (only spacing, punctuation or case
changes), the NetBox site keeps its old name; rename it in NetBox if you want the
new spelling.

## What it covers

The tag decides the site everywhere the sync names one: the device map, the
VLAN map (VLANs are site-scoped), the site list, and the scope and orphan checks
on Scope Reconciliation. A site created from a tag has no Forward location of its
own, so its address comes from the Forward location with the same name if there is
one, otherwise from its first location by name.

Cisco ACI fabric names are not sites and are not affected: a fabric is still named
from the APIC's location.

## Applying it

Apply the tags in Forward, by whichever route you use for device tags (a tag rule
on location name, the UI, or the API). Then:

1. Run the sync. Devices move to the tagged site, which is created if it does not
   exist. Their interfaces are written again in the same run, so each untagged VLAN
   points at the new site's VLAN (the old site's VLAN cannot stay on an interface
   of a device at another site). Preview the run first: every tagged device moves.
   This holds when the interface map fetches its rows in full, which is the
   default. An interface map that reads Forward's own snapshot diff only receives
   rows that changed, so a moved device's interfaces are not re-applied; run that
   sync once with a full fetch after the move.
2. The sites the devices left: the sync deletes a site it produced once Forward
   no longer returns it and nothing references it. A site that is still
   referenced (by VLANs, racks or other objects) is skipped and tried again on the
   next run, so run the sync again. A site the sync never produced is not deleted by it
   (as far as NetBox's change log can show: see above). **Scope Reconciliation** counts empty ones as empty orphan sites, and
   **Prune orphans** (dry run first) removes them.

**Republish before you tag.** An organisation whose maps are bound to saved queries
runs those saved copies. Tag devices only after the republished queries are in place;
otherwise the device-tag scope check names the tag's site while the old maps still
name the location, and every run records warnings for the location sites it tries and
fails to remove.

If you use the shipped queries from the plugin, republish them after upgrading:
**Export Live Query Drift**, then **Publish Bundled Queries**, then run the sync.
An organisation that keeps its own copies of the maps needs the same change in
those copies: use the `deviceSiteName` helper from `netbox_utilities` wherever the
copy derives a site from `locationName`.

## Checking it

Scope Reconciliation compares each NetBox device's site with the site the sync
would put it at. After tagging, a device that has not moved yet shows as a
mismatch until the next sync; after it, the mismatch count should return to zero.
