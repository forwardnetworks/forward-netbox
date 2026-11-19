# Site Mapping Rules

Forward knows a device by its **location**. NetBox sites are often coarser: one data
centre can be dozens of Forward locations (halls, fabrics, zones). **By default a
device's NetBox site is its Forward location**, so the sync creates one NetBox site
per Forward location. Site mapping rules are how you change that, in the Forward
source settings, without writing anything in Forward.

## The rules

On the Forward source, the **Site mapping rules** field takes one rule per line:

```text
location HQ_* -> hq-campus
location Annex_Lab -> annex
tag Prod_Core -> core
```

| Rule | Matches a device when |
| --- | --- |
| `location <pattern> -> <site>` | its Forward location matches the pattern, ignoring case |
| `tag <tag> -> <site>` | it carries that Forward tag, exactly |

- A location pattern is a glob: `*` matches any run of characters, `?` one character
  and `[12]` one of the listed characters. A pattern of just `*` is refused, since it
  would put every device in one site.
- A tag is chosen from a dropdown filled from Forward's own tag list (the field's
  **Add a rule** builder; save the source first so it knows which network to ask).
  The textarea also accepts a tag typed by hand.
- A location pattern is typed, with Forward's location names offered as suggestions
  as you type (the same builder; they are only suggestions, since a rule is a
  pattern such as `HQ_*`, not one location).
- The site is the NetBox site name. It is lowercased and slugged like every site
  name the sync creates.
- At most 100 rules. The same location pattern or tag twice, and two site names that
  make the same slug, are refused when you save.

## Which rule wins

For each device the first match wins:

1. A `NetBox_Site_<name>` tag on the device (see [Site Tag Override](site-tag-override.md)).
2. The first rule that matches, top to bottom.
3. The device's Forward location, as before.

A device that matches nothing keeps its location, so adding a rule never moves a
device it does not match. The map of devices, VLANs, locations and SNMP endpoints all
follow the same order.

## Before you save

**Publish the bundled queries first.** The rules reach the queries that name a site
(devices, VLANs and locations). A query published to your Forward organization before
this feature does not take them. With no rules set that does not matter: the sync
runs it as it always did. With rules set, nothing is applied for that run, and the
sync says which map: if the plugin can see the query is out of date it stops before
fetching, and if only Forward's own rejection shows it, the run is blocked from
merging. The devices are never quietly placed by their Forward location instead.
Forward NQE Maps -> **Publish Bundled Queries**, then refresh the query IDs. Health's
query drift section lists a published query that is missing the `site_rules`
parameter.

A map that runs your own query instead of the bundled one takes the rules only if
the query declares a `site_rules` parameter in its `@query` line **and** the map is
saved with a `site_rules` parameter (value `[]`; the plugin fills it in). Without both,
the sync stops before fetching, and Health's **Site rules** check names the model.

## Check them on the Scope page

Scope Reconciliation shows a **Site Mapping Rules** card after the next Refresh: how
many devices each rule places, how many carry a `NetBox_Site_` tag, and how many stay
at their Forward location. A rule that places no device is flagged: usually a
misspelt pattern, or a Forward tag no device carries.

The support bundle carries the same figures under the same labels, as counts: each
rule's position, kind and device count. The patterns and site names are not exported.

## What a change does

Devices move to the new site on the next sync, and each moved device's interfaces are
re-applied so their untagged VLANs point at the new site's VLANs, as for a
`NetBox_Site_` tag. A sync that moves devices fetches the full inventory: the
incremental diff is not used while rules are set, because its queries cannot carry
them.

- The sync never renames a site it matched, and never deletes a site another system
  created or one that something still references.
- The sites a move empties, and the VLANs left on them, stay in NetBox. After the move,
  **Prune orphans** (dry run first) removes the empty sites.
- Changing the rules is a change of scope: the first sync after takes a full pass
  rather than an incremental one.
- Removing every rule puts devices back at their Forward location on the next sync.
- Two Forward sources that cover the same devices with different rules would move
  them back and forth on every run. Give a device one source.

## If a sync stops

The full message, with the map's name and what to do, is on the sync **job's page** in
NetBox (the job's log). The plugin's own Logs tab and the log export redact warning
text, because a map's name is your own word, so they show a second, shorter line for
each refusal that carries only the model, the reason label and what to do. The
support bundle shows the reason label.

| Message or reason label | Meaning | Fix |
| --- | --- | --- |
| `site-rules-require-diff`: "...need a full fetch ... `Require diff`" | The sync's diff fallback mode is `Require diff`, which refuses the full fetch that site rules need | Set the sync's diff fallback mode to `Allow full fallback`, or remove the rules |
| "The site rules saved on the Forward source are not valid" | Rules saved by hand (the REST API) that the form would have refused | Edit the source and correct the named rule |
| `site-rules-query-rejected`, `site-rules-query-not-declared`: "...rejected the site mapping rules" or "this query does not take them" | A query in your Forward organization predates the rules | Publish Bundled Queries, refresh the query IDs, or remove the rules |
| `site-rules-custom-map`: "...runs a custom query that does not take them" (and Health: "Site rules ... cannot apply them") | An enabled map for devices, sites or VLANs is your own query, which cannot take the rules | Enable the bundled map for that model, declare `site_rules` on your query and in the map's parameters, or remove the rules |

The validation page for the run gives the same advice for the cause it found.

## Upgrading

The parameter is new, so a Forward organization that runs the shipped queries needs
them published once. A source with no rules is unaffected by the upgrade, whether or
not the queries were republished, and its durable state is unchanged.
