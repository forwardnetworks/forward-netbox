# Back up the configurations Forward holds only as a parsed tree

## Goal

The config backup fetches each managed device's collected configuration
command. Forward holds no such command for some platforms, so on one estate a
backup produced 2,743 files for 3,555 devices in scope, and 767 of the 813
without a file had a configuration Forward had parsed: 578 Cisco NX-OS
switches in ACI mode, 133 F5 hypervisors and 56 Cisco APIC controllers. Back
those up too, and include endpoints whose collected CLI output is a
configuration, without making the query depend on either existing.

## Constraints

- The query's one parameter, `forward_netbox_shard_keys`, is unchanged, so no
  published copy and no bundled-query signature changes.
- A single NQE record field is capped at 1,000,000 bytes and one parsed tree
  is about 5 MB as JSON, so the tree is rendered to text inside the query.
- The tree nests at most six levels on every estate measured. Anything deeper
  is marked in the file rather than dropped silently.
- It must not fail on an estate without ACI devices, hypervisors or endpoints:
  every branch reads fields that exist in Forward's schema and emits no row
  when there is nothing to read.
- Empty list literals are rejected by Forward at runtime, so none are used.
- No customer names, hostnames or counts beyond the aggregate above in the
  committed content.

## Touched Surfaces

- `forward_netbox/queries/forward_config_backup.nqe`
- `forward_netbox/tests/test_config_backup_query.py`
- `docs/01_User_Guide/configuration.md`
- `scripts/tests/test_verify_release_provenance.py` (main only, see Decision Log)
- this plan

## Approach

Two row sources joined in one `@query`. Devices emit the text of their
`CONFIG` command, or, when they have none, their parsed `files.config` tree
rendered with two-space indentation. Endpoints emit their configuration-style
CLI responses (`show run*`, `show config*`, anything containing
`running-config` or `current-configuration`) joined into one file. Each level
of the render adds its own leading newline and joins its children with
`join("", ...)`, so no conditional is needed for a node without children.

## Validation

- Run live against a real network: 3,509 rows for 3,555 in-scope devices (the
  2,742 that already had a command plus 767 recovered), no errors, largest
  config 20.7 MB, in about 12 minutes at the job's page size.
- On NX-OS the rendered tree matches the collected text line for line.
- The rendered text of the devices-only and the final query are identical on a
  sample across NX-OS, APIC, F5, IOS-XE, Arista, Palo Alto and Fortinet.
- Not exercised on real data: the endpoint branch, because no endpoint on the
  network used for validation carries CLI output. It compiles and returns no
  rows there.
- Full `invoke ci`.

## Rollback

Revert the commit; the query returns to fetching only collected commands.

## Decision Log

- **Rendered in NQE, not in the job.** The field cap makes the raw tree
  unfetchable, and server-side rendering keeps the job unchanged.
- **The parsed rendition is the fallback, not the preference.** A collected
  command is what the device returned; the tree is Forward's parse of it.
- **Endpoints are conditional on CLI output.** Forward collects endpoints over
  SNMP by default, which carries no configuration text, so nothing is invented
  from SNMP descriptions.
- **Provenance test fixture fixed on main in the same change.** Advancing the
  anchor to `v3.0.1` made `main`'s scripts suite fail thirteen tests, because
  its fixture hard-coded `v3.0.1` as the release under test and the anchor is
  now that tag. Every push to `main` runs that suite, so nothing could land
  until it was fixed. The 2.9.x lane already derives the tag under test from
  the anchor; `main` takes the same test file.
