# Subinterface sync

## Goal

An IP address on a subinterface (a VLAN or VRF sub-unit such as `eth1/52.100`) lands on that subinterface in NetBox, not on its parent. Today the IP queries emit the parent's name for every subinterface address, so a routed port with ten VRF subinterfaces shows ten addresses on the parent, and Forward does not list subinterfaces as interfaces, so NetBox has nothing to attach them to.

## Constraints

- Always on, no source setting. Only a subinterface that carries an address is emitted, which on the measured network is 21,343 of over a million (about 3.6% more interfaces than today), so no off switch is needed. A setting would also make the published queries require a parameter the previous plugin does not send, which fails the sync.
- No customer names, network IDs, snapshot IDs or hostnames in the repo, plans, tests or docs. Tests use synthetic names.
- Query changes are paired with tests and reference-doc updates. A pinned query ID never gets the new text on upgrade, so the health staleness check must name the new parameter and customers must run Publish Bundled Queries.
- A row failing the interface apply must not stop its IPs being reported; the IP is a dependency-skip, not a corruption.
- No fix without a failing test first.

## Touched Surfaces

- `forward_netbox/queries/forward_interfaces.nqe`, `forward_ip_addresses_ipv4.nqe`, `forward_ip_addresses_ipv6.nqe`.
- Production apply is `apply_engine_bulk.py` (`bulk_orm_apply_interface`), not `apply_dcim_interface`; `sync_interface.py` stays in step. `bulk_merge.py` defers `parent` only on the change-merge path, so it is not support for the apply.
- Docs: `docs/02_Reference/built-in-nqe-maps.md`, user guide source settings page.

## Approach

1. Failing tests first: the apply links `parent` in either row order, skips a missing parent and never rewrites a same-named top-level interface; the interfaces query emits only addressed subinterfaces and not a base unit named like its parent; the IP queries report the subinterface name; every union branch of the interfaces query selects the same columns.
2. Interfaces query: add `subinterface_interfaces` rows (name = the subinterface's own name, type `virtual`, `parent` = the parent interface, `enabled` from the subinterface operStatus), only for a subinterface that carries an IPv4 or IPv6 address and whose name differs from its parent. Pad the endpoint and subinterface branches with the full column set: NQE keeps only the columns every member of a `+` union shares, and the endpoint branch (device, name, type, enabled) had been stripping `lag`, `mode`, `untagged_vlan`, `mtu`, `speed` and `description` from every row.
3. IP queries: a subinterface address reports the subinterface's own name.
4. Apply: `bulk_create` needs the parent's pk, so set `parent` in a post-create `bulk_update(fields=["parent"])` phase, following the LAG pattern in `apply_engine_bulk.py`. Row order is query order and nothing sorts parents first, so the deferred link phase is the ordering mechanism. A missing parent is a dependency-skip that calls `_mark_dependency_failed`. The row-by-row adapter (`sync_interface.py`) mirrors it but applies rows in order, so a child that arrives before its parent is skipped there.
5. Collision rule: coalesce is `(device, name)`, so a subinterface row named like an existing top-level interface would update that row. Skip such a subinterface row and report it; never overwrite a top-level interface.
6. Docs (built-in maps reference, troubleshooting), and the post-release step: publish the bundled queries to the org (see the release playbook), since a pinned query keeps the old text.

## Validation

- `invoke harness-check`, `invoke harness-test`, `invoke lint`, `invoke check`.
- Targeted tests, then `invoke test`.
- A live read-only check of the new queries against a real snapshot, on a 40-device sample: no duplicates, no name collisions, no missing parents, every IP row has an interface row, every interface row has all 11 columns.
- Measured on the same network: about 596,000 top-level interfaces, over a million subinterfaces, 21,343 subinterfaces with an address on 1,964 devices.
- Verified live: a Forward subinterface has `name`, `operStatus`, `networkInstanceName` and its own addresses.
- Not verifiable without the reporting customer's data: their VRF-subinterface switch case; the stand-in is a firewall.

## Rollback

Revert the release and republish the previous queries. Subinterfaces already created stay as ordinary interfaces and are removed by the normal absent-row delete path once the queries no longer emit them, child before parent.

## Decision Log

- First planned as an opt-in source setting because the interface count can multiply. Measured instead: only 21,343 of over a million subinterfaces carry an address, so emitting only those grows the interface count by about 3.6% and the setting is unnecessary. Dropping it also avoids a published query requiring a parameter the previous plugin does not send, which fails the sync.
- Delete path: `dcim.interface` is in the diff-removal models, so a subinterface that stops being emitted is deleted through the normal diff, child before parent.
- Not setting the interface `vrf` in this change: IPs already carry their VRF.
- The column padding fixes a bug that predates this change (the endpoint branch stripped the columns). It changes what the next sync writes: LAG membership, VLAN modes, speeds, MTUs and descriptions start arriving.
