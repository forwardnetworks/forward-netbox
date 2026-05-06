# Interface LAG Membership

## Goal

Model Forward aggregate interfaces as native NetBox LAG interfaces and attach physical member interfaces through `Interface.lag` when Forward reports aggregate membership.

## Constraints

- Keep interface shaping in shipped NQE where possible.
- Use the native NetBox `dcim.interface` model and `lag` relationship.
- Do not infer MTU values when Forward reports a normalized MTU; preserve the source value until Forward exposes a more authoritative jumbo MTU field.
- Do not commit customer identifiers, network IDs, snapshot IDs, credentials, or screenshots.

## Touched Surfaces

- `forward_netbox/queries/forward_interfaces.nqe`
- `forward_netbox/utilities/sync.py`
- `forward_netbox/tests/test_query_registry.py`
- `forward_netbox/tests/test_sync.py`
- `docs/02_Reference/built-in-nqe-maps.md`
- `docs/02_Reference/model-mapping-matrix.md`

## Approach

Emit `type: "lag"` for Forward `IF_AGGREGATE` interfaces and emit a nullable `lag` field on physical Ethernet rows from `interface.ethernet.aggregateId`. Sort `dcim.interface` rows so LAG parent rows apply before member rows inside a single shard, then set the member's native NetBox `Interface.lag` foreign key during adapter upsert. If sharding applies a member before its aggregate row, create a minimal native LAG placeholder and allow the aggregate row to update it later.

Keep MTU mapped to `interface.mtu`. Forward documents this as normalized L2 MTU, and the live APIC case currently reports `1500` there. The plugin should not synthesize jumbo MTU from platform assumptions.

## Validation

- `python -m compileall forward_netbox/utilities/sync.py forward_netbox/tests/test_sync.py forward_netbox/tests/test_query_registry.py`
- Focused Docker Django tests for the interface query contract, same-shard LAG membership, and cross-shard LAG placeholder behavior.
- Live Forward org library query update for `/forward_netbox_validation/forward_interfaces`; committed source was verified to match the local shipped query exactly.
- Live query-by-ID smoke returned aggregate rows as `type: "lag"` and physical rows with populated `lag` values.
- `invoke ci`

## Rollback

Remove the `lag` output column from the interface query, restore aggregate interfaces to `other`, remove interface row sorting and adapter `lag` assignment, and remove the new tests/docs.

## Decision Log

- Chosen: use `interface.ethernet.aggregateId` for physical member-to-LAG assignment because Forward exposes it directly on member interfaces.
- Chosen: use `IF_AGGREGATE` to set NetBox interface type `lag`, matching NetBox's native LAG model.
- Chosen: allow member rows to create minimal native LAG placeholders because multibranch sharding can separate aggregate and member rows.
- Rejected: inferring jumbo MTU in the plugin because Forward's only native MTU field reports `1500` for the observed APIC rows and no alternate schema field was found.
