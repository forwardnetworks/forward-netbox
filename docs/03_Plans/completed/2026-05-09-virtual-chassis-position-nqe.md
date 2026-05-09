# Virtual Chassis Position NQE

## Goal

Prevent NetBox virtual chassis membership imports from assigning a device to a virtual chassis without `vc_position`.

## Constraints

- Keep the normalization in NQE; the NetBox adapter should receive native NetBox fields.
- Do not mutate or infer this in Python when Forward has enough HA role/member data in the query.
- Preserve the existing query-id workflow for live NQE maps.
- Do not include customer, network, or snapshot identifiers in committed files.

## Touched Surfaces

- `forward_netbox/queries/forward_virtual_chassis.nqe`
- `forward_netbox/tests/test_query_registry.py`
- `docs/02_Reference/built-in-nqe-maps.md`

## Approach

Emit `vc_position` from the virtual chassis map.

For MLAG peer pairs, use the deterministic member ordering already used for `vc_domain`: the first member is position `1` and the second member is position `2`.

For vPC domains, map primary roles to position `1` and secondary roles to position `2`. If the role is absent or unknown, skip the membership row instead of sending a virtual chassis assignment that NetBox will reject.

Update the org library copy of the virtual chassis query by query ID with the same NQE shape.

## Validation

- Focused NetBox test: `python manage.py test forward_netbox.tests.test_query_registry --keepdb -v 2`
- Live NQE execution by query string returned positioned virtual chassis rows with zero missing `vc_position` values.
- Org NQE commit dry-run for the virtual chassis map returned zero new errors.
- Org NQE execution by committed query ID returned positioned virtual chassis rows with zero missing `vc_position` values.
- `invoke sensitive-check`

## Rollback

Revert the local query, test, docs, and this plan file. In the org NQE library, revert the virtual chassis query to the prior source or restore the previous repository commit for that path.

## Decision Log

- Rejected Python-side defaulting: NetBox requires member position, and the query has better access to Forward HA role/member data.
- Rejected emitting virtual chassis without position for unknown vPC role: that reproduces the NetBox validation failure.
