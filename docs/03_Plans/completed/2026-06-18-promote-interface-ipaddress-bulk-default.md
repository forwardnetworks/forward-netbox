# Promote dcim.interface and ipam.ipaddress to the Default Bulk-ORM Safe Set

## Goal

Move the two highest-volume models — `dcim.interface` and `ipam.ipaddress` —
from experimental opt-in to the default bulk-ORM safe set, so standard syncs get
their batched-write speedup without an explicit allowlist. These are the largest
ingest contributors, so default bulk is where most of the throughput win lives.

## Constraints

- Only promote models that carry adapter-vs-bulk parity tests AND
  compare-before-write (no update churn). Both qualify after the recent
  hardening (parity tests + churn fixes; interface also has a LAG-delegation
  parity test).
- `ipam.prefix` stays experimental and adapter-required (IPAM hierarchy /
  `_depth` parity not yet proven).
- No change to the bulk apply implementations themselves — only classification.

## Touched Surfaces

- `forward_netbox/utilities/apply_engine_decision.py` — add `dcim.interface`
  and `ipam.ipaddress` to `BULK_ORM_ENABLED_MODELS`; shrink
  `EXPERIMENTAL_BULK_ORM_MODELS` to `{ipam.prefix}`; base
  `APPLY_ENGINE_MODEL_CLASSIFICATIONS` on `BULK_ORM_ENABLED_MODELS` (superset of
  the simple set) so the promoted custom-spec models classify as
  `bulk_orm_candidate`.
- `forward_netbox/tests/test_bulk_adapter_parity.py` — add LAG-membership
  delegation parity test for interface.
- `forward_netbox/tests/test_sync.py`, `test_health.py`,
  `test_architecture_audit_command.py` — update classification / expansion-status
  expectations (status is now `blocked_pending_parity` because the only remaining
  experimental model is adapter-blocked).
- `docs/01_User_Guide/configuration.md` — safe-set list.

## Approach

The bulk dispatch already routes `ipam.ipaddress`/`dcim.interface` to their
specialized apply functions; promotion is purely a classification move. With the
models in `BULK_ORM_ENABLED_MODELS`, `enable_bulk_orm` (and the
fast-bootstrap/branching auto-enable) select bulk for them by default. The
classification map now keys off `BULK_ORM_ENABLED_MODELS` so the promoted models
are not reported as unclassified.

## Validation

- `forward_netbox.tests.test_bulk_adapter_parity` (create/update/LAG/macaddress/
  churn parity), `test_apply_engine`, `test_health`,
  `test_architecture_audit_command`, `test_sync`.
- Full `forward_netbox.tests` suite.
- `invoke harness-check`, lint.

## Rollback

Move both models back to `EXPERIMENTAL_BULK_ORM_MODELS` and revert the test
expectations. No schema, data, or migration impact; default-off opt-in is the
prior behavior.

## Decision Log

- Promote both together: both are parity-covered and churn-free, and they are
  the top-two volume models, so the perf benefit is concentrated here.
- Hold `ipam.prefix`: NetBox Prefix hierarchy/`_depth` recomputation and
  parent-prefix semantics are not yet parity-proven for bulk writes.
- Classification map rebased on `BULK_ORM_ENABLED_MODELS` rather than
  `SIMPLE_BULK_CANDIDATE_MODELS` so custom-spec bulk models (interface/ipaddress)
  classify correctly without being mislabeled simple.
