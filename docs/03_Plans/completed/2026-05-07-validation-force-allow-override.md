# Validation Force-Allow Override

## Goal

Add an audited break-glass path for blocked validation runs so operators can record why a policy block was accepted and rerun the sync against the same snapshot and policy.

## Constraints

- Keep validation and drift policy behavior inside the existing `ForwardValidationRun` and `ForwardDriftPolicy` workflow.
- Do not invent a separate merge branch or alternate reconciliation model.
- Preserve NetBox-native UI and API patterns.
- Do not commit customer identifiers, network IDs, snapshot IDs, credentials, or live row examples.

## Touched Surfaces

- `forward_netbox/models.py`
- `forward_netbox/utilities/validation.py`
- `forward_netbox/forms.py`
- `forward_netbox/views.py`
- `forward_netbox/api/serializers.py`
- `forward_netbox/api/views.py`
- `forward_netbox/templates/forward_netbox/`
- `forward_netbox/tests/test_models.py`
- `forward_netbox/tests/test_api_views.py`
- `forward_netbox/migrations/0010_forwardvalidationrun_override_applied_and_more.py`
- `docs/00_Project_Knowledge/validation-matrix.md`
- `docs/01_User_Guide/usage.md`
- `docs/01_User_Guide/troubleshooting.md`

## Approach

1. Add audited override fields to `ForwardValidationRun`.
2. Provide a `force_allow()` helper that records the reviewer, reason, timestamp, and overridden blocking reasons.
3. Teach the validation runner to accept a previously force-allowed run for the same snapshot and policy.
4. Expose the override action in the UI and API.
5. Update docs and tests.

## Validation

- Targeted model and API tests passed.
- `invoke lint`
- `invoke ci`

## Rollback

Revert the model fields, runner logic, UI/API action, tests, migration, and docs. Validation runs will again remain strictly blocked with no operator override path.

## Decision Log

- Chosen: record the override on the validation run itself so the audit trail stays attached to the specific blocked decision.
- Chosen: allow only the same snapshot and policy to reuse a force-allowed validation, so the override does not become a blanket bypass.
- Rejected: moving the override into sync merge or branch code because the operator decision belongs to validation, not to the NetBox branch lifecycle.
