# Close the open items from the bundle work: the unseen-device advice, audit text figures, and Health coverage

## Goal

Three items were left open after the bundle work: the advice for devices the management-address step recorded nothing for was hedged because its cause was unread; the audit summary filter hid text figures that are not names; and the page-versus-bundle check compared little on Health.

## Constraints

- Counts and fixed labels only; no names. No migration. No Forward call.

## Touched Surfaces

- `forward_netbox/utilities/primary_ip_reasons.py`: advice for a device the step never saw, and for `tag-unresolved` and `no-management-address`.
- `forward_netbox/views.py`: `_AUDIT_TEXT_FIGURES`.
- `forward_netbox/utilities/bundle_label_index.py`: rows and cards for the Counts explained, Types and ingestion issue pages, and entries for the Uncovered devices and delete pages.
- `forward_netbox/tests/test_explained_findings.py`, `test_log_export.py`, `test_bundle_label_index.py`.

## Approach

`apply_primary_ip_from_mgmt_tags` takes the union of the devices Forward returned a `Mgmt_` tag for and the devices it returned a management address for, both read inside the sync's tag scope and each only when its setting is on, then looks each up in the branch by name. A device outside that union, or absent from the branch under that name, gets no outcome. So "recorded nothing" usually means the step never saw it. Two other paths leave no record either: a device the step set in the branch reads as bare on main until the ingestion merges, and two NetBox devices sharing Forward's name are dropped from the branch lookup. The advice names all three. The other two codes the step writes (`tag-unresolved`, `no-management-address`) get their own advice.

The audit summary rows whose value is text the audit wrote itself (a snapshot id, a selector, model labels, a reason) are kept; every other text value stays out.

The page-versus-bundle test now builds an ingestion with a finished job and model results, compares 19 Health rows instead of a handful, and fails below a per-page floor.

## Validation

- `invoke test-isolated` on the explained-findings, log-export, redaction and label-index suites.
- The full `forward_netbox.tests` suite.

## Rollback

Revert.

## Decision Log

- The text-figure allowlist is by label, not by shape: a name could look like any string.
