# Support bundle carries the page figures; shared addresses classed as shared; accept netbox-routing 0.5.1

## Goal

A customer on 3.0.9 quoted figures off the Scope Reconciliation and Drift report pages that could not be found in their support bundle by the page's words, was told 346 devices without a primary IP were fixable by a Mgmt_ tag when the management-address step had recorded another device owning the address, and had every fast path off because they run netbox-routing 0.5.1. Fix all three.

## Constraints

- The support bundle stays name-free: counts, slugs and NetBox primary keys only.
- No migration. No Forward call. No change to which address becomes primary: only how a device without one is explained.
- netbox-routing 0.5.0 stays accepted.

## Touched Surfaces

- `forward_netbox/utilities/bundle_ui_figures.py` (new): the Scope Reconciliation figures under the page's labels with the parts beside each total, the device keys behind every `*_device_ids` set, and the Drift report table under its column headings and badge words.
- `forward_netbox/views.py`: the scope and dependency-preview bundle sections carry `ui_figures`, `device_keys` and `drift_table`; the primary-IP classifier gets the step's per-device outcomes at every call site.
- `forward_netbox/utilities/primary_ip_reasons.py`: a device whose recorded outcome is `shared` or `holder` is "Shares an address with another device".
- `forward_netbox/utilities/validated_runtime.py`, `plugin_integrations/registry.py`, `constraints.txt`, `development/constraints-upgrade-from.txt`, `poetry.lock`, development image comments: netbox-routing 0.5.0 and 0.5.1.
- Tests: `test_log_export`, `test_bundle_ui_figures`, `test_explained_findings`, `test_optional_plugin_versions`, `test_plugin_integrations`.

## Approach

1. The export filter turns any `*_device_ids` or `*_pks` key into a count, so the device keys are carried under `device_keys`, keyed by the set's path without that suffix.
2. The classifier already had the step's outcomes beside it but only used them to annotate; it now uses `shared`/`holder` to decide the reason, because NetBox alone cannot see the owner when it belongs to another sync or sits on an interface outside the count.
3. netbox-routing 0.5.1 adds `pk` to model ordering, aligns owner foreign keys with NetBox 4.7, and moves the OSPF uniqueness message into a constant with the same words (the clash match reads `must be unique per device`, still present).

## Validation

- Each new test fails without its change and passes with it (shown for the classifier: KeyError on the old code).
- `invoke test-isolated` on the routing, version, primary-IP and bundle suites, with the image rebuilt on netbox-routing 0.5.1.
- The full gate at the 3.0.10 cut.

## Rollback

Revert. The bundle loses the added sections, the classifier returns to NetBox-only evidence, and a 0.5.1 runtime returns to the slow paths.

## Decision Log

- Device keys are exported in full up to 5,000 per set: keys are not customer data, and a count says how many devices but never which.
- The Drift table is rebuilt from the same computed report the page renders rather than from the template, so the two cannot disagree on numbers; only the words are duplicated.
- 0.5.1 becomes the tested pin; 0.5.0 stays accepted because nothing in 0.5.1 changes what the adapters write.
