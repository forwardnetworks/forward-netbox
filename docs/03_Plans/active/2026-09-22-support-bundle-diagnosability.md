# Make the support bundle answer the questions we actually had to ask

## Goal

A customer upgraded to v2.9.7 and their sync broke twice. Both root causes were
found by probing their live Forward org with admin credentials over ~2 hours.
Neither was obtainable from the support bundle, which exists precisely so that
is unnecessary. Close both blind spots.

**Failure 1 - stale published query (already fixed at source).** Their maps
resolve through queries published in their Forward org repository. v2.9.7
changed the `@query` signature of `forward_interfaces.nqe` and
`forward_ip_addresses_ipv4.nqe` from one parameter to seven. Upgrading the
package never rewrites the published copy, so the plugin sent seven parameters
to a query declaring one and Forward returned HTTP 400. The operator saw only
`Dependency preview query validation failed for 2 model(s): dcim.interface,
ipam.ipaddress` - the names, never the reason - and the bundle could not even
name which query ID got the 400.

**Failure 2 - constraint violation with no row identity (still open).**
`bulk_create` hit `dcim_device_unique_name_site`; the issue row carried the
constraint name and raise site and nothing else. The Forward side was ruled out
by pulling all device rows for that network and finding zero duplicate
`(name, site)` pairs under exact, casefolded and name-only comparison, so the
new row collides with a device already in their NetBox - which only their
database knows.

The blind spot: five bulk paths re-raise when `branch_active`, because branch
rows, ObjectChanges and ChangeDiffs are one transaction and cannot be re-driven
through per-row saves. On main `_isolate_bulk_objects` records the row's name
and pk; inside a branch - every real sync - nothing does.

## Constraints

- **Two tiers of disclosure.** The GUI, the REST API and the server log are
  in-deployment reads and may show the values behind a failure. Anything that
  leaves as a FILE carries primary keys, constraint and field names, counts and
  shapes - enough to say `dcim.Device pk=12345 already holds this (name, site)`
  without naming a customer's network. When we need the name behind a pk we ask
  for that one pk.
- The bundle is generated from stored state and makes no live Forward calls.
  It must stay that way.
- Forward API call volume is audited; no change may add per-map calls on a
  healthy run.
- No customer names or identifiers in this plan, the commits or the PR.

## Touched Surfaces

- `forward_netbox/utilities/export_redaction.py` (new) - `export_safe_payload`,
  `OPERATOR_DETAIL_KEY`.
- `forward_netbox/utilities/diagnostics.py` - `assert_export_safe_diagnosis`,
  int-preserving `safe_diagnosis` lists.
- `forward_netbox/utilities/sync_reporting.py`,
  `forward_netbox/utilities/sync_orchestration.py` - operator tier plus the
  write-time invariant on both issue writers.
- `forward_netbox/views.py`,
  `forward_netbox/utilities/support_bundle_archive.py` - the two export exits.
- `forward_netbox/api/serializers.py` - tier-1 exemption, documented.
- `forward_netbox/utilities/constraint_diagnosis.py` (new) and the five bulk
  paths in `forward_netbox/utilities/apply_engine_bulk.py`.
- `forward_netbox/utilities/query_binding_resolution.py`,
  `forward_netbox/utilities/query_fetch_execution.py`,
  `forward_netbox/models.py`.
- Tests: `forward_netbox/tests/test_export_redaction.py` (new) and per-commit
  additions alongside `tests/test_issue_diagnosis.py`.

## Approach

Five sequenced commits.

1. **Export choke point plus write-time invariant.** `JsonResponse(` occurs
   exactly once in `views.py`, inside `_download_json_response`, with six
   callers - including the dependency-preview `?format=json` and the log
   export, **neither sanitized at all** before this. That helper plus
   `support_bundle_zip_response` (which builds its own bytes) are the only two
   exits. `export_safe_payload` drops the reserved `operator_detail` key and
   absorbs the drop/count suffix rules that `_bundle_safe_report` used to carry
   privately, which now delegates - one filter, not two. `record_issue` and
   `_record_forward_sync_failure` assert the invariant before writing.
   `unrecognized_validation_rules` is exempt by name because
   `redacted_message_shape` already masked it with a STRICTER rule (purely
   alphabetic tokens only).
2. **Constraint diagnosis, value-free tier.** New
   `annotate_integrity_error(exc, model, *, create_objects, update_objects,
   using, limit)`: resolve constraint to fields via named `_meta.constraints`,
   else database introspection for `unique_together` auto-names, else record
   `constraint_fields_resolved: False` and stop - never guess. A `Counter`
   finds in-batch duplicates with no query; chunked SELECTs find collisions
   with rows already in NetBox. SELECTs only, legal on the branch alias because
   the inner `atomic` savepoint has already rolled back. Records
   `exc.netbox_pk` and `exc.safe_diagnosis`, the two conventions issue writers
   already consume. Wired at all five sites immediately before
   `if branch_active: raise`, which stays bare; the whole body cannot raise.
3. **Operator tier.** `exc.operator_detail` carried into
   `raw_data["operator_detail"]`, rendered on the issue page, dropped on
   export.
4. **Parameter-signature drift.** `_live_drift_result_from_committed_query`
   already holds both the committed and the bundled source and compares only
   normalized text; adding `declared_query_parameters()` on both sides costs
   zero API calls. New status `live_query_id_parameter_mismatch`, severity
   `danger`, outranking `source_modified` because it is the one producing a
   hard 400. Persisted on the map so the bundle reads stored state. Also stop
   withholding `query_id`/`commit_id` while exporting a raw `query_path` - that
   is backwards, since the path can embed org names and the ids cannot.
5. **Stop discarding what is already captured.** The dependency preview keeps
   only `model_string`; carry `failure_exception`, `failure_reason` and
   `query_path_resolution` through. `plan_item_model_result` drops
   `query_parameters` and the fingerprints - keep them.

Deliberately out of scope: runtime and version facts, which
`_environment_bundle_payload` already puts in the bundle.

## Validation

- Full `invoke ci` on an isolated compose project before push.
- Per-commit targeted runs; commit 1 is green at 62/62 across
  `test_export_redaction`, `test_issue_diagnosis` and `test_log_export`.
- Commit 2 must include a test that provokes a real `IntegrityError` on the
  branch alias and then issues a SELECT, pinning the savepoint assumption - a
  refactor flattening that nesting would turn every diagnosis into
  `InternalError: current transaction is aborted`.
- End-to-end: a device batch whose row collides with an existing
  `(name, site)`, applied inside a branch, must produce an issue naming
  `constraint_fields` and a conflicting pk, while the exported bundle contains
  no device name.
- Export safety: a fixture carrying `operator_detail` must be absent from both
  the JSON and the zip byte streams.

## Rollback

Five independent commits. Commit 1 is additive at the export boundary and
reverts cleanly on its own; commits 2 and 3 are paired (3 writes the key 1
drops and 2 populates); 4 and 5 are independent of the rest.

## Decision Log

- **Two-tier rather than pk-only or values-everywhere.** Product owner's call:
  keep customer data redacted in exports, but carry enough to troubleshoot.
  Primary keys are the mechanism that satisfies both - and `exc.netbox_pk`
  already existed, rendering `Affected NetBox row: pk N.`; nothing had ever
  populated it from a bulk constraint failure.
- **The invariant is honest about its limit.** A token pattern cannot tell
  `core-sw-01` from a slug. It rejects sentences and nested payloads, which is
  the accident worth preventing; the disclosure boundary is `operator_detail`
  plus the export filter, not the pattern.
- **All five bulk sites together.** This repo has shipped the same bug twice by
  fixing one guard branch and leaving its siblings.
- **The REST API stays tier 1**, documented in the serializer so it is not
  "fixed" later: an authenticated in-deployment read is the same trust level as
  the GUI, and the operator troubleshooting their own estate is why the values
  exist.
