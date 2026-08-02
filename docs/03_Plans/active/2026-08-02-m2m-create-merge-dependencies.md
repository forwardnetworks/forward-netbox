# M2M Create Merge Dependencies

## Goal

Make a branch merge succeed on its first attempt when an existing object update
adds a writable many-to-many reference to an object created in the same branch.
The incident reproduction is an existing DLM CVE whose
`affected_software` update references a newly created SoftwareVersion.

## Constraints

- Preserve NetBox-native and Branching-native ObjectChange replay.
- Model writable M2M dependencies generally; do not special-case DLM CVEs.
- Add an edge only when the serialized M2M value resolves exactly to a
  collapsed CREATE in the same merge. Ambiguous or unsupported values retain
  today's ordering.
- Reject candidate edges that would close a dependency cycle. A missing edge
  retains the known row-isolated failure; a cyclic edge can stop the entire
  merge.
- Do not weaken existing merge, retry, DLM, or relationship tests.
- No migration or persisted-state change.

## Touched Surfaces

- `forward_netbox/utilities/bulk_merge.py`: derive and admit M2M CREATE
  dependency edges during merge graph construction.
- `forward_netbox/tests/test_bulk_merge.py`: first-merge DLM regression and
  multi-hop cycle-safety coverage.
- This active plan records the high-risk merge-path mechanism and validation.

## Approach

1. Reproduce the latent defect: an existing CVE collapses to UPDATE, a new
   SoftwareVersion collapses to CREATE, the CVE postchange M2M contains the new
   primary key, and action priority otherwise schedules UPDATE before CREATE.
2. Inspect only forward `ManyToManyField` values present in a collapsed CREATE
   or UPDATE's `postchange_data`. Resolve dictionary values only through an
   explicit `id`/`pk`; resolve scalar values only by an exact collapsed-change
   key match. Add no edge for missing, ambiguous, non-CREATE, or absent targets.
3. Submit candidate `(dependent, target-create)` edges through the existing
   acyclic dependency admission path before mutating the graph.
4. Make that admission path inspect the transitive closure of existing
   dependencies, so a candidate cannot close a cycle through a multi-hop path.
5. Prove the customer-visible contract with a real branch merge: the first
   `merge_branch()` call succeeds and materializes both the SoftwareVersion and
   CVE M2M relation in main.

The M2M population path entered the product in `6db0596` and first shipped in
v2.5.11. The missing merge dependency is therefore latent and is not a 2.7.0 or
netbox-dlm 0.6.0 semantic regression.

## Validation

- New first-merge regression in `forward_netbox.tests.test_bulk_merge`.
- New multi-hop cycle rejection test for the shared acyclic-edge admission
  helper.
- `forward_netbox.tests.test_bulk_merge` fully green in isolated Compose project
  `forward-netbox-codex-m2m-tests`, with
  `FORWARD_NETBOX_DOCKER_PROJECT=forward-netbox-codex-m2m`.
- `forward_netbox.tests.test_dlm_integration` green in the same isolated project.
- `invoke harness-check` passes.
- Isolated web port is 8143; no shared runtime or customer data is used.

## Rollback

Revert the production helper, its call from dependency graph construction, and
the paired tests. No migration, data rewrite, or branch cleanup is required.
Branches that encountered the old ordering remain retryable after their target
CREATEs have reached main.

## Decision Log

- Rejected a CVE/SoftwareVersion special case because the defect is the merge
  graph's omission of writable M2M references.
- Rejected global CREATE-before-UPDATE reordering because it broadens merge
  behavior without relationship evidence and can disturb established release
  ordering.
- Rejected best-effort coercion of serialized values. Exact in-merge CREATE
  identity is required before an edge is proposed.
- Reused the acyclic edge admission precedent instead of adding edges directly;
  the helper is strengthened to detect cycles through transitive existing
  dependencies.
