# COPY/SQL Port and IPAddress Clean Cost

## Goal

Land the proven COPY/SQL apply engine on the 2.6.6 tree, restore the reference
MAC engine behaviour it depends on, and remove the per-row validation cost that
actually dominates IPAddress baseline shards.

## Contract

- COPY/SQL stays **off by default**: per-sync opt-in, per-model kill switch,
  `dcim.macaddress` only, exact runtime tuple, active provisioned branch.
  Anything else selects the existing engine.
- `bulk_orm_apply_macaddress` no longer aborts a whole plan item because one row
  fails validation.
- IPAddress creates produce the same validated rows as before, with fewer
  queries — no validation is skipped.
- `netbox_branching` stays in the ingestion path.

## Constraints

- Do not enable COPY/SQL for any model by default, and do not widen the
  allowlist beyond `dcim.macaddress`.
- Do not weaken `full_clean()` coverage on the IPAddress create path.
- The backup tree is 2.6.1-era: port hunks, never whole files.

## Touched Surfaces

- `forward_netbox/utilities/apply_engine_copy_sql.py` (new)
- `forward_netbox/utilities/apply_engine{,_decision,_bulk}.py`
- `forward_netbox/{choices,forms}.py`,
  `forward_netbox/utilities/{model_validation,sync_facade,health_summary_blocks}.py`
- `forward_netbox/tests/test_copy_sql_apply_engine.py` (new) and the form/model/
  facade test additions
- `scripts/benchmark_copy_sql_mac.py`, `docs/02_Reference/apply-engine-model-matrix.md`
- This plan.

## Approach

**1. Port, do not copy.** `~/forward-netbox-copysql-backup-20260727` is based on
a 2.6.1-era tree. Three files would have silently regressed current behaviour if
taken wholesale — the backup predates `has_diff_commit_id` in the health summary,
the fast-baseline sync parameters, and the diff-contract validation in
`model_validation`. Those were merged hunk-by-hunk instead.

**2. Restore the reference MAC engine's missing companion hunks.** The ported
paired-branch test failed with `Cannot reassign MAC Address while it is
designated as the primary MAC for an object`, raised from
`bulk_orm_apply_macaddress`. This is **not** a regression: the containment it
needs never existed in any commit (`git log -S"ambiguous_mac_keys"` → zero); it
lived only as uncommitted working-tree changes in the backup, made during the
WP-A work. Two pieces:

- *Ambiguity detection.* Two persisted rows can share a canonical MAC. The
  prefetch was last-write-wins, so the customer's assignment landed on whichever
  row the query happened to return last. Now recorded and rejected explicitly.
- *ValidationError containment.* Both `full_clean()` calls are wrapped. This
  matters more than it looks: the `except IntegrityError` handler below
  re-raises unconditionally while a branch is active, so the update path had no
  containment at all and one bad row killed the entire plan item. On the update
  path the assignment is restored and `_prechange_snapshot` deleted before
  recording, or a row that was never written still contributes a prechange
  snapshot to the branch diff.

Deliberately **not** ported: the backup's `get_prep_value`/`valid_mac_values`
pre-filter. `_is_parseable_mac` (2.6.5) supersedes it and is better — it rejects
the offending row individually with an issue instead of dropping it silently.

**3. IPAddress create cost.** The recorded suspicion — that `address__net_host`
is an unindexed lookup and that index maintenance dominates — is **wrong on both
counts**. `ipam_ipaddress` carries `btree((host(address)::inet))`, NetBox's
`NetHost` lookup emits `CAST(HOST(...) AS INET) = %s` to match it (index scan
0.062 ms at 1M rows), the prefetch is batched 500 hosts per query, and
`bulk_create` is ~1% of shard time.

The cost is `ip.full_clean(...)` on the create branch: `validate_unique=False`
does not suppress `Model.clean()`, and NetBox's `IPAddress.clean()` issues ~9
queries and ~6.9 ms per created row. Two of those queries are pure waste —
passing `assigned_object_type`/`_id` to the constructor leaves the generic-FK
`fields_cache` cold, so `clean()` refetches the interface and its device, and
`__init__` records `_original_assigned_object_*`, which makes `clean()` run its
"reassigning a primary IP" block on an object that has never been saved (it
compares `None` to `None`). Assign through the descriptor and clear those two
attributes.

## Validation

- `test_bulk_merge`, `test_set_based_merge`, `test_copy_sql_apply_engine`,
  `test_forms`, `test_models`, `test_sync_facade`: **273 tests, OK.**
- Trusted baseline for comparison: committed HEAD, run in isolation, 96 tests OK
  (`test_bulk_merge` + `test_set_based_merge`).
- `scripts/tests`: 247 tests OK. `check_harness.py` passed.
  `check_release_preflight.py` passed.
- IPAddress Fix A measured by the investigation at 4,004,000 destination rows:
  6.928 → 4.453 ms/row, 9.02 → 6.00 queries/row (synthetic local data).

## Rollback

Leave `enable_copy_sql` off — the default — or add `dcim.macaddress` to
`copy_sql_kill_switches`; both select the existing engine without changing
planned rows or branch lifecycle. No migration and no persisted state are
introduced. The MAC and IPAddress changes revert with the commit.

## Decision Log

- 2026-07-30: Ambiguous MAC identity is dispositioned **`skipped`**, not
  `failed`. It was first written as `failed` to match what
  `bulk_orm_apply_macaddress` already does for unparseable MAC and missing
  device — but that matched the *shape* of the neighbouring cases without asking
  what distinguishes this one. Those are defects in the **incoming row**, so
  failing them is correct. A duplicate canonical MAC is a pre-existing condition
  in the **destination**: failing it would convert a latent duplicate a NetBox
  has carried for months into a wedged sync on the next upgrade, because per the
  merge-failure dead end any failed row permanently blocks baseline promotion.
  Skipping leaves the assignment unconverged and visible in drift, which an
  operator can act on without the sync being stuck. The row is still reported as
  an issue either way. Parity is unaffected — COPY/SQL routes these rows back
  into this same function, so both branches run identical code; verified at 273
  tests OK under both dispositions.
- 2026-07-30: `full_clean()` failures keep `outcome="failed"`. Those *are* faults
  in the row being applied, and that path previously had no containment at all.
- 2026-07-30: IPAddress Fix B (replace `full_clean()` with `clean_fields()` plus
  re-implemented checks; measured ceiling 19.3×) **deferred**. It breaks the
  row-for-row adapter-parity contract this function documents at its docstring,
  and its safety argument depends on the plugin's prefetch staying in lockstep
  with NetBox's `get_duplicates()`. Needs parity tests and explicit sign-off.

## Evidence

- The 8.98× MAC benchmark and the paired-branch equivalence proof are in
  `docs/03_Plans/completed/2026-07-25-copy-sql-mac-wp-a.md`, unchanged.
- Destination-size scaling for IPAddress shards was **refuted** by measurement:
  an equal 4,000-row all-create shard against a table grown 0 → 4M rows moved
  28.8 s → 34.6 s (+20%), not the +112% the reported 70.2 → 149.0 s implies. The
  leading explanation for that incident is the create/unchanged mix (create
  6.93 ms/row vs update 0.358 ms/row, a 19× spread), which "equal shard size"
  does not hold constant. That is a hypothesis — it was never reproduced against
  the original campaign's data.
