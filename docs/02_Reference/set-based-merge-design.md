# Set-Based Forward Merge Design

## Status and invariant

This design extends the existing custom merge; it does not replace
`netbox_branching`. The provisioned branch, its ObjectChanges, ChangeDiffs, and
dependency graph remain authoritative. The set-based path is disabled by
default, exact-version allowlisted, independently killable per model, and
fail-closed. A family without paired-branch parity evidence remains on the
current path.

The first executable family is the Forward-owned `dcim.macaddress` assignment
contract. Its specification version is independent of later families so a MAC
kill switch does not affect their current-path behavior.

## Range materialization and ordering

Upstream squash/collapse and Forward's Kahn ordering continue to produce the
logical merge sequence. The dispatcher forms bounded, consecutive,
model-homogeneous ranges only after that ordering. It flushes a range before a
different model, a dependency-sensitive synthetic component, or the existing
merge flush threshold. Row-ineligible MAC changes remain at their exact ordinal
and are classified to current-path fallback before target mutation. Thus every
committed range derives from the existing dependency order; SQL grouping never
invents a dependency. If an accepted MAC depends on a rejected MAC, rejection
cascades to the dependent row so the current-path dependency cannot be
overtaken. Independent rows may be applied set-wise before rejected rows; the
MAC specification proves there is no hidden same-family ordering contract for
those rows.

Each range is copied to PostgreSQL temporary tables with:

- stable range ordinal;
- branch ContentType/object primary-key identity and logical action;
- full and clean original/modified payloads plus a payload hash;
- branch-modified field mask;
- resolved scalar, FK, and GFK identities;
- eligibility/rejection code;
- locked destination pre-state, chosen operation, and post-state.

Temporary tables are transaction-scoped and `ON COMMIT DROP`. A branch/model
transaction advisory lock prevents two workers from applying the same model
range concurrently. Target and evidence rows are then locked in deterministic
primary-key order.

## FK, GFK, M2M, and identity rules

The logical merge identity is always the collapsed ObjectChange key:
`(ContentType, object ID)`. Human-readable or natural keys are resolution data,
not a license to coalesce a different destination row. Existing-create replay
therefore requires the current path's AppliedChange/ObjectChange provenance;
matching values alone do not prove identity.

Ordinary FKs are resolved as `(target ContentType/model, target PK)` and locked
before target DML. GFKs add an allowlisted ContentType discriminator and a
locked referenced object. A missing, ambiguous, wrong-type, concurrently
deleted, or model-unsupported reference rejects the whole identity bucket to
the current path. WP-A permits a null assignment or the exact `dcim.Interface`
GFK only; VMInterface and arbitrary GFK targets remain current-path operations.

For a parity-approved future M2M contract, desired through-row identities will
be materialized in a separate temp table. Guarded anti-joins will delete only
relationships owned by the branch change and `INSERT ... SELECT ... ON
CONFLICT DO NOTHING` will add the desired set. The target scalar row, through
rows, audit, AppliedChange, and ChangeDiff updates must share one transaction.
WP-A does not claim M2M writes: MAC tag changes and any relation-bound delete
stay on the current Collector/M2M path. Existing unchanged tags and side-table
rows may be preserved by an otherwise eligible update and are covered by the
paired proof.

## Concurrent-main and lineage rechecks

The merge changes branch status to `merging` before application. A main edit
after that transition may no longer refresh the source ChangeDiff, so the SQL
path cannot trust cached `current` blindly. Under locks it:

1. locks and verifies exactly one source ChangeDiff whose
   original/modified/action matches the collapsed branch evidence;
2. locks the actual destination row and related FK/GFK rows;
3. serializes the actual destination pre-state as the authoritative merge-time
   `current` state;
4. applies only fields changed between branch original and modified payloads;
5. captures the locked post-state for destination audit.

This reproduces `ObjectChange.apply()` partial-update behavior: an unrelated
main edit survives, while a same-field conflict is overwritten by the branch
only when the current merge would overwrite it. Deletes use the actual locked
row and missing deletes are successful no-ops. On this exact runtime the
current generic MAC delete path emits no destination ObjectChange or
AppliedChange; WP-A preserves that behavior instead of inventing audit rows.
Existing-PK creates stay on the current locked replay-lineage verifier in WP-A.

## Guarded DML and family admission

An admitted family owns explicit SQL, never serializer inference:

- `INSERT ... SELECT` carries the branch PK and every approved persisted field;
- `UPDATE ... FROM` includes the expected target identity and field mask;
- `DELETE ... USING` includes locked identity and relationship-safety guards;
- M2M DML, when separately proven, is scoped by exact through-row identities.

The runtime preflight requires PostgreSQL, NetBox 4.6.5, Branching 1.1.1, the
exact proved optional-distribution and plugin-app tuple, the exact model schema,
no model CustomFields, no configured model validators or protection rules, no
relevant EventRules, no model field migrators or denormalized-field
registration, the exact `MACAddressIndex` field/weight contract, and the exact
proved pre/post-save/delete signal receiver set. Any mismatch selects the
current merge before DML. Known changelog, search-cache, and competing-
ChangeDiff effects are reproduced in the range transaction. A subscribed MAC
update stays on the current path because NetBox's notification receiver has an
additional Notification side effect; a relationship-table write barrier makes
that rejection race-free. Row-level eligibility is narrower still.

Search-cache DML follows the observed current paths, not an idealized signal:
an UPDATE refreshes MAC/description cached values even when the destination
scalar is already equal, a real DELETE removes them, and the existing bulk
CREATE path emits none because `bulk_create()` bypasses `post_save`. Paired
evidence covers all three cases.

MAC WP-A admits default scalar state and an Interface assignment. It rejects to
the current path existing-PK creates, primary-MAC reassignment/deletion,
relation-bound deletes, nonempty tags on creates, changed tags or custom fields,
owner/scalar changes outside the Forward contract, non-Interface GFKs, invalid
payloads, missing dependencies, subscribed updates, and missing/ambiguous
source evidence. An update may preserve existing tags and generic side-table
rows; the paired proof covers that no-write relationship case.

## Audit, conflict, and completion transaction

Every successful SQL range transaction contains:

1. guarded destination target and approved relationship DML;
2. the full NetBox destination ObjectChanges emitted by the current MAC
   contract, with invoking user, username, ingestion request ID, action,
   representation, and full pre/post payloads (creates and material updates;
   not destination no-ops or MAC deletes);
3. one AppliedChange per emitted destination ObjectChange, pointing to the
   provisioned source branch;
4. set-based maintenance of other ready branches' ChangeDiff `current` state
   for destination create/update ObjectChanges, matching the existing
   global-change receiver;
5. the evidence needed for the caller to account the logical range.

The source branch ChangeDiff original/modified/current/conflicts is locked and
verified but not rewritten. Actual destination no-ops and MAC deletes emit no
artificial destination audit row, matching the current path, but are logically
accounted after the transaction commits. Other ready branches still receive
the same `current` maintenance emitted by the current path for create/update.
On the pinned runtime the generic MAC delete emits no destination ObjectChange,
so it also emits no competing-branch ChangeDiff refresh; WP-A preserves that
observable behavior. The source branch's already-computed three-way evidence
remains unchanged.

Fault hooks exist after target DML, after ObjectChange/AppliedChange insertion,
and during ChangeDiff maintenance. An unexpected fault rolls back the complete
range. Target rows, Bookmark/JournalEntry/Subscription/TaggedItem side tables,
search cache, ObjectChanges, AppliedChanges, and ChangeDiffs are fingerprinted
before and after; current-path fallback is allowed only after an unchanged
fingerprint. `JobTimeoutException` is re-raised unchanged.

The existing merge orchestrator remains the only completion authority. It
attests the branch, records zero failures, runs Branching cleanup, advances the
baseline, and permits branch removal only after every collapsed logical change
is accounted. A SQL range cannot mark the branch merged or baseline-ready.

## Measured opportunity and expected phase savings

The completed 5,000-change profile measured 80.17% of merge wall time and 7.822
statements/change in Forward-owned code. This design targets the following
measured phases; it does not reclassify or re-measure them.

| Measured phase | Current median | Set-based effect for admitted rows |
| --- | ---: | --- |
| Forward bulk application/relation preparation | 5.7085 ms/change; 7.0144 statements/change | Replaced by range COPY, locked set resolution, and constant-count target DML. Removes per-row deserialization, relation preparation, validation/save, and target round trips. |
| Forward audit and AppliedChange lineage | 1.0280 ms/change; 0.5510 statements/change | Full evidence remains, but ObjectChange and AppliedChange are inserted by range CTE instead of Python object construction and per-chunk ORM orchestration. |
| Forward existing-create replay verification | 0.9385 ms/change; 0.2520 statements/change | Missing-PK creates are proven in one locked anti-join. Existing-PK creates remain on the current lineage verifier in WP-A. |
| Forward set-based ChangeDiff resolution | 0.0123 ms/change; 0.0018 statements/change | Preserved as set DML; little additional saving is expected because it is already set-based. |
| Upstream ObjectChange.apply fallback | 1.2051 ms/change; 1.9320 statements/change | Eliminated only for admitted MAC updates/deletes; all rejected rows and unapproved families retain it. |
| Upstream squash/collapse | 0.3479 ms/change | Preserved. |
| Upstream cleanup | 0.2300 ms/change | Preserved. |
| Dependency graph plus Forward ordering | 0.0136 + 0.0117 ms/change | Preserved. |

A 10x improvement across the Forward-owned 80% was projected by the profile to
about 3.6x overall and an estimated 52 minutes at 1,182,931 changes. That is a
portfolio target, not a MAC WP-A promise. Extending proof through
InventoryItem/relation-heavy families was estimated at 8-15x overall and 12-25
minutes. A five-minute merge remains unproven.

## Impact rank and proof order

The raw migration impact rank is measured 5,000-row milliseconds per row times
fixture share. Counts below scale the same share to 1,182,931 only to make the
relative order concrete; they are estimates, not a measured production mix.

| Impact rank | Family | ms/row | Fixture share | Impact units (ms x share) | Estimated rows at 1,182,931 | Risk note |
| ---: | --- | ---: | ---: | ---: | ---: | --- |
| 1 | Interface | 8.251 | 54.0% | 445.6 | 638,783 | Highest aggregate opportunity; signal, relationship, parent, and primary-MAC semantics make it a poor first proof. |
| 2 | InventoryItem | 25.741 | 8.4% | 216.2 | 99,366 | Highest per-row cost in both staging and merge; MPTT/component semantics require a dedicated proof. |
| 3 | MACAddress | 5.728 | 28.0% | 160.4 | 331,221 | High volume, no tree fields, staging COPY/SQL parity oracle exists; selected for WP-A proof value. |
| 4 | IPAddress | 8.172 | 5.3% | 43.3 | 62,695 | GFK assignment, primary/NAT relationships, and address identity need separate proof. |
| 5 | Prefix | 5.215 | 3.6% | 18.8 | 42,585 | Tree rebuild semantics; retain the existing specialized path. |
| 6 | Device | 6.832 | 0.6% | 4.1 | 7,098 | Dense signals, ownership-controlled deletes, component replication, and many relationships. |
| 7 | Site | 9.386 | 0.1% | 0.9 | 1,183 | MPTT and cached-scope signals for little measured aggregate return. |

Proof order starts with MAC to validate the common range/audit/conflict engine,
then returns to the raw impact order: Interface, InventoryItem, IPAddress, and
only then lower-impact families whose semantics can be proved. InventoryItem's
cost makes it a priority, not permission to bypass MPTT.

## Explicit non-goals

WP-A will not attempt:

- InventoryItem, Prefix, Site, ModuleBay, or any other MPTT/tree family;
- Interface, Device, Module, Cable, IPAddress, primary-IP/MAC, termination, or
  ownership-sensitive relations;
- arbitrary GFKs, relation-through models, plugin models, or optional-plugin
  families;
- signal-generating, EventRule, CustomField, custom-validator, protection-rule,
  or field-migrator configurations;
- set-based MAC tag writes or relation-bound deletes;
- exact restart manifests, arbitrary resume cursors, conflict-policy changes,
  or replacement of Branching collapse/cleanup.

These remain on the current path until their own paired-branch proof exists.

## Proof limits

Paired synthetic branches can prove the enumerated MAC contract and concurrent
main cases on the exact allowlisted runtime. They do not prove a conflict-heavy
customer distribution, optional-plugin model mix, deadlock behavior under many
simultaneous merges, million-row memory/WAL ceilings, or end-to-end runtime at
1,182,931 changes. Benchmark results must be reported at their measured volumes
without extrapolating a five-minute or million-row outcome.
