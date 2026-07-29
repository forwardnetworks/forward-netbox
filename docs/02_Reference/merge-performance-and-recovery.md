# Merge Performance, Restartability, and Recovery

This reference records the measured Forward branch-merge path, the boundary
between Forward NetBox and `netbox_branching`, and the recovery guarantees. It
does not replace the operator procedure in the user guide.

## Production observation that motivated the profile

An observed first-sync branch contained 1,182,931 logical changes. Staging took
11,259.496 seconds. The merge reached 70,000 changes in 522 seconds, about 134
changes/second, before the worker process disappeared. There was no OOM,
database deadlock/timeout, container restart, or durable wrapper evidence for
the termination. The database contained 75,001 durable `AppliedChange` rows,
showing that another sub-batch committed after the 70,000 progress observation.

At that observed rate, 1,182,931 changes would take approximately 2.45 hours to
merge. This is an **estimate**, not a completed end-to-end measurement. The
previous 14,337.411-second baseline stopped at `ready_to_merge` with automatic
merge disabled and therefore did not measure merge.

## Profile method and limits

`forward_profile_merge` creates anonymous real NetBox objects in a provisioned
branch and invokes the production merge path. It writes one fsync'd JSON record
after each round. The default design runs three rounds at 1,000 and 5,000
logical changes. The fixture preserves the dominant core dependency shape:

```text
device ──> interface ──> MAC address
   │            └──────> IP address
   └────> inventory item

prefix and tree-backed site rows are also present
```

The normalized fixture models represent approximately 84% of the observed
first-sync rows. Optional third-party models and cables are not synthesized,
because a fake representation would give misleading cost measurements. Names,
identities, source settings, addresses, and MAC addresses are generated and do
not contain production identifiers.

The recorder measures:

- exclusive wall time by phase and model;
- Python process CPU, sampled Python peak RSS, and database execute wait;
- SQL execute/executemany calls, used as database round trips;
- SQL verb counts, WAL bytes, temporary bytes, and PostgreSQL block I/O time;
- host cgroup CPU and memory samples for the Python and PostgreSQL containers.

PostgreSQL index and constraint maintenance is included in statement execution
and WAL cost. Stock PostgreSQL does not expose a reliable per-index maintenance
CPU timer, so that cost cannot be split further without an invasive extension
or changing indexes/constraints and thereby changing merge semantics.

## Measured cost decomposition

The completed profile JSONL and summarized median/variance table are delivered
with the run artifacts. This section is updated only from completed rounds; a
partial round is never promoted as a measurement.

<!-- MERGE_PROFILE_RESULTS_START -->

All six detached rounds completed with exit status 0. Variance below is sample
variance across three completed rounds at each volume.

| Logical changes | n | Median wall | Wall variance | Median rate | Rate variance | Median round trips/change | DB-wait wall fraction | Peak Python RSS |
| ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| 1,000 | 3 | 9.010214 s | 0.113366 s² | 110.985/s | 18.155730 (changes/s)² | 9.8290 | 0.3297 | 384.27 MiB |
| 5,000 | 3 | 46.994236 s | 5.956124 s² | 106.396/s | 33.048279 (changes/s)² | 9.7578 | 0.3706 | 555.86 MiB |

The wall-time ratio was 5.216 for a 5x volume increase, a power exponent of
1.026. The measured local curve is therefore approximately linear at these
volumes. Extrapolating 106.396 changes/second to 1,182,931 changes gives about
3.09 hours. That is an **estimate**, not a completed million-row measurement;
it is close enough to the observed 134 changes/second to reject the idea that
the production result was only an unrelated anomaly.

### Ours versus upstream: primary decision result

| Volume | Owner | Median exclusive wall fraction | Fraction variance | Median statements/change |
| ---: | --- | ---: | ---: | ---: |
| 1,000 | Forward NetBox | 80.53% | 0.00009215 | 7.8830 |
| 1,000 | `netbox_branching`/NetBox core | 18.92% | 0.00007259 | 1.9350 |
| 1,000 | Unattributed | 0.55% | 0.00000119 | 0.0110 |
| 5,000 | Forward NetBox | 80.17% | 0.00019530 | 7.8220 |
| 5,000 | `netbox_branching`/NetBox core | 19.14% | 0.00017899 | 1.9328 |
| 5,000 | Unattributed | 0.62% | 0.00000068 | 0.0030 |

The bottleneck is therefore predominantly **our merge path**, not machinery
that can only be changed upstream. The word "bulk" in the current fast path is
important but insufficient: it still deserializes, resolves relations,
validates, and prepares model instances row by row before the final batched
write. At 5,000 changes that phase alone consumed a median 5.708471
milliseconds/change and 7.0144 statements/change.

### Five-thousand-change phase decomposition

These are medians of per-round aggregate phase costs; independently computed
phase medians need not sum exactly to the median whole round.

| Owner and phase | Median ms/change | Variance (s/change)² | Statements/change | DB ms/change |
| --- | ---: | ---: | ---: | ---: |
| Forward: bulk application | 5.708471 | 0.0000000012 | 7.0144 | 2.050522 |
| Forward: audit and `AppliedChange` lineage | 1.027973 | 0.0000000003 | 0.5510 | 0.208311 |
| Forward: replay-existing-create verification | 0.938508 | 0.0000001733 | 0.2520 | 0.776088 |
| Upstream: `ObjectChange.apply` fallback | 1.205051 | 0.0000000005 | 1.9320 | 0.442449 |
| Upstream: squash/collapse | 0.347925 | 0.0000000002 | 0 | 0 |
| Upstream: merge cleanup | 0.229988 | 0.0000000121 | 0.0008 | 0.013370 |
| Upstream: FK dependency graph | 0.013555 | approximately 0 | 0 | 0 |
| Forward: cycle preparation plus Kahn ordering | 0.011717 | approximately 0 | 0 | 0 |
| Forward: set-based `ChangeDiff` current/conflict update | 0.012254 | approximately 0 | 0.0018 | 0.004186 |
| Unattributed orchestration | 0.054280 | approximately 0 | 0.0030 | 0.004094 |

Conflict fields are calculated by upstream `ChangeDiff` maintenance when the
branch or main object changes, before this timed merge. The production merge
does not perform a separate per-row conflict-detection pass. The generated
fixture had no concurrent main edits, so conflicted-merge validation cost is
**not measured**. The timed `ChangeDiff` phase above is Forward's set-based
equivalent of the upstream global-change receiver; it updates other ready
branches after main writes and had little work with no competing ready branch.

### Per-model merge cost curve

These model-specific costs exclude global collapse, ordering, cleanup, and
attestation phases.

| Model (fixture share) | 1,000-change ms/row | 5,000-change ms/row | 5,000 variance (s/row)² | 5,000 statements/row |
| --- | ---: | ---: | ---: | ---: |
| `dcim.device` (0.6%) | 9.942946 | 6.831775 | 0.0000000314 | 8.3333 |
| `dcim.interface` (54.0%) | 7.995973 | 8.251257 | 0.0000000083 | 9.0130 |
| `dcim.macaddress` (28.0%) | 5.719496 | 5.727691 | 0.0000000114 | 7.0136 |
| `dcim.inventoryitem` (8.4%) | 18.717404 | 25.741078 | 0.0000273916 | 26.0000 |
| `ipam.ipaddress` (5.3%) | 8.281917 | 8.172061 | 0.0000000914 | 10.0377 |
| `ipam.prefix` (3.6%) | 5.555146 | 5.214521 | 0.0000000438 | 4.1111 |
| `dcim.site` (0.1%) | 16.349398 | 9.385963 | 0.0000027947 | 6.4000 |

Interfaces dominate aggregate time because they are 54% of the fixture and
still require about nine statements each in Forward's nominal bulk path.
Inventory items are the highest per-row cost and account for the repeated
upstream `Applying change ... using default` log lines: this MPTT-backed model
uses `ObjectChange.apply` and its replay check, reaching 26 statements/row.

### Python, PostgreSQL, indexes, and constraints

| Volume | Median Python cgroup CPU | Median PostgreSQL CPU | Python/PG CPU split | Median WAL/change |
| ---: | ---: | ---: | ---: | ---: |
| 1,000 | 6.531844 s | 2.342267 s | 72.49% / 27.51% | 3,227.66 bytes |
| 5,000 | 32.661562 s | 14.105350 s | 69.76% / 30.24% | 3,486.14 bytes |

The merge is single-process Python dominated, but PostgreSQL execution wait is
still 37.06% of 5,000-row wall time. Index, foreign-key, uniqueness, and WAL
maintenance are included in that database time and in the measured WAL. They
cannot be split into reliable per-index CPU numbers with stock PostgreSQL, so
no finer index/constraint percentage is claimed.

<!-- MERGE_PROFILE_RESULTS_END -->

## Ownership boundary

The merge is neither wholly Forward code nor wholly upstream code.

| Phase | Owner | Constraint/actionability |
| --- | --- | --- |
| Queueing, job timeout, retry state, progress UI/logging, checkpoints, branch attestation | Forward NetBox | Directly changeable here |
| Streaming the branch changes and batching by 5,000 | Forward NetBox | Directly changeable here |
| Batched create, Prefix update/delete, deferred FK update, audit and `AppliedChange` bulk writes | Forward NetBox | Directly changeable, but audit/branch invariants must remain exact |
| Set-based updates to other ready branches' `ChangeDiff.current` and conflicts after main writes | Forward NetBox | Our implementation of an upstream-owned semantic contract |
| Batched missing-update checks and O((V+E) log V) Kahn ordering | Forward NetBox | Directly changeable |
| Squash/collapse of `ObjectChange` rows | `netbox_branching` | Reused upstream machinery; replace or fork only with compatibility evidence |
| FK dependency graph construction and merge cleanup | `netbox_branching` | Reused upstream machinery and signals; version-constrained |
| Fallback `ObjectChange.apply`, deserialization, validation/save, M2M handling | `netbox_branching` and NetBox core | Constrained; Forward chooses which rows enter this path |
| `ChangeDiff` meaning, conflict calculation, branch states, and audit lineage model | `netbox_branching` | Semantic contract; cannot be weakened |
| SQL execution, indexes, foreign keys, uniqueness checks, WAL | PostgreSQL/NetBox schema | Tunable only within the same correctness guarantees |

The plugin fast path handles non-tree creates in bulk. Tree creates and generic
updates/deletes still enter the upstream per-change path, except for Forward's
specialized Prefix and deferred-FK batches. A log line beginning `Applying
change ... using default` proves that a row entered upstream
`ObjectChange.apply`; it does not prove that every row did.

## Chunked exact-restart feasibility

### Required durable structures

A semantics-preserving exact restart needs more than a counter:

1. `MergeManifest`: branch, immutable source-change watermark/hash, merge
   strategy and ordering-algorithm versions, main/conflict baseline, manifest
   state, and total logical changes.
2. `MergeManifestItem`: durable ordinal, content type and object identity,
   action, normalized payload hash, dependency-component/SCC identity, and any
   synthetic component identity created by cycle/deferred-FK splitting.
3. `MergeComponentCheckpoint`: component/range identity, status, attempt,
   transaction identity, completed time, and audit-lineage evidence. The
   checkpoint must commit in the same transaction as the destination mutation
   and its `ObjectChange`/`AppliedChange` rows.

A bare `70,000` cursor is unsafe: a later ordering pass can produce a different
70,000-row prefix, one logical change can expand into multiple internal
components, and the counter does not attest conflict or audit state.

### Estimated storage at 1.18 million changes

These are **estimates** pending a real schema prototype:

- compact item heap: about 95-145 MB (roughly 80-120 bytes/item);
- item/manifest indexes: about 70-120 MB;
- component/checkpoint headers: about 5-20 MB when components are ranges;
- explicit dependency edges, if stored rather than reproducibly derived:
  another approximately 70-140 MB for roughly one edge/change.

The likely total is approximately 170-285 MB without explicit edges, or
approximately 240-425 MB with them. JSON payload copies would be substantially
larger and should not be part of the manifest; store hashes and reference the
immutable branch audit rows.

### Safe boundaries

The smallest safe checkpoint is one logical dependency component after all of
its split parts, destination writes, M2M writes, main audit rows, `AppliedChange`
lineage, and affected `ChangeDiff` updates commit. A topologically ordered
prefix is dependency-satisfied, but it is resumable only if the manifest is
immutable and every completed item is revalidated against concurrent main
edits.

Weakly connected components are naturally independent but can be very large:
shared catalog rows can connect many devices, and a device with its interfaces,
MACs, IPs, inventory, modules, and cables forms a substantial component.
Practical boundaries are therefore likely dependency-closed topological ranges
with an explicit frontier, not arbitrary 5,000-row slices. All synthetic parts
of one logical change must remain atomic.

### Conflicts and concurrent main edits

Conflict detection and `ChangeDiff` resolution must be frozen or versioned when
the manifest is built. On restart:

- an edit to an uncompleted object is rechecked before apply;
- an edit to a completed object after its checkpoint cannot be silently
  overwritten or ignored; it must invalidate the manifest, fail with a visible
  conflict, or cause a full replay/replan;
- branch completion is forbidden until every manifest component has a committed
  checkpoint and its audit lineage is present;
- cleanup is forbidden while any component is incomplete.

This is the highest regression-risk area. A restart system that merely skips
the first N changes would weaken concurrent-edit, dependency, conflict, audit,
and branch-completion semantics.

### Recovery-attempt interaction

The current four-attempt bound counts full logical replays. An exact-restart
implementation should retain that operator safety bound while distinguishing a
new process attempt from a new manifest generation. Recovering the same valid
manifest increments the process-attempt counter; invalidating and rebuilding a
manifest is a new generation and must remain operator-visible. Exhaustion still
fails closed for manual inspection.

### Build cost and risk

An exact-restart prototype is **estimated** at 6-10 engineer-weeks plus 3-5
weeks for concurrency, kill-injection, upgrade, and production-scale validation.
The highest regression risk is a main edit between component commits being
misclassified as already merged, followed by premature branch completion or
incorrect audit lineage.

A merge-fast project is **estimated** at 3-5 engineer-weeks after the measured
bottleneck is identified, plus 2-3 engineer-weeks of scale, concurrency, and
equivalence validation. It is lower risk when it extends the existing
bulk/audit path, but it cannot bulk-apply a model whose NetBox
save/tree/relationship semantics are not reproduced exactly.

## Recommendation: build fast first, keep restartability as the fallback

The evidence supports the product owner's preference: optimize the Forward-owned
merge path before building exact restart. About 80% of measured wall time and
7.822 of 9.758 round trips/change are directly actionable in this repository.
An upstream-only diagnosis is not supported.

A semantics-preserving set-based merge apply should:

1. materialize dependency-ordered, model-homogeneous ranges into PostgreSQL
   staging tables with the logical branch key, desired scalar state, resolved
   FK/GFK identities, and payload hash;
2. recheck concurrent-main and existing-create lineage in set operations under
   the same row/relationship locks used today, failing closed on missing,
   diverged, or unaudited state;
3. apply `INSERT ... SELECT`, `UPDATE ... FROM`, guarded deletes, and M2M writes
   only for parity-approved model families, retaining `ObjectChange.apply` for
   unsupported save/tree/relationship semantics;
4. write destination `ObjectChange`, `AppliedChange`, and affected
   `ChangeDiff` evidence in the same transaction as each dependency-closed
   range; and
5. keep upstream collapse, dependency graph construction, conflict fields, and
   branch cleanup, and forbid branch attestation/baseline advancement until all
   logical changes are accounted with zero failures.

The demonstrated staging COPY/SQL result of about 10,000 rows/second is an
upper bound, not a merge forecast: merge must also preserve conflict, ordering,
audit, lineage, and completion guarantees. Amdahl's law on this profile says a
10x improvement limited to the current Forward-owned 80% yields only about
3.6x overall, approximately 380 changes/second or **an estimated 52 minutes**
for 1.18 million rows. Moving inventory-item fallback and relation resolution
onto parity-proven set-based paths as well makes **an estimated 8-15x overall
(roughly 12-25 minutes)** plausible. A five-minute merge is **not proven** by
this profile and would require reducing the remaining upstream collapse/cleanup
floor too.

Build cost for the fast tranche is **estimated** at 3-5 engineer-weeks for the
set-based model families and exact audit/lineage writes, plus 2-3 engineer-weeks
for paired-branch equivalence, concurrent-main edits, kill injection, optional
model coverage, and million-row validation. The highest regression risk is
declaring a model bulk-safe without reproducing its FK/GFK, MPTT, M2M, signal,
conflict, or audit behavior. Gate each family independently and retain fallback.

Exact restart remains the pragmatic second layer only if customer-equivalent
acceptance still approaches 134 changes/second, optional models dominate the
remaining cost, or the residual merge window is operationally unacceptable.
Its estimated 9-15 engineer-weeks, 170-425 MB manifest storage, dependency-frontier
complexity, and concurrent-main correctness risk make it the more expensive and
riskier first move. The observability shipped here makes that later decision
measurable while today's bounded full replay remains honest and supported.

## What remains unproven

- A synthetic fixture does not prove the complete 1,182,931-row production
  model mix, especially optional plugins and cables.
- Two test volumes establish a local scaling curve, not a million-row memory or
  WAL ceiling.
- The profile contains first-baseline creates and no concurrent main edits; it
  does not measure a conflict-heavy update/delete merge or conflict
  acknowledgement policy.
- Catchable-signal and dead-worker tests do not prove evidence capture for
  `SIGKILL`, host power loss, kernel failure, or storage loss; those can only be
  reported later from RQ evidence or as unknown termination.
- The exact-restart schema, storage estimate, dependency-frontier algorithm,
  and concurrent-main-edit rules have not been implemented or kill-tested.
- Stock PostgreSQL measurements do not isolate index-maintenance CPU from the
  statement that performed it.
- The 8-15x fast-path range and 12-25 minute full-scale runtime are estimates;
  no set-based merge implementation or million-row merge has proven them.
