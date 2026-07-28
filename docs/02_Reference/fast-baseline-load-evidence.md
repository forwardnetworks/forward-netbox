# Fast Baseline Load: Design and Measured Evidence

## Blunt result

For the measured anonymized customer-shaped database workload, the fastest
defensible first-baseline load is **about 5 minutes 41 seconds** for the target
transaction, or **about 5 minutes 54 seconds** for the detached harness including
fixture construction and post-load streaming fingerprints. This excludes the
real Forward NQE fetch, which is common to both apply paths and depends on the
Forward deployment.

That number is conditional: every eligibility and row-contract check below
must pass. The safety price is no branch review, no per-row audit lineage, and
no branch rollback for this one baseline. Recovery after a successful load is
database restore/reseed. Ongoing and incremental syncs still use
`netbox_branching`.

## Eligibility: fail closed before DML

The feature is disabled by default. Selection requires all of the following:

- explicit `enable_fast_baseline_load`, bulk ORM, and auto-merge;
- only full workloads, no primary-IP post-fetch overlay, and no delete rows
  except the versioned normalization-stamped CVE tombstones proven to be
  physical no-ops on the empty target;
- exact NetBox `4.6.5`, `netbox-branching` `1.1.1`, Forward NetBox `2.6.1`,
  optional-plugin versions, and configured plugin applications;
- only versioned allowlisted models;
- a complete pre-DML row-contract proof for every specialized model;
- no prior Forward ingestion, current workload/contributor baseline, or device
  identity;
- no nonterminal competing Branching branch;
- empty selected target tables and empty owned InventoryItemRole,
  CableTermination/CablePath, and DLM side tables;
- no operator custom field on an admitted model (the one migration-owned Device
  field is pinned), enabled event rule, custom validator, or protection rule.

Mutable facts are checked again while a transaction advisory lock and target,
side, Branch, and Forward-state table locks are held. A pre-DML rejection uses
the ordinary branch path. A dependency-resolution failure after loading starts
raises and rolls back the whole transaction; it never changes engine after a
partial commit.

The specialized row contracts use the ordinary parity-tested batched Interface
engine for scalar fields, LAG membership, access/tagged mode, and untagged VLAN
resolution; admit flat component-free InventoryItems only when their weakest
normal-adapter identity `(device, name)` is unique, their asset tags and
role/manufacturer name-slug definitions are consistent, and any implicit
manufacturer can be created by the normal ensure adapter inside the transaction;
unique MAC identities; simple one-cable-per-non-LAG-interface links; valid,
syntactically valid IP rows whose ordinary batched engine preserves
network/broadcast skips and ordered host+VRF coalesce; valid unique Prefix
identities whose ordinary batched engine preserves implicit VRF creation and
NetBox hierarchy rebuild; and unique DLM finding triples with one software tuple per device.
Unsupported parent/component/cable/DLM ambiguity rejects the complete fast
baseline before target DML.

For authoritative full workloads, InventoryItems whose device is absent from
the full device workload are normalized out with reason
`device_not_in_workload`, just like unrepresentable cable and OSPF-interface
relationships. This normalization is shared by the ordinary and fast engines;
partial/diff device workloads do not claim authoritative parent coverage.

The relationship allowlist additionally pins `dcim.module`,
`extras.taggeditem`, `ipam.fhrpgroup`, `ipam.vlan`, and the BGP/OSPF models in
`netbox_routing`. These execute through the normal apply adapters inside the
same transaction, preserving their coalesce, generic-relation, shared-VIP, and
side-object semantics. Their preflight contract proves required keys, device
and interface coverage, routable identities/choices, and owned side-table
emptiness. When module sync is enabled, only module-native inventory rows move
to module ownership; all other InventoryItems retain the set-based loader.

Standalone preflight necessarily pays for one complete workload fetch. The
optional `require_fast_baseline_eligibility` single-pass mode eliminates a
second fetch: the normal sync proves its already-fetched workload, loads when
eligible, and aborts before branch/target mutation when ineligible. Normal sync
failure bookkeeping still records one failed ingestion for an ineligible run;
the standalone management-command preflight remains the zero-ingestion path.

## Evidence contract

The fast baseline produces:

- the same target model state as the current path;
- current-path-owned side state: InventoryItemRole, CableTermination, DLM
  SoftwareVersion/DeviceSoftware/CVE/affected-software, and search values;
- ingestion model results, issues, source statistics, and logical create totals;
- staged workload-state and contributor-baseline promotion when fetch produced
  them;
- device identities and the ordinary ownership/catch-up finalization inputs;
- `baseline_ready`, merge timestamps, completed sync state, and a durable
  `fast_baseline_load` attestation containing runtime/model spec versions,
  selected per-model engines, omitted evidence, and aggregate counts.

It intentionally does not produce a Branch, BranchEvent, source or destination
per-row ObjectChange, ChangeDiff, AppliedChange, or branch rollback point.
Those rows exist to review, conflict-check, replay, and roll back incremental
change. The later Forward diff/prune path consumes promoted workload/contributor
state and device identities, not baseline ObjectChange lineage. Omitting them
is safe for those downstream consumers but removes operator audit history and
branch rollback; that is the explicit tradeoff.

CablePath and InventoryItemRole search rows deserve special mention. The pinned
current branch merge retains CableTermination and InventoryItem search state but
does not retain simple CablePath, endpoint search, or InventoryItemRole search
signal side effects. Fast reproduces that current final state exactly rather
than the different state produced by saving the same objects directly one by
one. Normal later cable syncs therefore resolve an existing link from the
authoritative CableTermination relation when the optional Interface cable cache
is absent. This prevents a populated baseline from being mistaken for 23,083
new conflicting cables without changing the paired baseline state.

## Customer full-configuration measurement

The clean disposable customer-shaped run used all 28 enabled model strings and
32 active maps. Standalone preflight was eligible after a 179.500-second fetch
and 3.433-second local proof (182.934 seconds total), down from the measured
1,337 seconds. It left zero target rows, ingestions, branches, and issues.

The direct baseline completed 1,168,250 logical creates in a 3,289.205-second
target transaction (3,468.987 seconds including fetch/finalization), with
3,369,719 SQL statements, 2.884416 statements/change, 355.177 changes/second,
and 2,678,788,096-byte peak RSS. It created no branch and performed no merge;
all 28 versioned model specs, 28 workload states, two contributor relations,
and the no-branch evidence exclusions are present in the durable attestation.
There were no issues or failures.

The Interface shard rate remained flat (31.373 versus 31.335-second half
medians), but the IPAddress shards did not (83.168 versus 129.262 seconds, with
66.798 first and 143.840 last). The full customer workload therefore does not
support an unqualified flat-rate claim even though the largest Interface
family is flat.

The baseline normalized 4,632 CVE rows to the versioned
`cve_without_in_scope_vulnerability_v1` tombstone contract. Those tombstones
were preserved in durable workload state and omitted as proven empty-target
physical delete no-ops. It also moved 3,745 module-native inventory rows to
`dcim.module`, excluded 56 InventoryItems whose devices were absent from the
authoritative Device workload, and retained 78,771 physical InventoryItems.

The populated next-snapshot proof did **not** complete and is therefore a
release blocker. Ordinary staging produced 19,180 change records in 614.301
seconds. MERGE applied 19,179 in 316.692 seconds (60.560 applied changes/second)
and then failed one protected `netbox_dlm.softwareversion` delete. The branch
remained ready, the sync was failed, and the partial database retained zero FK
or generic-relation orphan groups. Four protective sync issue groups were also
present (three IP-address dependency skips and one VRF dependency skip), plus
the merge ProtectedError and partial-merge failure. This evidence is a
**NO-GO** for the complete customer workflow even though the clean fast
baseline itself is unblocked.

## Paired equivalence

Both engines started from independent clones of one pristine database and
consumed the same 0.5% fixture (5,868 customer-denominator logical changes).
Canonical streaming SHA-256 fingerprints excluded only volatile timestamps.

| Check | Result |
| --- | --- |
| Target counts and fingerprints | Equal |
| Cable termination/path state | Equal |
| Inventory role and search state | Equal |
| DLM versions, device software, CVEs, findings, affected-software | Equal |
| Ingestion logical totals | Equal: 6,015 creates, zero failures |
| Issues | Equal: zero |
| Expected audit difference | Current retained 6,015 destination ObjectChanges; fast retained none |

| Engine | Complete wall | Statements/change | WAL | Peak RSS |
| --- | ---: | ---: | ---: | ---: |
| Current branch stage + merge | 119.187 s | 18.3035 | 144.60 MB | 526.20 MiB |
| Fast baseline | 4.068 s | 0.09151 | 6.83 MB | 366.73 MiB |

The paired end-to-end speedup was **29.30x**. The current cell split into
1.935 s branch provision, 56.338 s staging, and 60.905 s merge. The current
statement rate includes both stage and merge; the production merge-only figure
remains 12.559 statements/change.

## Actual-scale measurement

The full fixture used the requested target counts exactly:

| Model | Rows |
| --- | ---: |
| `dcim.interface` | 535,777 |
| `dcim.macaddress` | 277,915 |
| `dcim.inventoryitem` | 82,572 |
| `netbox_dlm.vulnerability` | 70,230 |
| `ipam.ipaddress` | 51,944 |
| `ipam.prefix` | 34,388 |
| `dcim.cable` | 23,083 |
| `dcim.device` | 3,400 |

There were 1,079,468 source/target rows plus reference and owned side rows. The
fixture deliberately created 70,230 distinct anonymized CVEs so the pinned
current DLM path had no duplicate synthetic finding; as a result it produced
1,199,279 logical creates, slightly more than the customer's 1,173,589-change
denominator. Performance calculations retain the customer denominator and are
therefore conservative for the requested comparison.

| Metric | Full fast baseline |
| --- | ---: |
| Target-transaction wall | 341.136 s (5m41.1s) |
| Detached harness wall | 354 s (5m54s) |
| Customer-denominator throughput | 3,440.23 changes/s |
| SQL statements | 53,574 |
| Statements/change | 0.045650 |
| Peak RSS | 1,153.11 MiB |
| Incremental peak above generated fixture | 566.79 MiB |
| WAL | 1,462,253,440 bytes |
| Verification | Passed; exact requested target counts, zero issues |

The 100 periodic samples do not decelerate. Median first-half versus second-half
rates were 3,212 versus 3,271 rows/s for Interface, 4,391 versus 4,322 for
InventoryItem, 7,684 versus 7,623 for MAC, and 5,574 versus 5,303 for
Vulnerability. These changes remain approximately flat within family (the
largest shift was −4.9% across only seven Vulnerability samples), with no
systematic volume-driven collapse like the production per-change merge.

A complete current-path run was not attempted. Linear extrapolation of the
paired 49.23 changes/s cell is **6.62 hours**, explicitly an extrapolation. It
is not trusted as the operational forecast because the live run decelerated
from roughly 58/s to 21/s and projects approximately 3 hours staging plus 12.6
hours merge. The measured live forecast, about **15.6 hours**, is the honest
current-path comparison.

## Deceleration diagnosis and remedies

The current paired lifecycle issued 107,405 SQL calls; 91,854 (85.5%) were
SELECTs. Earlier merge profiling attributed about 80% of merge wall and 7.0144
statements/change to Forward relation preparation/bulk application, with Python
receiving roughly 70% of CPU. Constant per-change statement count combined with
a 5x live rate decline means the statements and Python work become more
expensive as target, ObjectChange, ChangeDiff, and AppliedChange indexes grow
and leave cache; it is not a new statement-count explosion. InventoryItem MPTT
is locally expensive but too small a share to explain the whole late curve.

One-factor 5% experiments were compared with a warmed 17.076 s control. Each
number includes the setting change and restoration/rebuild:

| Remedy | Wall vs control | WAL vs control | Decision |
| --- | ---: | ---: | --- |
| `synchronous_commit=off` | +2.10% | +0.05% | No gain; weakens crash durability |
| Defer all deferrable constraints | +14.10% | +0.06% | Rejected |
| Disable/restore autovacuum | +1.99% | +0.49% | No gain; risks post-load bloat/analyze debt |
| Drop/rebuild nonunique secondary indexes | −0.94% | −54.41% | Wall gain is noise-sized; DDL/availability cost is not justified |

Changing `maintenance_work_mem` was not separately useful because the selected
production design does not rebuild indexes. Batching is the remedy that matters:
10,000-row plan items and 5,000-row inserts made round trips proportional to
batch count and kept full-volume rates flat. No unsafe setting is enabled by
the implemented path.

## Artifacts and limitations

Machine-readable results are under
`docs/03_Plans/completed/evidence/fast-baseline-load/`; detached logs/status files
are under `/tmp/fnb-fastbaseline-20260726/`. The benchmark used real pinned
NetBox/plugin models and PostgreSQL but anonymized generated values. It did not
measure Forward NQE network time, simultaneous writers, crash recovery during
the final commit, or row shapes rejected by admission. Operators must treat a
preflight rejection as the correct safe result, not force the direct path.
