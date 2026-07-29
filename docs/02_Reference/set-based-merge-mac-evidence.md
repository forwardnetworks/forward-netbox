# Set-Based MAC Merge Evidence

## Scope and status

WP-A proves only the Forward-owned `dcim.macaddress` Interface-assignment
contract on NetBox 4.6.5, NetBox Branching 1.1.1, Python 3.14.4, and the exact
plugin tuple in the model specification. The optimizer remains disabled by
default, model-allowlisted, independently killable, and fail-closed.

The provisioned Branching branch remains authoritative. Collapse, dependency
ordering, source ChangeDiff evidence, current-path fallback, completion,
cleanup, and baseline advancement are not replaced.

## Paired-branch equivalence proof

`forward_netbox.tests.test_set_based_merge` provisions disjoint but logically
identical current-path and set-based branches, commits each merge, normalizes
only branch-specific identities/timestamps, and compares:

- destination MAC rows and final merged branch status;
- Bookmark, JournalEntry, Subscription, TaggedItem, Notification, and search
  CachedValue contents;
- complete destination ObjectChange pre/post payloads and metadata;
- AppliedChange lineage;
- source and competing-ready-branch ChangeDiff
  original/modified/current/conflicts;
- ingestion created/updated/deleted/applied/failed statistics and issues.

The mixed proof has ten logical branch changes. The observed SQL range executes
one create, three material updates, one real delete, and two destination no-ops.
A relation-bound delete and a subscribed update are explicitly rejected to the
current path; an update whose destination was concurrently deleted is accounted
by the existing skip path. The proof includes:

| Contract | Evidence |
| --- | --- |
| Create, update, no-op, delete, missing delete | Target, audit/lineage, ChangeDiff, search cache, statistics, and completion match. |
| PK/GFK identity | Branch PK is authoritative; Interface references are locked. Existing-PK create, primary-MAC, missing GFK, and source-evidence mismatch guards reject without mutation. |
| Concurrent main | Unrelated description survives; same-field assignment conflict follows current overwrite behavior; concurrently deleted update stays skipped. |
| M2M/GFK side tables | An eligible tagged update preserves unchanged Bookmark, JournalEntry, and TaggedItem rows. Relation-bound delete and subscribed update use the current path; Notification parity is compared. |
| Competing ready branch | Full competing ChangeDiff state matches the current path after create/update/delete cases. |
| Runtime hooks | Unexpected MAC signal receivers fail runtime selection; search-index and denormalized contracts are exact-version checked. |
| Transaction failure | Injected faults after target DML, after audit/lineage, and during ChangeDiff maintenance reach all three hooks. Target, search cache, ObjectChange, AppliedChange, and ChangeDiff state remains byte-for-byte unchanged before clean current-path fallback. |

The final targeted run executed nine tests and passed in 34.585 seconds. The
repository harness check passed, its 206 harness tests passed, and Django's
system check reported no issues.

## Measurement method

The disposable Compose project was `fnb-setmerge-wpa-20260726`, separate from
other worktrees and runtimes. The final campaign used a disjoint synthetic MAC
namespace and a deterministic 55% create / 25% update / 10% destination-no-op /
10% delete mix. Branch provisioning, staging, and the main-side no-op setup were
excluded. The measured interval is the production `merge_branch()` call through
branch completion.

Each engine ran three accepted rounds at 1,000 and 5,000 logical changes.
Engine order was counterbalanced by round. Every set-based result records one
real SQL range, all logical changes applied, zero fallback, exact I/U/N/D
operation counts, and successful final verification. Statement counts combine
Django execute-wrapper calls with the otherwise-unobserved COPY operation. RSS
is sampled from `/proc/self/statm` during the measured interval.

## Results

Values are mean plus or minus sample standard deviation across three rounds;
CV is the wall-clock coefficient of variation.

| Changes | Engine | Wall seconds | CV | ms/change | statements/change | Peak RSS MiB |
| ---: | --- | ---: | ---: | ---: | ---: | ---: |
| 1,000 | Current | 8.6141 +/- 0.0556 | 0.65% | 8.6141 +/- 0.0556 | 12.5830 +/- 0 | 357.99 +/- 0.11 |
| 1,000 | Set-based | 0.7414 +/- 0.0162 | 2.19% | 0.7414 +/- 0.0162 | 0.0700 +/- 0 | 325.66 +/- 0.41 |
| 5,000 | Current | 43.0333 +/- 0.0740 | 0.17% | 8.6067 +/- 0.0148 | 12.5594 +/- 0 | 424.06 +/- 0.53 |
| 5,000 | Set-based | 3.0360 +/- 0.0626 | 2.06% | 0.6072 +/- 0.0125 | 0.0140 +/- 0 | 381.11 +/- 0.11 |

Realized wall-clock speedup is **11.62x at 1,000** and **14.17x at 5,000**.
Statement reduction is 179.76x and 897.10x respectively. Mean total peak RSS is
9.0% lower at 1,000 and 10.1% lower at 5,000.

This clears the profile's projected 10x target for this admitted MAC family at
both measured volumes. It does not prove a 10x gain across the full Forward
model portfolio or establish an end-to-end million-change merge duration.

Machine-readable evidence is in
`docs/03_Plans/completed/evidence/set-based-merge-mac/summary.json`, with one
verified JSON artifact per engine, volume, and round in the same directory.

## What remains unproven

- Conflict-heavy performance and a production conflict distribution are
  unmeasured. The paired suite proves enumerated same-field, unrelated-field,
  competing-branch, and concurrent-delete cases only.
- Million-row wall time, memory, WAL, lock duration, deadlock behavior, and
  restart behavior under repeated worker death are unmeasured.
- InventoryItem, Interface, IPAddress, Prefix, Device, Site, and all other model
  families remain on their existing paths. InventoryItem is still the largest
  per-row cost in both staging and merge and still requires an MPTT/component
  proof.
- Existing-PK creates, primary-MAC changes, relation-bound deletes, subscribed
  updates, changed tags, non-Interface GFKs, runtime extensions, and any failed
  exact-version preflight remain current-path operations.
- No five-minute merge, 1,182,931-change runtime, or overall 8-15x portfolio
  speedup is claimed.
