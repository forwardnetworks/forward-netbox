# Config backup: scale-tested, and a real memory finding fixed

## Goal

Measure config backup against the fleet size it was designed for - 3,400
devices, 2.4 GB - rather than the one-device fixtures every prior test used,
and fix what that measurement found.

## Why

The original plan reasoned about memory from the NQE page size alone: "page
size 100 keeps worst-case page memory ~2 GB only if 100 such outliers
cluster, which the probe says they do not." That reasoning addresses FETCH
memory. It says nothing about what happens to a row after it is fetched.

Measured directly: 3,400 synthetic devices, ~1.9 GB of configs (sized to the
probed distribution, one 20 MB outlier), pushed through the real code path to
a real git server. Peak RSS grew by **4.2 GB** for a 1.9 GB payload - roughly
2.2x, not the near-zero the page-size reasoning implied.

The cause was `new_blobs`: every changed row's `Blob` was appended to a list
and held for the entire fetch loop, then written to the object store in one
pass afterward. On a first backup - or any run touching most of the fleet -
that list holds the whole corpus at once, each entry costing the raw text
plus dulwich's `Blob` wrapper and its zlib buffer. The page size bounds how
much is IN FLIGHT from Forward at any moment; it does not bound how much is
RETAINED after arrival, and retention is what dominated.

## Constraints

- No behaviour change to what gets written or what the result reports. This
  is a memory-shape fix only.
- The fix must not require holding fetched rows anywhere new; it removes a
  retention, it does not relocate one.

## Touched Surfaces

- `forward_netbox/utilities/config_backup.py` - write each changed blob to
  the object store as it is produced, inside the fetch loop, instead of
  collecting a list and writing it afterward.
- `forward_netbox/tests/test_config_backup.py` - a real page-boundary test
  asserting write/fetch ORDER, not merely a count.

## Approach

`repo.object_store.add_object(blob)` moves from a post-loop batch to
immediately after each blob is constructed. `DiskObjectStore.add_object`
writes a loose object to disk per call; there was never a reason to defer it,
only a reason (habit) not to.

## Validation

Re-ran the identical 3,400-device / 1.9 GB scenario after the fix: peak RSS
delta fell from 4.2 GB to 2.4 GB (a 42% reduction), elapsed time unchanged at
~53s, correctness identical (3,400 written, 0 unmapped, 0 warnings, push
succeeded). The residual 2.4 GB is the `config_entries` tree map plus the test
harness's own fake client holding all synthetic rows in memory at once - a
worse-than-production artifact of the test, not of the code, since the real
Forward client streams pages rather than pre-generating an entire fleet.

The new unit test crosses a real page boundary (`CONFIG_BACKUP_PAGE_SIZE + 5`
rows) and records fetches and writes on one shared timeline, asserting at
least one write lands before the second fetch. A first version of this test
asserted only the total count of `add_object` calls, which cannot distinguish
"batched afterward" from "written as produced" - both call it once per row,
merely at different times. Run as its own negative control against the
reverted code: the count-only version passed against the bug it was meant to
catch; the order-sensitive version correctly failed it (`timeline=['fetch',
'fetch', 'write' * 105]`), then passed once the fix was restored.

Full `test_config_backup` module: 13 tests OK.

## Rollback

Revert. The prior behaviour was correct, only more memory-hungry; nothing
here is a data-safety fix.

## Decision Log

- **Write inline, not in two passes.** The two-pass shape (accumulate, then
  flush) is a natural way to write this kind of loop and was never load-bearing
  - nothing downstream needs the blobs as a list, only the tree needs their
  SHAs, which `config_entries` already carries independently.
- **Fixed the test rather than accepting a passing-but-blind one.** A test
  that cannot fail against the defect it names is worse than no test: it
  reads as coverage that is not there. The negative control that exposed this
  is the same discipline as everywhere else in this codebase's test suite.

## Open

- The residual 2.4 GB includes the test harness's own row-holding, not
  production behaviour; a genuinely production-shaped memory measurement
  would need the real Forward client's streaming behaviour, which this
  environment cannot exercise without a live network of that scale.
- `config_entries` itself (all paths + SHAs for the whole tree) is retained
  for the full run because a `Tree` must be built in one pass; this is a
  much smaller structure than the blobs were (fixed-size dict entries versus
  full config text) and was not identified as a concern by the measurement.
