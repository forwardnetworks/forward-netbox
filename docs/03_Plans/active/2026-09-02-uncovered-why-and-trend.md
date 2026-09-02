# Uncovered devices: why, and whether it is growing

## Goal

Answer the two questions the `forward-uncovered` tag left open: WHY each
device this sync created is now uncovered, and whether the count is growing.
And make the unclaimed half listable in full, which nothing could do.

## Why

A customer reported 552 uncovered devices and that the number keeps growing.
`2026-09-01-uncovered-device-tag.md` made the owned half listable and recorded
"no health signal in this change" as its deferral. The cause split existed
for orphans since 2.7.x and never for the owned set; the trend existed for the
other two buckets and never for this one; the unclaimed half had a 25-name
sample and no way to see the rest.

## Constraints

- No new Forward query shape. The census that classifies orphans already
  carries no predicate, so it classifies the owned set in the same execution.
- Names stay capped at the sample size in the persisted payload. Keys are
  persisted instead, and NetBox's own device table renders them.
- The unclaimed half is not tagged. It is not this sync's data.

## Touched Surfaces

- `scope_reconciliation.py` - `_absence_kinds` / `_absence_summary` (the
  classifier split into "ask once" and "summarise a set"); the call site
  classifies `out_of_scope | owned_untagged` once; `unmanaged.owned_absence`;
  `owned_untagged_device_ids` / `unclaimed_device_ids`.
- `health.py` - `_uncovered_summary`, escalating to `danger` on growth;
  `_out_of_scope_summary` now escalates the same way.
- `views.py` - `forwardsync_uncovered_devices` and
  `forwardsync_unclaimed_devices`, one read-only table view each.
- The scope-reconciliation panel (cause badges, list links), the health page
  (Uncovered card), and `forwardsync_uncovered_devices.html`.

## Approach

The classifier used to take the orphan set, run the census, and return the
panel's counts. It now takes any set of names and returns each name's kind;
a second function turns a set plus those kinds into the panel's counts. The
call site asks once for the union and summarises twice.

## Validation

`test_uncovered_absence_and_trend.py`: the owned set split by cause; one
census for both sets; the customer's exact shape (orphans 0, owned present);
no census when nothing is absent; a failed census reads unavailable for both
sets rather than zero; keys persisted, names capped; the health signal at
info / warn / danger with the trend text; both list pages and the panel links.
Adjacent: `test_uncovered_device_tag`, `test_device_scope_reconciliation_audit_
command`, `test_scope_module_ui`, `test_health`. Full Django suite.

## Rollback

Revert. The panel loses the badges and the links; the health page loses the
card; the classifier returns to orphans only.

## Decision Log

- **One extra NQE execution in exactly one case.** When orphans are zero and
  owned-uncovered devices exist, no census used to run because nothing asked
  for one. Now one does - that is the customer's exact shape, and the case
  with a question. `forward_api_usage` moves by one execution per
  reconciliation there and nowhere else; recorded here and pinned by a test
  so the change in the count is not mistaken for chatter.
- **Keys in the payload, not names.** The job payload is a persisted
  diagnostic and device names are customer data; the sample cap exists for
  that reason. Primary keys carry no such weight and are exactly what a
  device table needs.
- **Out-of-scope escalates too.** It computed a trend and then hard-coded
  `warn`; a steadily growing orphan set looked no different from a stable one.

## Open

- Nothing.
