# Collapse benign IP-diagnostic noise to one line each

**Date:** 2026-06-26
**Status:** staged for the next release (no version bump).

## Goal
Stop the two benign IP diagnostics ("filtered unassignable", "no imported parent
prefix") from logging a wall of per-row example lines on a large network. Keep the
information, drop the noise.

## Constraints
- Logging volume only — no change to filtering/import behavior or data.
- Keep the count, the reason breakdown, and a few examples (no information lost).

## Touched Surfaces
- `forward_netbox/utilities/query_diagnostics.py` — both diagnostics now emit ONE summary line (count + plain-English "this is normal because…" + up to 3 inline examples + `(+N more)`), instead of summary + up to 20 per-row lines + a "suppressed N" line.

## Approach
Both diagnostics already produced a good summary line; they just also logged the
capped example list row-by-row plus a suppression notice. Fold up to 3 examples
into the summary and drop the per-row loop and the suppression line. The
`summarize_*` example cap (`IPADDRESS_*_DETAIL_LIMIT`) is unchanged and still used.

The apply-side `missing-interface` ("target interface was not imported") warning
is deliberately NOT collapsed: a live NQE probe showed the interface IS in the
source data, so that skip is an apply-time signal worth keeping visible, not
benign noise.

## Validation
Full suite 888 green (no test asserted the per-row diagnostic strings). Lint.

## Rollback
Single-file, logging-only. Revert restores the prior verbose output.

## Decision Log
- One line with inline examples over a separate summary + 20 rows + suppression:
  the operator action is identical for every row (none — it's expected), so a
  count plus a few examples conveys everything with none of the wall.
- Left `missing-interface` verbose on purpose — it can indicate a real apply gap;
  silencing it would hide that.
