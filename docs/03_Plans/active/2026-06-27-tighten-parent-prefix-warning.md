# Tighten the "no covering prefix" IP diagnostic wording

**Date:** 2026-06-27 — message-only; no version bump (rides the next release).

## Goal
The parent-prefix diagnostic said uncovered IPs are "normal for /32 loopbacks and
/31 or /127 links." That overstates: the prefix query
(`forward_prefixes_ipv4.nqe` / `_ipv6.nqe`) imports /31 and /127 link subnets as
prefixes (it only excludes `prefixLength == 32` / `128`), so /31//127 host IPs ARE
covered. The genuinely uncovered set is /32 and /128 host addresses with no
broader connected subnet (loopbacks, anycast, some VIPs).

## Constraints
- Logging text only. No behavior, query, or schema change.

## Touched Surfaces
- `forward_netbox/utilities/query_diagnostics.py` — `append_ipaddress_parent_prefix_diagnostics`
  warning text.

## Approach
Replace "/32 loopbacks and /31 or /127 links" with "/32 and /128 host addresses
(loopbacks, anycast, some VIPs) that have no broader connected subnet to derive a
prefix from."

## Validation
Lint. No test asserts the old string. Full suite at next release.

## Rollback
Revert the one-line message.

## Decision Log
- Confirmed against the prefix query: only /32 (v4) and /128 (v6) are excluded
  from prefix derivation; /31//127 subnets are emitted and cover their host IPs.
  So the prior wording named link sizes that are actually covered.
