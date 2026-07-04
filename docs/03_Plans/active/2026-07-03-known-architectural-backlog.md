# Known architectural backlog (tracked, not scheduled)

**Date:** 2026-07-03
**Source:** post-2.2.5 read-only architecture audit (4 agents).

## Enterprise / GA-readiness roadmap (2026-07-04, 7-agent assessment)

Verdict: **near-GA** — strong engineering core (reliability, observability,
idempotency, test depth all above NetBox-plugin baseline). Tranche 1 (see
[2026-07-04-enterprise-hardening-tranche-1](2026-07-04-enterprise-hardening-tranche-1.md))
landed the safe quick wins: stopped the doc-publish confidentiality leak, scrubbed
a committed customer name + hardened the scanner, added SECURITY.md / Dependabot /
pip-audit / migration-drift guard / issue+PR templates / pyproject metadata, and
made the missing-user sync attribution auditable. **Remaining GA items:**

- **GA-blocker — Trusted Publishing — DONE 2026-07-04.** Added
  `.github/workflows/release.yml`: pushing a `v*` tag builds + publishes to PyPI via
  OIDC Trusted Publishing with PEP 740 attestations (no stored token); `release.py`
  now points at it with twine as fallback. **One-time setup still required by the
  maintainer:** add the GitHub Trusted Publisher on PyPI (owner=forwardnetworks,
  repo=forward-netbox, workflow=release.yml, environment=pypi) and create a `pypi`
  GitHub environment. Until that is configured, the first tag-publish will fail and
  the twine fallback is used.
- **GA-blocker — upgrade safety — PARTIAL 2026-07-04.** Wrote the UPGRADE +
  ROLLBACK guide (`docs/01_User_Guide/upgrade.md`): backup-first, migrate, verify,
  and restore-from-backup rollback with the destructive-migration caveat. **Still a
  project:** an automated upgrade/downgrade test on a POPULATED DB across the
  destructive migrations (needs `django-test-migrations` or a hand-rolled migration
  executor + data fixtures) — deliberately not authored blind.
- **Decision — support posture / GA framing.** README still carries an
  "unsupported / not an official Forward product / no warranty" disclaimer and
  `Development Status :: 4 - Beta`; site is named "Forward Field Integration". These
  are business/legal calls (does Forward officially support + warrant this?), left
  for the maintainer, not changed unilaterally. Bumping to Production/Stable + a
  support-lifecycle statement gates on this decision.
- **Tier-1 — credential at rest (project).** Forward API password is plaintext in a
  JSONField (models.py); masking is display-only. Encrypt at rest or formally
  document the DB-at-rest-encryption requirement.
- **Tier-1 — object-permission trust boundary (decision/medium).** Sync/merge
  writes bypass NetBox object permissions (inventory-wide create/update/delete).
  Document as an intended trust boundary at minimum, or gate on underlying
  dcim/ipam perms.
- **Tier-1 — CI matrix + coverage floor (medium).** CI runs a single pinned NetBox
  patch; `.coveragerc` exists but coverage is never measured/enforced; no lockfile
  (tested != shipped). Restore a min+latest 4.6.x matrix (note: routing/peering
  optional-plugin caps still apply — see item 6), measure+floor coverage, commit a
  lockfile.
- **Tier-2 — enterprise ops surface.** No Prometheus/OTel metrics export (rich
  telemetry trapped in job.data/health page); stuck-job detector has no autonomous
  alert; `prune_orphan_devices` deletes by device NAME not PK (cross-site name
  collision risk); SaaS rpm hard-block is advisory-only; retry backoff lacks jitter
  and ignores Retry-After; no SBOM; no deprecation policy; no consolidated REST/API
  reference; CODEOWNERS still needs the maintaining team handle.

---

## Goal
Keep a durable register of the genuine structural items the audit surfaced, with
an explicit defer/blocked rationale for each, so they are tracked rather than lost.
This document schedules no work; it is the backlog the next planning pass draws
from.

## Constraints
- These items are project-sized, migration- or correctness-sensitive, or blocked
  on field data — none is a safe drive-by edit.
- The audit confirmed the source is otherwise clean (no dead code, no TODO/FIXME,
  no version shims), so this register is deliberately short.

## Touched Surfaces
None — this is a tracking document. Each item below names the code it would touch
if/when scheduled.

## Approach
Record each item with its current code status and a one-line verdict. Revisit at
the next planning pass; promote an item to its own plan only when it is scheduled.

## Backlog items

### 1. Dual apply engine + per-model parity matrix — DEFER (deliberate migration)
`apply_engine_decision.py` gates every model between the adapter path
(`sync_runner_adapters.py`) and the bulk-ORM path (`apply_engine_bulk.py`) via
hand-maintained sets (`BULK_ORM_ENABLED_MODELS`, `ADAPTER_REQUIRED_MODELS`,
`SIMPLE_BULK_CANDIDATE_MODELS`, `BULK_ORM_PARITY_GATES`). Both paths are
load-bearing; each model is promoted individually behind a parity test. Largest
structural tax, but an intentional in-flight migration. Converging to bulk-only is
a multi-release project, not a cleanup edit.

### 2. Unreachable `multi_branch` scaffolding — RESOLVED 2026-07-04
Removed. An investigation workflow (adversarial reachability, high confidence)
confirmed multi-branch execution is unreachable: the only dispatch path builds
`ForwardSingleBranchExecutor` unconditionally and exactly one branch is ever
provisioned. Retired the always-True fossil: stopped writing `parameters["multi_branch"]`
(model_validation, sync_facade, forms ×2, sync_state display), removed the
`uses_multi_branch()` method/import/definition and the `uses_multi_branch` workload-
summary key. NO schema migration (the keys lived only in the `parameters` JSON;
the one real column was on the already-dropped `ForwardExecutionRun`). Back-compat
preserved: `multi_branch` stays in the `clean_forward_sync` allowlist so old stored
syncs still validate (proven: a sync with `multi_branch=True` still `clean()`s).
`max_changes_per_branch` is untouched — it remains a live telemetry/budget param.
Naming debris (`multi_branch_lifecycle.py` module, `ForwardFastBootstrapExecutor`
class) is load-bearing and left for a separate cosmetic rename follow-up.

### 3. Density-learning dead write-loop — RESOLVED 2026-07-04
Removed `density_learning.update_density_learning` + `should_accept_observation`
(no production caller — the learned profile was never updated from observed runs)
and the three constants only they used, plus their two tests. The budget READ path
(`density_profile_summary`, `density_budget_policy`, `normalize_density_*`) and
`max_changes_per_branch` are LIVE and untouched. Suite 937 green (two dead tests
gone). A broader density simplification (fixed sub-batch size vs. the profile
machinery) remains a separate, larger question — not scheduled.

### 4. 1-created/1-deleted idempotency churn — BLOCKED (needs field data)
The read-only diagnostic shipped (`apply_identity_audit.py` +
`forward_apply_identity_audit`, 2.2.3) and flags `churn_suspect_models` by
comparing Forward-computed vs NetBox-stored identity keys. The root-cause fix
(stable identity keys in the apply path) needs Partner's audit output to pinpoint the
drift before it can be written.

### 5. NQE query consolidation — DEFER (smaller than believed)
Real de-dup surface is only ~2 v4/v6 variant pairs (`forward_ip_addresses_*`,
`forward_prefixes_*`); the 13 `forward_aci_*` queries are genuinely distinct.
Payoff is small; optional polish, not debt.

### 6. Optional-plugin CI coverage — BLOCKED upstream (investigated 2026-07-04)
Real gap, but not fixable at the current pins. `development/Dockerfile` installs
`netbox-routing==0.4.2` + `netbox-peering-manager==0.2.2`, but
`development/configuration/plugins.py` only ENABLES them when `NETBOX_VER`
starts with `v4.5`. Since 4.5 was dropped and CI is v4.6.4-only, both stay
disabled and 27 routing/peering tests skip in CI (covering the ~25KB
`sync_routing_impl.py` BGP/OSPF apply path). Attempting to enable them on 4.6.4
fails: routing 0.4.2 raises `CheckConstraint.__init__() got an unexpected keyword
argument 'check'` (NetBox 4.6's Django renamed `check=`→`condition=`), and
peering-manager 0.2.2 declares `max_version 4.5.99`. So the `v4.5` gate is
protective, not stale. **To close:** bump both plugins to 4.6-compatible upstream
releases (when available), update the Dockerfile pins, then enable on 4.6 and
confirm the 27 tests pass. Until then the skips are expected. (`plugins.py` now
carries a comment documenting this.)

## Validation
Not applicable — no code changes. The audit basis is recorded in the companion
`2026-07-03-tech-debt-cleanup.md` plan.

## Rollback
Not applicable — documentation only.

## Decision Log
- Record rather than execute: every item is project-sized or blocked; ripping any
  out immediately before a release trades real risk for cosmetic gain.
- Explicitly mark #4 BLOCKED on Partner's field data and #6 as the one small,
  genuinely-actionable follow-up, so the next pass can pick it up without re-deriving.
- The `_impl` + thin public-module seam and the inert TURBOBULK/PARQUET_BULK enum
  members are intentional and stay; the audit found no dead code, markers, commented
  blocks, or version shims to remove.
