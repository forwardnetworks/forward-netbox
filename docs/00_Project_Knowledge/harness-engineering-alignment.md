# Harness Engineering Alignment

Last reviewed: 2026-07-18

This project follows the agent-first harness principles described in OpenAI's
Harness Engineering article, but does not claim identical implementation or
full parity. The repository is the system of record and the harness is expected
to make product behavior, architecture, validation, and known debt legible to a
new agent without reconstructing private chat history.

## Current Alignment

| Principle | Repository evidence | Status |
| --- | --- | --- |
| Repository knowledge | `AGENTS.md` is a short map into `ARCHITECTURE.md`, project knowledge, references, plans, and runbooks. | Aligned |
| Application legibility | Compose projects isolate test runtimes by worktree; Playwright drives seeded UI paths and captures screenshots; health views and sanitized support bundles expose runtime evidence. | Aligned with gaps |
| Architecture enforcement | The boundary map, architecture audit, model/query contract tests, plan gate, and CI checks mechanically constrain high-risk changes. | Aligned |
| Feedback loops | Focused regressions, Django checks, scenario tests, full tests, Playwright, package builds, and release evidence form an executable ladder. | Aligned |
| Entropy control | Quality and debt documents exist, core knowledge freshness is enforced, and a weekly workflow runs the harness checks. | Partial |
| Human steering | Humans set release scope and acceptance; agents implement, test, collect evidence, and prepare branches without silently publishing releases. | Aligned |

## Mechanical Invariants

- `AGENTS.md` stays at or below 120 lines so it remains a map instead of a
  monolithic manual.
- This document and `quality-score.md` must carry a review date no more than 90
  days old.
- High-risk code, workflow, package, query, and production-boundary changes
  require a versioned plan in the same diff.
- Required knowledge files, headings, cross-links, validation commands, and CI
  hooks are checked by `scripts/check_harness.py` and its tests.
- The scheduled harness-gardening workflow detects stale core knowledge even
  when ordinary feature work is quiet.

## Known Gaps

1. The 2026-07-18 audit found 102 files in `docs/03_Plans/active/`. This is an
   entropy signal: active plans need evidence-backed triage into active,
   blocked, completed, or superseded states.
2. The weekly workflow detects knowledge drift but does not open cleanup pull
   requests. Automated write access requires a separately approved identity and
   review policy.
3. The isolated harness exposes application logs, UI state, job state, and
   support evidence, but it does not provide a full per-worktree metrics and
   traces stack. Add that only when concrete runtime questions justify the
   operational cost.
4. Customer field validation remains a release acceptance step. Synthetic and
   isolated tests cannot prove customer-specific Forward data, tag policy, or
   query publication state.

## Review Procedure

1. Run `invoke harness-check`, `invoke harness-test`, and `invoke docs`.
2. Count and review active plans; move only evidence-complete work and preserve
   unresolved decisions in the technical-debt tracker.
3. Confirm the quality score describes current code and runtime evidence rather
   than historical intent.
4. Confirm UI-changing workflows remain directly drivable in the isolated
   Playwright runtime.
5. Update the review dates only after the checks and document review complete.
