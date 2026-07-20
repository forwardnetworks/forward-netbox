# Harness Engineering Alignment

Last reviewed: 2026-07-18

The repository is the system of record for architecture, behavior, and release
evidence. The harness is designed so a maintainer or
agent can reason about the product without reconstructing private conversations
or customer data. This is alignment with the principles in OpenAI's Harness
Engineering article, not a claim of implementation parity with OpenAI's
internal systems.

## Current Alignment

| Principle | Repository evidence | Status |
| --- | --- | --- |
| Repository knowledge | `AGENTS.md` routes to `ARCHITECTURE.md`, the boundary map, validation matrix, release playbook, and plans. | Aligned |
| Application legibility | Isolated Compose runtimes, seeded Playwright paths, native health/drift views, ingestion issues, and sanitized support bundles expose product state. | Aligned |
| Architecture enforcement | Harness checks, model/query contracts, migration tests, single-branch tests, strict merge tests, and ownership audits constrain high-risk changes. | Aligned |
| Feedback loops | Focused Django tests, scenario tests, full tests, Playwright, exact-version startup, package installation, and CI form an executable release ladder. | Aligned |
| Entropy control | Freshness gates, plan structure checks, sensitive-content scanning, and scheduled gardening expose drift. | Aligned with manual triage |
| Human steering | Humans set release scope and acceptance; automation prepares and verifies changes but does not silently publish a release. | Aligned |

## Mechanical Invariants

- `AGENTS.md` remains a short routing document rather than a complete manual.
- Core knowledge review dates remain within the freshness window enforced by
  `scripts/check_harness.py`.
- High-risk production, query, package, workflow, and migration changes include
  a versioned implementation plan.
- The 2.6 runtime gate uses NetBox `4.6.5` and
  `netbox-branching` `1.1.1`.
- Sync tests enforce one branch per sync and no direct-write bootstrap path.
- Merge tests enforce that partial application cannot become a completed
  baseline and remains inspectable/retryable.
- Ownership tests enforce main-schema-only, per-sync generation claims and
  stale-overlay rejection.
- Sensitive-content checks cover both the working tree and repository history.
- UI behavior changes include Playwright validation; release/package changes
  include fresh installation evidence.

## Application Legibility

The primary runtime facts are directly visible:

- `ForwardSync` and `ForwardIngestion` expose sync, snapshot, branch, job, and
  baseline state.
- validation rows and model results explain pre-branch policy and query results.
- ingestion issues and merge counters explain incomplete branch application.
- ownership reconciliation rows distinguish pending, completed, and failed
  post-merge domains.
- drift and health refuse to equate a merged branch with complete ownership.
- support bundles export sanitized evidence for these same persisted facts.

## Architecture Enforcement

The highest-risk contracts are executable:

- configuration normalization removes retired execution parameters
- branch provisioning occurs after query preflight and blocking validation
- branch-native bulk writes emit reviewable ObjectChanges
- custom merge dependency order and retry behavior are tested
- module bays materialize through the branch merge path
- ownership migrations are not applied independently to branch schemas
- post-merge overlays verify both snapshot and ingestion generation
- cross-sync union/last-claim behavior is transaction/concurrency tested
- ownership audit exits nonzero for inconsistent state

When a document and a test disagree, the discrepancy is a harness defect to
resolve before release; neither is accepted as intent by itself.

## Entropy Control

- Active plans are reviewed by content and state, not by a hard-coded file-count
  assertion.
- Completed plans retain evidence and do not define current product behavior.
- Scheduled gardening detects stale knowledge. Changes still require a reviewed
  human or automation identity; the workflow does not bypass repository review.

## Review Procedure

1. Verify the diff changes only the intended architecture, code, test, plan, and
   operator surfaces.
2. Run `invoke harness-check`, `invoke harness-test`, and `invoke docs`.
3. Run the focused behavior tests named by the boundary map, then the complete
   release ladder in `quality-score.md` on the exact dependency matrix.
4. Confirm drift, health, support bundles, and ownership audit all derive their
   conclusions from persisted current facts.
5. Confirm UI-changing paths remain directly drivable in the isolated
   Playwright runtime.
6. Confirm active plans have current decisions and evidence; do not infer plan
   health from a stale count.
7. Update review dates only after the document review and checks complete.
