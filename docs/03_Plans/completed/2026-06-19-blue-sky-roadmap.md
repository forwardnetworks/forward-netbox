# Blue-Sky Roadmap

## Goal

Capture forward-looking ideas for the forward-netbox plugin after the 1.5.x line
stabilized (bulk-ORM coverage complete, heavy UI actions on background jobs,
scope reconciliation + backfilled-tag + module-readiness workflows shipped). This
is a menu to pull from, not a single committed change.

## Constraints

- Forward **Predict** (paid, not GA) is out of scope. Reachability, path, and
  blast radius are GA Forward capabilities and ARE in scope.
- Read-only sourcing via NQE, consistent with the existing sync model.
- No customer data, credentials, or network ids in repo/tests/docs.

## Touched Surfaces

Varies per idea — see each item under Approach for its specific surfaces. Spans
`scripts/`, `tasks.py`, `docs/`, `forward_netbox/views.py` + templates,
`forward_netbox/utilities/` (scope reconciliation, health summary, execution
ledger), and new NQE maps/query specs.

## Approach

### Quick wins — toil and quality

**Release automation (`invoke release X.Y.Z`).** One command for the full release
flow: bump version + 3 README tables, scaffold the plan, `git add -A` then the
local CI mirror, branch, push, wait for CI, FF main, tag, GitHub release, PyPI,
sync. Removes per-release toil and the avoidable CI round-trips (sensitive guard
firing on untracked files, plan-file gate). Surfaces: `scripts/release.py`,
`tasks.py`. *Shipped in this branch.*

**Plan-dir hygiene + Operations Guide.** Archive superseded plans to
`completed/`; fold the live operator workflows into one Operations Guide.
Surfaces: `docs/03_Plans/`, `docs/01_User_Guide/`. *Shipped in this branch.*

### Medium — operator confidence at scale

**Sync observability panel.** Per-sync run history: per-model throughput/timing,
change-volume trend, and a what-changed-and-why summary from the execution ledger
and per-model statistics. Surfaces: `views.py` + templates, execution ledger.

**Collection-gap health signal.** Trend the backfilled (tagged-but-not-collected)
device count, flag spikes, and surface it in the sync health summary instead of a
manual probe. Surfaces: `utilities/scope_reconciliation.py`,
`utilities/health_summary_blocks.py`, sync detail page.

### Big bets — product differentiation

**Surface Forward reachability / path / blast radius into NetBox.** Shipped the
GA-NQE-derivable proxies in 1.6.x (device-analysis: reachability/collected,
connectivity-degree blast radius, real CVE). DECISION (2026-06-19): do NOT
reimplement true path search / Predict blast radius in NetBox — it duplicates
Forward's strength on the wrong (static) surface, the real blast radius needs
Predict (paid/non-GA) + scenario context, and impact decisions are made in Forward
during change planning. If pivot-to-Forward is wanted, add a cheap device-panel
**deep-link** to Forward's path-search/blast-radius UI (best ROI); build a stored
criticality metric only on a concrete customer ask, gated on Predict availability.

**Bidirectional drift report.** Generalize `scope_reconciliation` into a
multi-model NetBox-vs-Forward drift report (IPs, prefixes, platforms), catching
operator edits that diverge from ground truth. Surfaces: a drift utility, a drift
view/report, optionally an audit command.

## Validation

Each item ships with its own tests and the standard local CI mirror + GitHub CI
on both NetBox matrices. This roadmap doc itself needs no validation beyond
mkdocs build.

## Rollback

Per-item; each is independent and revertable on its own. Removing this doc has no
runtime impact.

## Decision Log

- Suggested order: (1) release automation, (2) plan hygiene + Operations Guide,
  (3) collection-gap health signal, (4) sync observability panel, (5) reachability
  / path / blast-radius surfacing, (6) bidirectional drift report — cheapest /
  highest-leverage first, large differentiating bets last.
- Predict excluded because it is a paid, non-GA Forward feature; blast radius kept
  because it is generally included.
