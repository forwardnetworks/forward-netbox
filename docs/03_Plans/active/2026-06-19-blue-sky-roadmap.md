# Blue-Sky Roadmap

Forward-looking ideas for the forward-netbox plugin, captured after the 1.5.x
line stabilized (bulk-ORM coverage complete, heavy UI actions on background jobs,
scope reconciliation + backfilled-tag + module-readiness workflows shipped).

Grouped by effort. Nothing here is committed work — this is a menu to pull from.

Out of scope: Forward **Predict** (paid, not GA). Reachability, path, and blast
radius are GA Forward capabilities and ARE in scope below.

---

## Quick Wins — toil and quality

### Release automation (`invoke release X.Y.Z`)

**What:** one command that runs the entire release flow: bump version
(`pyproject.toml`, `forward_netbox/__init__.py`) and the 3 README compatibility
tables, scaffold the release plan file, `git add -A` then the full local CI mirror
(pre-commit clean + run-twice, harness check, harness tests, py_compile, mkdocs
--strict, build), create the release branch, push, wait for GitHub CI on both
NetBox matrices, fast-forward `main`, tag, GitHub release with artifacts, PyPI
upload, then sync local `main` and delete the branch.

**Why:** the 1.5.x line was cut by hand release after release, including CI
round-trips from avoidable mistakes — the sensitive-content guard firing on an
**untracked** plan file (the mirror skips untracked files), and the plan-file
gate. A script that always `git add`s before the mirror and encodes the gates
removes both the toil and that class of failure.

**Touched surfaces:** `tasks.py`/`invoke` (or `scripts/release.py`), referencing
the existing `scripts/check_harness.py`. No product code.

**Effort:** small. Highest leverage relative to size.

### Plan-dir hygiene + Operations Guide

**What:** archive superseded plans from `docs/03_Plans/active/` to
`completed/`, and fold the live operator workflows — background-job actions
(prune orphans, create module bays, dependency preview, tag backfilled), scope
reconciliation, module readiness, snapshot selectors — into a single
**Operations Guide** under `docs/01_User_Guide/`.

**Why:** 30+ files in `active/`, many May-era architecture roadmaps that are
done or superseded. Operator-facing knowledge currently lives mostly in release
notes. One legible guide + a clean planning dir makes the repo maintainable for
the next contributor.

**Touched surfaces:** `docs/03_Plans/`, `docs/01_User_Guide/`, `mkdocs.yml`.

**Effort:** small–medium, docs only.

---

## Medium — operator confidence at scale

### Sync observability panel

**What:** a per-sync run-history view with per-model throughput/timing,
change-volume trend over runs, and a "what changed this sync and why" summary
(created/updated/deleted by model, with the apply-engine decision and reason).

**Why:** on large fabrics (5000+ devices) operators infer health from the
changelog plus ingestion issues. A first-class panel turns that into a glance.
Much of the data already exists in the execution ledger and per-model statistics
— this is largely surfacing, not new collection.

**Touched surfaces:** `views.py` + templates, `utilities/execution_ledger*`,
health summary blocks.

**Effort:** medium.

### Collection-gap health signal

**What:** extend the 1.5.9 backfilled work — trend the backfilled (tagged but
not freshly collected) device count across runs, flag a spike, and surface it in
the sync health summary instead of requiring a manual reconciliation probe.

**Why:** the backfilled count is a leading indicator of a Forward collection
problem (the 22 devices on the live fabric were real gear with a collection gap,
not a plugin issue). Today that requires a manual probe; this makes it a standing
dashboard number with a clear "investigate collection" call to action.

**Touched surfaces:** `utilities/scope_reconciliation.py`,
`utilities/health_summary_blocks.py`, the sync detail page.

**Effort:** medium, builds directly on shipped 1.5.9 code.

---

## Big Bets — product differentiation

### Surface Forward reachability / path / blast radius into NetBox

**What:** bring GA Forward analysis into NetBox as device/prefix panels or custom
fields — e.g. reachability state, representative path info, and **blast radius**
(what a device/link failure would impact). Read-only, sourced from NQE like the
rest of the plugin.

**Why:** today NetBox is an inventory mirror of Forward. Forward also knows
operational truth — reachability, paths, blast radius — that NetBox has no view
of. Surfacing it makes NetBox a richer source of truth and differentiates the
integration beyond inventory sync.

**Scope note:** GA capabilities only. Forward **Predict** (paid, not GA) is
explicitly excluded.

**Touched surfaces:** new NQE maps + query specs, new sync models or custom
fields, device/prefix detail panels.

**Effort:** large. Biggest payoff.

### Bidirectional drift report / write-back guardrails

**What:** a "NetBox says X, Forward says Y" drift report extended beyond device
scope to IPs, prefixes, platforms, etc. — catching operator edits in NetBox that
diverge from Forward ground truth. Optionally, guardrails before write-back.

**Why:** the sync is one-way (Forward → NetBox). Operators can edit NetBox and
silently diverge. The scope-reconciliation panel already proves the "compare two
sources" pattern works; this generalizes it to more models.

**Touched surfaces:** a drift utility (generalize `scope_reconciliation.py`),
a drift view/report, optionally an audit command.

**Effort:** large.

---

## Suggested order

1. Release automation (small, immediate toil + error reduction).
2. Plan hygiene + Operations Guide (small, makes the repo legible).
3. Collection-gap health signal (medium, extends shipped work).
4. Sync observability panel (medium).
5. Reachability / path / blast-radius surfacing (large, differentiating).
6. Bidirectional drift report (large).
