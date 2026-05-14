# Forward Roadmap: Intent, Path, Blast Radius, Predict

## Goal

Define the next product layers for the NetBox plugin so it can expose Forward-native intent verification, path search, blast-radius style impact analysis, security posture checks, and eventual predict workflows without leaving the NetBox-native sync model.

The outcome should be a roadmap that keeps the current sync bridge intact while adding read-only/operator-facing analysis surfaces around it.

## Constraints

- Keep NQE as the normalization and source-of-truth layer for row shape.
- Keep NetBox-native objects and Branching-native execution as the mutation path.
- Do not add a separate query authoring system in NetBox.
- Do not add customer-specific shortcuts, hidden overrides, or data-mutation logic in Python that competes with NQE contracts.
- Preserve the current sync workflow, including query-path binding, query-id resolution, and branch-backed review.
- Keep any future analysis features read-only unless a separate implementation plan explicitly adds a mutation path.
- Do not commit customer identifiers, network IDs, snapshot IDs, screenshots, or credentials in examples, tests, or docs.

## Touched Surfaces

Planned implementation work will likely touch:

- `forward_netbox/models.py`
- `forward_netbox/views.py`
- `forward_netbox/forms.py`
- `forward_netbox/tables.py`
- `forward_netbox/api/`
- `forward_netbox/utilities/`
- `forward_netbox/queries/`
- `forward_netbox/tests/`
- `docs/01_User_Guide/`
- `docs/02_Reference/`
- `docs/03_Plans/`

## Approach

Build the roadmap in four layers.

### 1. Analysis surfaces first

Expose read-only Forward analysis results alongside sync state so operators can see:

- intent verification results
- path verification/search results
- security posture results for firewall and enforcement-point analysis
- known routing or reachability warnings
- model-scoped diagnostics for a sync run

These surfaces should be attached to existing NetBox sync and ingestion records instead of creating a separate navigation model.

### 2. Impact before mutation

Add blast-radius and change-scope previews that answer:

- what objects would be affected
- how large the resulting branch/workload would be
- whether the workload should stay on the branch path or move to the fast bootstrap path
- whether the run should be split or delayed

This layer should help users reason about oversized imports before they hit NetBox Branching limits.

When possible, keep these previews tied to the same path-search and intent-verification semantics that Forward already exposes, rather than inventing a new policy language in the plugin.

### 3. Predict readiness

Prepare the plugin to accept predict-style results as a future advisory layer:

- keep the change-set and sync context available
- preserve the exact imported state separate from predicted state
- allow predicted results to be presented as guidance, not as mutation authority

The plugin should not become a second modeling engine. It should ask Forward to evaluate the scenario and then display the result in a NetBox-native workflow.

### 4. Unified reporting

Surface all analysis results through the same operational objects already used for sync:

- ingestion detail pages
- validation runs
- job logs
- model-specific result panes
- API endpoints for automation

The operator experience should be “look at the sync, see the analyses, decide what to do next,” not “jump into a separate Forward app clone.”

### 5. Keep curated interesting-paths separate

Treat the `interesting-path-queries` surface as a curated diagnostics/helper workflow rather than a core verification primitive.

It is useful for PoCs, demos, and exploratory troubleshooting because it samples representative firewall/load-balancer and suspicious-path cases, but it should remain separate from:

- intent verification
- path verification/search
- security posture
- blast radius
- predict advisories

The plugin can expose or consume those curated results where helpful, but the roadmap should not depend on them as the primary user contract.

### 6. Add lifecycle enrichment

Add EoS/EoL-style lifecycle enrichment as an inventory advisory layer.

This should help operators prioritize upgrades and replacements by surfacing lifecycle risk against the NetBox object model:

- manufacturer
- device type
- platform
- software version
- support window or end-of-life date

Like CVE data, lifecycle enrichment should remain read-only context around the source of truth, not part of the mutation contract.

## Validation

This is a roadmap plan, so the immediate validation is document review and alignment against the current architecture.

When the implementation starts, each tranche should require:

- unit tests for any new result coercion, binding, or summary logic
- model tests for any new persisted metadata
- API tests for any new analysis endpoints
- UI tests or Playwright coverage for the operator-visible surfaces
- `invoke harness-check`
- `invoke harness-test`
- `invoke lint`
- `invoke check`
- `invoke scenario-test`
- `invoke test`
- `invoke docs`
- `invoke ci` before release

## Rollback

If the roadmap is implemented incrementally, each tranche should be reversible by removing its new read-only surfaces and leaving the core sync path unchanged.

The safe rollback boundary is:

- keep the current query registry and sync execution untouched
- remove the new analysis views, serializers, and docs for the tranche being reverted
- preserve existing sync records and ingestion history

## Decision Log

- Rejected building a separate analysis engine in the plugin because Forward already owns the computation and NQE contract.
- Rejected turning NetBox into a query authoring environment because the current product direction keeps query logic in Forward.
- Rejected treating predict as a mutation path because it should inform the operator, not replace the NetBox-native sync and review model.
- Rejected coupling blast-radius sizing to hidden heuristics because the user should see the same object-model boundaries the plugin actually uses.
- Rejected calling security posture a generic posture check in this plan because the product surface is specifically firewall/security-posture oriented.
- Rejected making interesting-path generation the core roadmap primitive because it is a curated diagnostics surface, not the main verification contract.
- Rejected treating EoS/EoL as a separate lifecycle system because it is better handled as inventory enrichment around the NetBox object model.
