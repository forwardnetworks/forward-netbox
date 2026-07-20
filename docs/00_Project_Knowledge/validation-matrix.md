# Validation Matrix

Run the smallest gate that proves a change, then run the complete release gate
before publishing.

| Change type | Required validation |
| --- | --- |
| Documentation only | `invoke harness-check`, `invoke harness-test`, `invoke docs` |
| Query map or NQE helper | Harness, lint, full Django tests, built-in NQE reference, and validation-org publication audit |
| Forward API client | Lint, Django check, full tests, retry/rate/pagination coverage, and stored API-budget evidence |
| Planning, branch, merge, or recovery | Lint, check, scenario tests, full tests, exact-version smoke, and Playwright for changed UI/API paths |
| Validation, scope, drift, or ownership | Lint, check, scenarios, full tests, Playwright, ownership audit, and customer-equivalent sync evidence |
| NetBox model adapter | Lint, check, scenarios, full tests, repeat-sync no-op coverage, and targeted exact-version sync |
| Optional plugin integration | Harness, lint, check, full tests, architecture audit, no-plugin startup, exact installed-plugin migration, live row shape, and repeat-sync idempotence |
| UI/API workflow | Lint, check, full tests, Playwright, and browser verification |
| Release | Every core command below, fresh migration/install, customer-equivalent acceptance, and GitHub CI on the release commit and tag |

## Core Commands

```bash
invoke harness-check
invoke harness-test
invoke lint
invoke check
invoke scenario-test
invoke test
invoke playwright-test
invoke docs
invoke package
invoke ci
```

Do not run Django or Playwright tests against the shared local runtime while a
Forward sync is queued, syncing, or merging. The test tasks inspect current
`ForwardSync` state and use an isolated Compose project when the shared runtime
is active or cannot be inspected.

## Exact Runtime

Release evidence must use NetBox `4.6.5`, Branching `1.1.1`, Python `3.14`, and
the wheel built from the release commit. A fresh database must apply all NetBox,
Branching, optional-plugin, and Forward NetBox migrations without model drift.

The exact-runtime acceptance run must verify:

- preflight and query execution complete on one snapshot
- one branch is staged and merged with durable merge attestation
- the ingestion becomes baseline-ready only after merge finalization
- device identity and ownership domains finalize for the same ingestion
- an unchanged repeat sync is a no-op
- failed rows, stale generations, ambiguous identities, and protected deletes
  fail closed with persisted evidence
- `forward_ownership_audit --fail-on-inconsistent --require-no-open-branches`
  passes after completion

## Optional Plugins

Routing, Peering Manager, Cisco ACI, and DLM are supported only for the model
sets declared in `plugin_integrations/registry.py`. Acceptance must test both
plugin-absent startup and the exact installed-plugin matrix. Every declared
model requires an apply/delete adapter, shipped query contract, migration, and
repeat-sync test.

The Cisco ACI supported set is:

- fabric, pod, node, tenant, VRF, bridge domain, application profile, endpoint
  group, contract, filter, L3Out, and static port binding
- APIC node and CIMC inventory from the current command inventory declared in
  the integration registry

The DLM supported set is software versions, hardware notices, device software,
CVEs, and per-device vulnerabilities. Acceptance must prove that standalone
software versions without a device association are not created and that CVE
details retain their software-version and device associations.

## Live Evidence

Use a configured validation source and sanitized evidence:

```bash
invoke validation-org-query-audit --source-name '<validation source>' --fail-on-gap
invoke smoke-sync --plan-only
invoke smoke-sync
invoke sync-release-gate --sync-ids '<sync id>'
invoke runtime-capacity-review --source-name '<validation source>'
```

For repeated operational soak runs:

```bash
invoke scale-soak --runs 3 --max-changes-per-staging-item 10000
```

Keep source query concurrency within measured worker, PostgreSQL, and Forward
API capacity. Use the structured Health and support-bundle evidence instead of
inferring readiness from fetched row counts.

## Sensitive Content

```bash
python scripts/check_sensitive_content.py
python scripts/check_sensitive_content.py --protected-history
python scripts/check_sensitive_content.py --git-files --protected-history \
  --require-env-patterns --require-baseline-env
```

Use `.sensitive-patterns.local.txt` for customer-local identifiers that must not
enter tracked files. Release CI additionally requires a nonempty secret-backed
pattern feed and an external baseline trust anchor supplied independently of
the tracked files; never derive the trust-anchor environment variable from the
candidate baseline. Exact current hashes approve reviewed binary documentation
assets. Historical binary exceptions require externally supplied
commit/path/digest approval.
