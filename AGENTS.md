# Forward NetBox Agent Guide

This repository is a NetBox plugin that syncs Forward Networks inventory into NetBox through Forward API/NQE and stages changes through `netbox_branching`.

## Start Here

- Read `ARCHITECTURE.md` before changing production code.
- Use `docs/00_Project_Knowledge/README.md` for validation, release, local Docker, and quality guidance.
- Use `docs/03_Plans/plan-template.md` for non-trivial implementation plans.
- Keep user-facing docs in `docs/01_User_Guide/` and reference docs in `docs/02_Reference/`.

## Core Boundaries

- `forward_netbox/models.py` owns persisted plugin state and job entrypoints.
- `forward_netbox/utilities/forward_api.py` owns Forward API access, proxy handling, pagination, and NQE execution.
- `forward_netbox/utilities/multi_branch.py` and `branch_budget.py` own branch-native sync planning and execution.
- `forward_netbox/utilities/sync.py` owns NetBox model adapters and row application.
- `forward_netbox/queries/` contains shipped NQE; keep query changes paired with tests and reference-doc updates.

## Working Rules

- Prefer NetBox-native and Branching-native behavior over side channels.
- Do not commit customer identifiers, tenant labels, network IDs, snapshot IDs, credentials, or screenshots that expose private data.
- Put local-only sensitive patterns in `.sensitive-patterns.local.txt`.
- Keep release commits authored by the human operator configured in git.
- Do not split large production modules casually. First document the boundary and add tests that preserve behavior.

## Agent Workflow

- Use `docs/00_Project_Knowledge/agent-workflow.md` to choose the correct path for docs, queries, sync behavior, UI/API behavior, and releases.
- Use `docs/00_Project_Knowledge/code-boundary-map.md` before touching overgrown modules.
- Create or update a plan under `docs/03_Plans/active/` for non-trivial behavior changes, then move it to `completed/` with validation evidence when done.
- Run `invoke harness-check` before committing. It validates required repo knowledge and plan structure.

## Required Checks

Use the smallest relevant set while developing, and run the full gate before releases:

```bash
invoke harness-check
invoke lint
invoke check
invoke test
invoke docs
invoke ci
```

If `invoke docs` fails locally because a docs dependency is missing, install the Poetry dev dependencies or match the GitHub Actions dependency list before treating it as a docs failure.
