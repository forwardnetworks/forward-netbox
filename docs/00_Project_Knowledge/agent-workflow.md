# Agent Workflow

Use this workflow before changing the repository.

## Choose The Lane

| Work type | Required harness path |
| --- | --- |
| Small docs update | Update docs, run `invoke harness-check` and docs build |
| Query or NQE map change | Update query, tests, built-in NQE reference, and validation matrix if needed |
| Sync behavior change | Create active plan, update tests first, run NetBox checks and tests |
| UI/API behavior change | Create active plan, update tests and screenshots/docs when visible behavior changes |
| Release | Follow the release playbook and verify GitHub CI on branch and tag |

## Before Editing

- Read `ARCHITECTURE.md` and the [Code Boundary Map](code-boundary-map.md).
- Check `docs/03_Plans/active/` for existing work.
- Create an active plan for behavior, release, or architecture changes.
- Confirm the validation matrix contains the gate that will prove the change.

## During Editing

- Keep changes inside the documented boundary.
- Prefer tests over prose when behavior changes.
- Update docs near the changed behavior.
- Add local sensitive patterns for customer-derived names before running checks against live data.

## Before Commit

```bash
invoke harness-check
invoke harness-test
invoke lint
invoke check
invoke scenario-test
invoke test
```

For docs or release work, also run:

```bash
invoke docs
python -m build
```

Move active plans to `docs/03_Plans/completed/` only after validation evidence is recorded.
