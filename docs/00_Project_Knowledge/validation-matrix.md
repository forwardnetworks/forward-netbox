# Validation Matrix

Run the smallest gate that proves the change, then run the release gate before publishing.

| Change type | Required validation |
| --- | --- |
| Documentation only | `invoke harness-check`, `invoke harness-test`, `invoke docs` |
| Query map or NQE helper change | `invoke harness-check`, `invoke harness-test`, `invoke lint`, `invoke test`, update built-in NQE reference |
| Forward API client change | `invoke lint`, `invoke check`, `invoke test` |
| Sync planning or branch budget change | `invoke lint`, `invoke check`, `invoke scenario-test`, `invoke test`, local Docker sync smoke test |
| NetBox model adapter change | `invoke lint`, `invoke check`, `invoke scenario-test`, `invoke test`, targeted local Docker sync |
| UI/API workflow change | `invoke lint`, `invoke check`, `invoke test`, browser/UI verification when visible behavior changes |
| Release | `invoke ci`, GitHub CI success on `main` and tag |

## Core Commands

```bash
invoke harness-check
invoke harness-test
invoke lint
invoke check
invoke scenario-test
invoke test
invoke docs
invoke package
invoke ci
```

## Sensitive-Content Gate

The sensitive-content guard must stay in local and CI validation:

```bash
python scripts/check_sensitive_content.py
python scripts/check_sensitive_content.py --all-history
```

Use `.sensitive-patterns.local.txt` for local-only customer names, tenant labels, network IDs, or other identifiers that should never be committed.
