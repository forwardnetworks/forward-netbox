# Harness Engineering Restructure

## Goal

Make the repository substantially more harness-based: agent-legible repo map, architecture map, project knowledge docs, plan structure, and mechanical validation.

## Constraints

- Preserve production sync behavior.
- Avoid production module moves in this pass.
- Keep direct-push release workflow as the default.
- Keep sensitive-content and CI gates central.

## Touched Surfaces

- Root agent and architecture guidance.
- Project knowledge docs.
- Plan templates and plan directories.
- Harness validation script.
- Invoke tasks and GitHub Actions.

## Approach

Added root guidance, stable boundary maps, validation/release/local workflow docs, plan structure, and a script-backed harness check. CI and local tasks now run the harness check so missing repo knowledge or malformed plans are caught mechanically.

## Validation

- `invoke harness-check`
- `invoke lint`
- `.venv-release/bin/mkdocs build --strict`
- `invoke check`
- `invoke test`
- `python -m build`

## Rollback

Remove the added harness docs/script, remove `harness-check` from `tasks.py` and `.github/workflows/ci.yml`, and restore docs navigation.

## Decision Log

- Kept production code layout unchanged to avoid turning a harness pass into a sync refactor.
- Added structural plan validation to make future multi-file work explicit before implementation.
