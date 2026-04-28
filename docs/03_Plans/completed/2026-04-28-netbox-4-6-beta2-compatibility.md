# NetBox 4.6.0-beta2 Compatibility Check

## Goal

Create an isolated compatibility branch that runs the plugin test and UI harness against NetBox `v4.6.0-beta2` so we can identify required changes before advertising NetBox 4.6 support.

## Constraints

- NetBox `v4.6.0-beta2` is a beta release and must not replace the validated `4.5.x` release line on `main`.
- TurboBulk is not part of NetBox core in 4.6; keep TurboBulk support optional and capability-detected.
- Do not commit customer data, network IDs, snapshot IDs, credentials, or generated logs.
- Preserve the existing native sync workflow and Branching behavior.

## Touched Surfaces

- `development/.env`: update local Docker NetBox image tag for this branch.
- `.dockerignore`: keep local-only artifacts out of the Docker build context.
- `.github/workflows/ci.yml`: build a missing NetBox beta base image in CI before the project image build.
- `tasks.py`: let local `invoke docs` use `.venv-docs` automatically when present.
- Production code was not changed because the compatibility run did not expose a required 4.6 fix.

## Approach

1. Built an exact local base image for NetBox `v4.6.0-beta2` because Docker Hub did not publish that tag.
2. Updated the local development NetBox image tag to `v4.6.0-beta2`.
3. Rebuilt the plugin development image against the local beta2 base.
4. Restarted the Docker stack and let NetBox apply 4.6 migrations.
5. Ran the local quality gates against the 4.6 deployment.

## Validation

- `docker run --rm --entrypoint /bin/bash netboxcommunity/netbox:v4.6.0-beta2 -lc 'cat /opt/netbox/netbox/release.yaml'`
  - Confirmed `version: "4.6.0"` and `designation: "beta2"`.
- `invoke build`
- `invoke start`
- `invoke check`
- `invoke test`
  - 116 tests passed.
- `invoke scenario-test`
  - 4 tests passed.
- `invoke harness-check`
- `invoke harness-test`
- `invoke lint`
- `invoke playwright-test`
  - UI harness passed against `http://127.0.0.1:8000`.
- `PATH=.venv-docs/bin:$PATH invoke docs`
- `invoke sensitive-check`
- `invoke package`
- `invoke ci`

## Findings

- No production code changes were required for the current test surface under NetBox `v4.6.0-beta2`.
- The upstream `netboxcommunity/netbox:v4.6.0-beta2` image tag was not available from Docker Hub, so the local base image was built from the upstream NetBox tag using `netbox-docker`.
- The default Buildx container builder attempted to resolve the missing remote image. Switching to the default local Docker builder allowed the project image to use the locally built beta2 base.
- The repo lacked a `.dockerignore`, which caused large local artifacts to be included in Docker builds. A narrow ignore file was added to reduce build context risk and runtime image churn.
- The GitHub Actions workflow now builds a missing NetBox base image locally before the project image build, which keeps beta compatibility branches testable before Docker Hub publishes a matching image tag.

## Rollback

- Switch back to the validated `development/.env` NetBox image tag from `main`.
- Stop the Docker stack with volume removal to avoid mixed 4.5/4.6 database state.

## Decision Log

- Rejected: Update `main` directly to NetBox 4.6 beta. Reason: 4.6 is not a production release and has no support commitment yet.
- Rejected: Treat 4.6 as enabling TurboBulk. Reason: TurboBulk remains a separate Cloud/Enterprise capability, not core NetBox.
