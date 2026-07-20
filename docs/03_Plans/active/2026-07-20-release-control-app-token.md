# Release-control GitHub App token

## Goal

Make protected tag authorization executable and independently verifiable with
a short-lived GitHub App installation token instead of the under-privileged
automatic Actions token.

## Constraints

- Scope the App token to `forwardnetworks/forward-netbox` only.
- Grant no Contents write permission.
- Keep the environment-only deploy key as the only tag-writing identity.
- Require the independent `release-tag` environment approval before either a
  verification-only run or tag creation can access credentials.
- Keep the controller byte-identical in the bootstrap anchor and 2.6 release.

## Touched Surfaces

- `.github/workflows/trusted-tag.yml`
- `scripts/verify_release_provenance.py`
- release-control and provenance tests
- `docs/00_Project_Knowledge/release-playbook.md`

## Approach

1. Mint a one-hour installation token with the SHA-pinned
   `actions/create-github-app-token` action.
2. Request only Actions, Contents, Environments, pull-request, and status read
   plus Administration write, which GitHub requires to return ruleset bypass
   actors.
3. Run the existing live-control and provenance verifier with that token.
4. Require the exact App ID, App private key, and deploy-key secret names in the
   protected environment.
5. Preserve a verification-only dispatch that runs authorization without
   installing the deploy key or creating a tag.

## Validation

- `python scripts/check_sensitive_content.py --protected-history`
- `SKIP=sensitive-content-files pre-commit run --all-files`
- `invoke harness-check`
- `invoke harness-test`
- Exact-SHA GitHub CI and CodeQL
- Protected-environment verification-only workflow run against every live API
  endpoint before creating `security-bootstrap-2.6`
- Independent rereview of the exact bootstrap and candidate commits

## Rollback

Do not merge the bootstrap or create the anchor. Remove the App secrets and App
installation, then revert the controller changes on the unmerged branch.

## Decision Log

- Rejected the automatic `GITHUB_TOKEN`: it cannot read repository Actions
  administration or complete ruleset bypass data.
- Rejected a personal access token: it is long-lived, user-owned, and broader
  than the maintainer-ready repository-scoped installation token.
- Rejected giving the App Contents write: live verification is read-only, while
  tag creation already has a separately protected deploy-key identity.
