# Security Policy

## Reporting a vulnerability

Please report security vulnerabilities **privately** — do not open a public issue,
pull request, or discussion for a suspected vulnerability.

Preferred channel:

- **GitHub Private Vulnerability Reporting** — on this repository, go to the
  **Security** tab → **Report a vulnerability**. This opens a private advisory
  visible only to the maintainers.

Alternative channel:

- Email **security@forwardnetworks.com** with the details below. Use the subject
  line `forward-netbox security report`.

Please include, where possible:

- A description of the issue and its impact.
- The plugin version (`pip show forward-netbox`) and the NetBox version.
- Steps to reproduce, a proof of concept, or affected code paths.
- Any suggested remediation.

We aim to acknowledge a report within **3 business days** and to provide a
remediation plan or assessment within **10 business days**. Please give us a
reasonable opportunity to remediate before any public disclosure; we will
coordinate a disclosure timeline with you.

## Supported versions

Security fixes are provided for the **latest released minor version** only. This
plugin pins hard minimum NetBox and `netbox-branching` versions per release (see
the compatibility matrix in the README); fixes target the currently supported
platform baseline.

| Version | Supported |
| --- | --- |
| Latest release (currently `2.6.x`) | Supported |
| Older releases | Upgrade to the latest release |

## Scope and handling notes

- **Forward credential at rest.** The Forward API password is **encrypted at rest**
  with Fernet, using a key derived from Django's `SECRET_KEY`, before it is stored
  in the `ForwardSource` parameters; it is also masked in every UI/API display and
  redacted from logs. A database dump therefore no longer contains a usable
  password. Two consequences: (1) protect `SECRET_KEY` like a credential (a leaked
  `SECRET_KEY` + DB dump can recover the password), and (2) **rotating `SECRET_KEY`
  makes stored Forward passwords undecryptable** — after a rotation, re-enter the
  password on each Forward source. Runtime rejects plaintext and undecryptable
  credential values before any Forward request. Still keep the NetBox database
  and its backups access-controlled, and scope the Forward service account to
  least privilege.
- **Sync is an inventory-wide write trust boundary.** A Forward sync creates,
  updates, and deletes DCIM/IPAM objects across NetBox via the branch-merge apply
  path; these writes are **not gated by NetBox object-level permissions**. Treat
  the ability to create a Forward source or trigger a sync as equivalent to
  broad DCIM/IPAM write access, and restrict it (via NetBox permissions on the
  plugin's own models and operational process) to trusted operators. Destructive
  actions (device prune, IPAM delete-tagging) are additionally dry-run-by-default
  and refuse to act on an empty Forward scope.
- Do not include customer names, organization, network, snapshot, or query
  identifiers, or credentials in reports committed to this repository. The
  fail-closed pre-commit and CI scanner (`scripts/check_sensitive_content.py`)
  blocks known identifier formats. It scans current files, changed historical
  blobs, commit messages, and annotated tag messages. Binary or non-UTF-8 files
  are rejected unless their exact path and SHA-256 are recorded in the reviewed
  `.sensitive-binary-allowlist`. Maintainers keep customer-specific literals and
  regular expressions in the gitignored `.sensitive-patterns.local.txt`; CI
  supplies the equivalent secret-backed list through
  `FORWARD_SENSITIVE_PATTERNS`. Release validation requires that feed to be
  nonempty and verifies `.sensitive-history-baseline` against the external
  `FORWARD_SENSITIVE_HISTORY_BASELINE` repository variable. Historical binary
  exceptions are accepted only from the external
  `FORWARD_SENSITIVE_BINARY_HISTORY_ALLOWLIST` secret as an exact
  commit/path/SHA-256 tuple. A base-branch `pull_request_target` workflow scans
  fork PR objects without checking out or executing candidate code, then posts
  an authenticated status on the exact candidate SHA. Release publication
  requires two separately check-gated squash merges: the production tree and an
  evidence-only child. The release workflow verifies both GitHub-signed main
  commits, exact merged-PR associations, authenticated trusted-scan statuses,
  and exact successful CI/CodeQL workflow identities
  before the protected PyPI environment can publish.
  `--all-history` remains available for a forensic audit without making
  published-history rewrites a release action.
