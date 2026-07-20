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
| Latest release (currently `2.3.x`) | ✅ |
| Older releases | ❌ (upgrade to the latest release) |

## Scope and handling notes

- **Development service credentials.** The repository contains no shared
  PostgreSQL, Redis, Django, or API token pepper values. Development and CI
  create unique, ignored files under `development/secrets/` and mount them
  through Docker Compose secrets. The generator preserves existing files,
  rejects links and permissive modes, and never prints values. Do not copy this
  directory into build contexts, support bundles, or Git.
- **Forward credential at rest.** The Forward API password is **encrypted at rest**
  with Fernet, using a key derived from Django's `SECRET_KEY`, before it is stored
  in the `ForwardSource` parameters; it is also masked in every UI/API display and
  redacted from logs. A database dump therefore no longer contains a usable
  password. Two consequences: (1) protect `SECRET_KEY` like a credential (a leaked
  `SECRET_KEY` + DB dump can recover the password), and (2) **rotating `SECRET_KEY`
  makes stored Forward passwords undecryptable** — after a rotation, re-enter the
  password on each Forward source. Still keep the NetBox database and its backups
  access-controlled, and scope the Forward service account to least privilege.
- **Sync is an inventory-wide write trust boundary.** A Forward sync creates,
  updates, and deletes DCIM/IPAM objects across NetBox via the branch-merge apply
  path; these writes are **not gated by NetBox object-level permissions**. Treat
  the ability to create a Forward source or trigger a sync as equivalent to
  broad DCIM/IPAM write access, and restrict it (via NetBox permissions on the
  plugin's own models and operational process) to trusted operators. Destructive
  actions (device prune, IPAM delete-tagging) are additionally dry-run-by-default
  and refuse to act on an empty Forward scope.
- Do not include customer names, network identifiers, snapshot IDs, or credentials
  in reports committed to this repository; a pre-commit and CI content scanner
  (`scripts/check_sensitive_content.py`) blocks such identifiers.
