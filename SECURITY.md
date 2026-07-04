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
| Latest `2.2.x` | ✅ |
| Older releases | ❌ (upgrade to the latest release) |

## Scope and handling notes

- The plugin stores Forward API credentials in the NetBox database and performs
  inventory-wide writes to DCIM/IPAM during a sync. Deployments should protect the
  NetBox database at rest and restrict who can create/trigger Forward syncs.
- Do not include customer names, network identifiers, snapshot IDs, or credentials
  in reports committed to this repository; a pre-commit and CI content scanner
  (`scripts/check_sensitive_content.py`) blocks such identifiers.
