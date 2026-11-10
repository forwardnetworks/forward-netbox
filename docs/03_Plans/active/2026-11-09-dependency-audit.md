# Dependency audit for 3.0.15

## Goal

Land every open Dependabot bump and the forward-sdk 0.1.36 pin that is safe,
each as its own commit so any one can be dropped, and record what was left and
why. In particular, remove the paramiko advisory waiver if the lock can
genuinely move to a fixed paramiko.

## Constraints

- No bump lands that the lock cannot resolve, or that `poetry check --lock`,
  `pip-audit` or the release preflight reject.
- `development/constraints-upgrade-from.txt` is never edited here; the release
  step moves it. This supersedes the Dependabot diffs that edited it.
- Release-workflow actions stay SHA-pinned, with the version in a trailing
  comment where the existing line had one. The SHAs are Dependabot's, and each
  was checked against the upstream tag list.
- The Django suite and a live sync were not run for this change.

## Touched Surfaces

- `poetry.lock` - netmiko 4.7.0 -> 4.8.0, paramiko 4.0.0 -> 5.1.0 (ruamel.yaml
  drops out; netmiko 4.8.0 made it an optional extra), forward-sdk 0.1.36,
  cryptography 50.0.1 -> 50.0.2, python-dotenv 1.2.4, build 1.6.1,
  pymdown-extensions 12.3.
- `pyproject.toml` - forward-sdk `0.1.36`; pymdown-extensions ceiling `<13.0`.
- `constraints.txt` - forward-sdk `0.1.36`, cryptography `50.0.2`.
- `scripts/validate_sbom.py` - forward-sdk `0.1.36`.
- `scripts/check_release_preflight.py` - `ACCEPTED_DEPENDENCY_ADVISORIES`
  emptied (PYSEC-2026-2858 waiver removed).
- `.github/workflows/release.yml` - setup-python, setup-node,
  gh-action-pypi-publish, upload-artifact, download-artifact.

## Approach

### paramiko (PYSEC-2026-2858 / CVE-2026-44405, SHA-1 allowed in rsakey.py)

The old waiver said netmiko pins `paramiko<5.0`. That was true of netmiko 4.7.0
only: netmiko 4.8.0 requires `paramiko>=3.5.1` with no ceiling (a `par4` extra
caps at `<5`, not installed by default). netbox-validity 3.6.1 allows
`netmiko<5,>=4.0.0`, so 4.8.0 is reachable. scrapli_netconf's `paramiko<4`
sits in an unused optional extra; django-storages, dulwich and scp do not cap
paramiko. `poetry update netmiko paramiko --lock` resolves paramiko 5.1.0.

`constraints.txt` does not pin paramiko or netmiko, so the shipped image takes
whatever pip resolves, which is now paramiko 5.1.0 given netmiko 4.8.0. Running
`pip-audit -r constraints.txt` with no ignore list reports no vulnerabilities,
so the waiver is removed and `ACCEPTED_DEPENDENCY_ADVISORIES` is empty.

Reachability, for the record: forward_netbox never imports paramiko, netmiko or
scrapli. The vulnerable path (RSA key signing with SHA-1) is reachable only
through netbox-validity's own SSH device polling, which config backup (Forward
to a repository over dulwich) exists to avoid. Dulwich can use paramiko for SSH
remotes; that is the only route by which plugin-driven traffic could touch it.
Either way the fix is in.

Not verified: netbox-validity 3.6.1 against paramiko 5.1.0 / netmiko 4.8.0 at
runtime. paramiko 5 is a major version; Validity's SSH polling and the dulwich
SSH remote were not exercised.

### forward-sdk 0.1.36

Release notes: new entity_decorators, flow_trace, ServiceNow and CMDB routes;
URL fields read from Forward keep an empty path on pydantic 2.12+ (so
`str(url)` is `https://host`, not `https://host/`); ServiceNow
autoCreateImpact/Urgency values are now "1"/"2"/"3".

Audit of the plugin:

- Every SDK client attribute the plugin calls (`change_sets`, `checks`,
  `classic_devices`, `data_files`, `device_tags`, `network_maps`, `networks`,
  `nqe`, `snapshots`, `user_accounts` and the methods used on them) exists on
  0.1.36, and every `forward_sdk` import in the plugin and its tests imports.
- The only generated models with URL-typed fields are the Tenable, Rapid7,
  ServiceNow and `UrlObjectCollection` models. The plugin reads none of them.
- The plugin never persists or compares a URL returned by the SDK. Source URLs
  are the plugin's own strings: `models.py` (`forward_ui_url`),
  `utilities/network_maps.py` (`map_url`), `utilities/model_validation.py`
  (`source.url.rstrip("/")`) all strip trailing slashes already, so the new
  no-slash form is the form it already normalises to.
- No ServiceNow code exists in the plugin.

Result: pin bumped.

### Small bumps

cryptography 50.0.2 (constraints.txt and lock), python-dotenv 1.2.4, build
1.6.1, pymdown-extensions 12.3 (docs build; ceiling widened from `<12.0` to
`<13.0`), each in its own commit; CI actions one commit each (setup-python
7.1.0, setup-node 7.1.0, gh-action-pypi-publish 1.14.3, upload-artifact 7.0.2
plus download-artifact 8.0.2, bumped together).

### Left alone

- `requirements-release.txt` (hash-pinned `pip-compile` output on Python 3.14)
  still has build 1.5.0, cryptography 50.0.0 and python-dotenv 1.2.3. The
  Dependabot PRs never touched it, `pip-audit` is clean on it, and regenerating
  it is the release-toolchain step recorded in its header.
- `development/constraints-upgrade-from.txt`: moved by the release step.

## Validation

- `poetry check --lock`, `pip-audit -r constraints.txt` and
  `-r requirements-release.txt`: clean, no ignore list.
- `pre-commit run --files` on changed files, `invoke harness-check`,
  `invoke harness-test`.
- Not run: Django suite, `invoke ci`, live sync, docs build with
  pymdown-extensions 12.3, the release workflow with the new action SHAs.

## Rollback

Each dependency is a separate commit; drop or revert any one. Reverting the
paramiko commit also needs the preflight waiver restored.

## Decision Log

- Lifted the paramiko waiver instead of pinning paramiko in `constraints.txt`:
  the file only pins the plugin's own tested set, and the audit passes unpinned.
- Did not edit `requirements-release.txt` by hand; hashes come from
  `pip-compile`.
