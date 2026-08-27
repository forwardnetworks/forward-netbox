# Make poetry.lock record the runtime we actually validate

## Goal

Bring `poetry.lock` back into agreement with what the release gate installs,
and say plainly what keeps it there - because nothing currently does.

## Why

`poetry.lock` recorded `netboxlabs-netbox-branching==1.1.1` while
`constraints.txt` said `1.1.2`, and had done since 2.6.7. Chasing that turned up
the larger half: the lockfile was missing **37 packages entirely** -
`netbox-validity` and its whole transitive subtree (`netmiko`, `paramiko`,
`scrapli`, `scp`, `ntc-templates`, `boto3`, ...) - never recorded when Validity
was added as an optional dependency in 2.9.0.

That subtree is not academic. It is exactly the dependency chain the 2.9.0
release preflight traced when paramiko's `PYSEC-2026-2858` blocked the gate: the
lockfile could not have shown anyone where paramiko came from, because it did
not know paramiko existed.

The drift was invisible because **nothing consumes this file**. No Dockerfile,
no workflow, no script references it - verified by grep. The images install with
`uv pip install --constraint constraints.txt`, so `constraints.txt` is the real
authority and the lockfile has been decorative.

## Constraints

- Regenerate, never hand-edit. A hand-patched lockfile is worse than a stale
  one because it looks resolved.
- Change no pin that ships. `constraints.txt` remains the authority.
- The residual divergence must be named, not smoothed over.

## Touched Surfaces

- `poetry.lock` only.

## Approach

`poetry update --lock netboxlabs-netbox-branching`, which moves branching to
1.1.3 and backfills the missing subtree in one resolution. 1,346 inserted
lines, of which exactly one is a version CHANGE - every other entry is a
package that was absent.

## Validation

Compared the regenerated lockfile against every pin in `constraints.txt`:

    cryptography     constraints 50.0.0   lock 50.0.1   MISMATCH
    httpx            0.28.1               0.28.1        ok
    netbox-cisco-aci 0.4.0                0.4.0         ok
    netbox-dlm       0.9.1                0.9.1         ok
    netbox-branching 1.1.3                1.1.3         ok
    netbox-peering-manager 0.3.0          0.3.0         ok
    netbox-routing   0.4.3                0.4.3         ok
    netbox-validity  3.5.2                3.5.2         ok
    pyzipper         0.4.0                0.4.0         ok

Eight of nine agree. `cryptography` does not, and it is left disagreeing: it is
not declared in `pyproject.toml` at all - it arrives transitively with NetBox -
so poetry resolves the newest patch the graph allows, and forcing 50.0.0 would
mean adding a direct dependency this package does not have just to satisfy a
file nothing reads.

Both versions are advisory-clean (`pip-audit`, checked). What ships is 50.0.0,
because the images install under `--constraint`.

## Rollback

Revert. The lockfile returns to describing a runtime from 2.6.7.

## Decision Log

- **Separated from the branching bump** (#300). One version change against
  1,346 lines of backfill is not a reviewable "chore: bump", and the backfill is
  a pre-existing gap that has nothing to do with branching.
- **Left the cryptography divergence in place and named it.** The alternative -
  declaring a direct dependency on cryptography purely to make an unread file
  self-consistent - buys a worse lie.
- **Did not add a lock-vs-constraints check.** It would need an exception for
  `cryptography` on the day it was written, and a check whose first act is to
  exempt its only finding teaches nothing. The real question is below.

## Open

- **Should this file exist?** Nothing reads it, nothing keeps it current, and it
  silently described a runtime three releases stale. Either something should
  consume it (a `poetry check --lock` in the harness) or it should go. Leaving
  it accurate-today and unenforced only resets the clock on the same drift.
- `cryptography` will drift again on the next resolution, for the same reason.
