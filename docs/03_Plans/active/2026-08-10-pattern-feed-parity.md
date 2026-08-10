# Run the release-time sensitive scan before the tag

## Goal

Stop a customer identifier in the tree from being discovered only after the tag
is immutable.

## Contract

- The scan the publish workflow runs is the scan the preflight runs, superset
  feed included.
- A checkout that cannot verify parity says so loudly and is recorded, rather
  than passing quietly.

## Constraints

- `FORWARD_SENSITIVE_PATTERNS` is a repository SECRET. It cannot be read by
  `gh`, it is not on the release host, and it must not be committed. Any design
  that requires the feed's contents locally is unavailable to a checkout that
  does not already have it.
- The local `.sensitive-patterns.local.txt` is gitignored and will always be a
  subset unless someone maintains it by hand.
- The preflight is the fast, sub-second stage of the gate; the scan must stay
  cheap enough to belong there.

## Touched Surfaces

- `scripts/check_release_preflight.py`
- `scripts/check_release_authorization.py` (accept the acknowledgement in
  exact-environment matching)
- `scripts/tests/test_release_preflight.py`

## Approach

The guard already supports the superset feed through `--require-env-patterns`;
the publish workflow passes it and nothing else ever did. The check is
unchanged - only its timing is. `check_sensitive_pattern_parity` runs the same
command the workflow runs, at preflight, where a refusal costs nothing.

Three outcomes, all explicit:

- feed present and the tree is clean: `verified`.
- feed present and the tree matches: hard failure, quoting the guard's own line.
- feed absent: hard failure, unless `FORWARD_NETBOX_PATTERN_PARITY_UNVERIFIED`
  is set, in which case the result string says `UNVERIFIED` and instructs that
  it be recorded in the release authorization.

The acknowledgement is added to the authorization checker's optional-environment
set so a release that used it still matches the recorded command exactly. This
mirrors `FORWARD_NETBOX_UPGRADE_FROM_VERSION`, which is already carried in the
evidence rather than hidden.

## Validation

`scripts/tests` covers all four branches: refused without the feed, allowed and
labelled with the acknowledgement, refused when the scan matches, and verified
when it does not - the last asserting that the command actually carries
`--require-env-patterns` and `--protected-history`, so the check cannot decay
into a weaker scan.

## Rollback

Revert. The scan returns to running only in the publish workflow, and a customer
identifier again costs a version number instead of a preflight second.

## Decision Log

- **Move the existing scan rather than replicate the feed.** Mirroring the
  secret into the repository, or committing a digest of it, both put customer
  names closer to the tree. The feed stays where it is; the scan comes earlier.
- **Fail closed when parity cannot be checked.** The alternative - passing
  silently when the feed is absent - is the exact behaviour that spent
  `v2.7.7`, since every local run was in that state and none of them said so.
- **Allow an explicit acknowledgement.** A hard requirement would make this
  host unable to cut any release at all, since the secret is not available
  here. A recorded gap is honest and reviewable; an unrecorded one is what we
  had.

## Open

- The acknowledgement path is the one this host must use, so on this machine the
  gap is narrowed but not closed: the publish gate remains the first place the
  superset feed is applied. Closing it fully means giving the release host the
  feed.
