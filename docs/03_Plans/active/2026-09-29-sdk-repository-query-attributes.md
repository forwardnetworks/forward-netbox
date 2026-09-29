# Read the SDK's RepositoryQuery by its real attribute names

## Goal

`nqe.repo.queries()` returns the SDK's plain `RepositoryQuery` dataclass
(`query_id`, `path`, `commit_id`, `intent`, `repository`, `source`). The code
read `last_commit_id`, `last_commit` and `source_code`, which belong to the
wire model, so every committed-query lookup by path raised `AttributeError`
against a real Forward. Fix the reads, move to the SDK release that also
returns the requested path, and make the tests fail on this class of mistake.

## Constraints

- `forward-sdk` is pinned exactly, in `pyproject.toml`, `constraints.txt`,
  `poetry.lock`, the upgrade-from constraints and the SBOM validator, so all
  five move together.
- 0.1.16 answers a path-filtered lookup with an entry whose path is empty, so
  the match on the requested path fails before the attribute error is reached.
  0.1.20 fills the requested path in; the attribute fix is needed either way.
- 0.1.17 to 0.1.20 add no behaviour change to the NQE run, diff, snapshot,
  network, device or retry surfaces this plugin uses.
- `development/constraints-upgrade-from.txt` must equal `constraints.txt`
  except for the release under test, so it moves with the pin.

## Touched Surfaces

- `forward_netbox/utilities/forward_api_impl.py`: repository listing and
  committed-query path lookup.
- `forward_netbox/tests/test_forward_api.py`,
  `test_head_commit_from_listing.py`: real `RepositoryQuery` results in place
  of `SimpleNamespace`.
- `forward_netbox/tests/test_sdk_result_contract.py`: new.
- `pyproject.toml`, `constraints.txt`, `development/constraints-upgrade-from.txt`,
  `poetry.lock`, `scripts/validate_sbom.py`.

## Approach

Read `commit_id` and `source`. Build every repository-lookup result in the
tests as a real `RepositoryQuery`. Add a contract test that reads the real
dataclass's fields and asserts the wire-model names are absent.

## Validation

- Full `invoke ci`, including the artifact and upgrade legs.
- One manual live probe of the path-filtered lookup against a real Forward
  before the release that carries this change is tagged.

## Rollback

Revert the commit; the pin returns to 0.1.16 and the lookups return to their
previous, broken behaviour.

## Decision Log

- **Fixed with the bump, not before it.** The attribute fix stands alone, but
  the path lookup cannot succeed on 0.1.16, so shipping only the fix would
  turn one error into another.
- **Contract test reads the SDK type, not a hand-written list of names.** A
  rename in a future SDK release then fails in CI.
