# Say why the "none chosen" devices have no primary IP, in the bundle

## Goal

A support bundle showed 408 devices under "Has an address, none chosen" while the sync's own management-address summary said 585 devices share their address with another device. The finding sent the operator to the job log for a number the bundle already held. Put the reason in the finding and the bundle, so no one has to ask the customer.

## Constraints

- Counts and device ids only in the bundle; no names.
- No claim about what a sync will do; the text reports what the last management-address step said.

## Touched Surfaces

- `forward_netbox/utilities/primary_ip.py` (the step's log lines, the log reader).
- `forward_netbox/utilities/primary_ip_reasons.py`, `explained_findings.py`, `views.py` (bundle payload).
- `forward_netbox/tests/test_explained_findings.py`, `test_primary_ip.py`, `test_primary_ip_integration.py`; `docs/01_User_Guide/troubleshooting.md`.

## Approach

1. The unchosen-address reason carries the management-address step's reported counts as a cross-reference.
2. The "already on" advice stops pointing at the job log and says what a Mgmt_ device tag does.
3. The bundle lists a few device ids per reason, from one classification shared with the reasons.
4. The management-address step writes ids-only job-log lines (`[code]: pk` or `pk>holder pk`, chunked, behind a marker line written even for zero devices so an older log is not read as "recorded nothing") for each bare device and its reason, including a device that lost the address to its current primary owner. Parsing them back needs no migration. The bundle and the finding split each reason's devices by what the step recorded, count the bare devices it recorded nothing for, and report logged devices that are not bare here.

## Validation

- `invoke test-isolated` on `test_explained_findings`, `test_bundle_triage_diagnostics`, `test_primary_ip` and `test_primary_ip_integration` (real branch runs).
- `invoke harness-check`, `pre-commit run --all-files`, then the pre-push `invoke ci`.

## Rollback

Revert; the finding returns to the previous wording and classification.

## Decision Log

- Ids are job-log evidence from the last ingestion; the branch merge keeps object ids, and the bundle counts any logged id that is not a bare device of this sync rather than trusting it.
- The classifier is unchanged: broadening "shared" to any holder was reviewed out, because the sync-time step does not skip on sharing and uniqueness is per address record, so it could hide fixable devices.
- The summary counts are shown as a sync-wide cross-reference, not as a split of this reason's devices, because the step counts every bare device and the classifier counts by what NetBox holds.
- `filelock` moved past three advisories (release lock 3.31.1 to 4.2.4, which needs Python 3.11; `poetry.lock` to 4.1.0, the newest that still supports the 3.10 floor). The pre-push release preflight refused the push on them; `pip-audit` on the release lock reports none.
