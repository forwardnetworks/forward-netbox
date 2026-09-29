# Port everything maint/2.9.x shipped after 2.9.6 onto main

## Goal

`main` (3.x, NetBox 4.7) has never released past 3.0.0 and is missing every
fix `maint/2.9.x` shipped as 2.9.7, 2.9.8, 2.9.9 and 2.9.10. A customer on
NetBox 4.6 asked whether 3.x carries the 2.9.x fixes; it does not. Bring all
of them across in one change, so 3.0.1 can ship them and a 4.6 customer
moving to 4.7 loses nothing.

## Constraints

- **One merge, not a replay.** 2.9.9 shipped a site test that called every
  live device out of scope, and 2.9.10 fixed it. Replaying the commits would
  reintroduce the regression before removing it. `main` already carries
  everything through 2.9.6, so the merge base is `v2.9.6`, not the older
  fork point git would pick. That gives one small conflict per file rather
  than re-conflicting 2.9.3-2.9.6.
- **`main`'s own work wins where the lanes overlap:** release tooling, the
  forward-sdk client (`ForwardClient` has no public methods), change control,
  the 4.7 runtime and validated-plugin set (ACI cannot install on 4.7), and
  its sync-job delete guard.
- **NetBox 4.7 differs from 4.6:**
  - a single device uniqueness constraint,
    `dcim_device_unique_name_site_tenant`, compares NULLs as equal;
  - `JobsMixin.delete()` batches job deletion ahead of `pre_delete`, which
    is why `ForwardIngestion` needs the same guard `ForwardSync` already has.

## Touched Surfaces

- Everything 2.9.7-2.9.10 changed under `forward_netbox/`, with `main`'s
  version kept for the overlapping release, runtime and client code.
- Merge fix-ups:
  - `utilities/validated_runtime.py`: the subset rule tolerates a
    distribution this runtime has not validated (ACI).
  - `utilities/forward_api_impl.py` / `forward_api.py`:
    `get_configured_device_tags` and `get_classic_device_collection` as
    forward-sdk free functions; their callers in `scope_reconciliation.py`.
  - `utilities/constraint_diagnosis.py`: a key containing NULL counts as a
    value when the violated constraint declares `nulls_distinct=False`.
  - `models.py` / `signals.py`: `ForwardIngestion.delete()` and the extracted
    ingestion guard.
- Tests adapted to `main`'s free functions (`run_nqe_query`, the
  configuration reads) rather than client methods.

## Approach

`git merge-tree --merge-base=v2.9.6 main maint/2.9.x`, then per file:

1. Release, version and packaging plumbing takes `main`'s side (README,
   changelog, tasks, scripts, Dockerfile, pyproject, version pins).
2. Code takes the 2.9.10 side, adapted to the forward-sdk and 4.7.
3. Migrations were already reconciled by the lineage change; the 2.9.x
   `0057` is `main`'s.
4. Flake8 caught a duplicate `cancel_enqueued_jobs_on_sync_delete` receiver
   auto-merged from the 2.9.x side; `main`'s stays.

## Validation

- Every 2.9.10 entry point is present in the merged tree, and the 2.9.9
  site-slug code is absent.
- Targeted run of the modules most exposed to the merge: migration lineage,
  the fast paths and validated runtime, scope reconciliation and the
  site-relabel repair, config backup, routing catalogue, constraint
  diagnosis, redaction and bundle diagnostics, and the COPY/SQL and fast
  baseline engines. 352 tests.
- Full `invoke ci` before push, including the upgrade from 2.9.10 on
  NetBox 4.6 onto this tree on NetBox 4.7.

## Rollback

No migration beyond the lineage change already on `main`. Reverting this
change restores `main` to 3.0.0 behaviour.

## Decision Log

- **Not held for a per-item review of 2.9.7-2.9.9.** Each of those changes
  was gated and reviewed on its own lane; what needs review here is how the
  lanes meet, which the conflict list is.
- **Tests, not just code, adapted.** The 2.9.x tests mock `ForwardClient`
  methods that no longer exist here. They now delegate the free functions to
  the same mock, as `main`'s existing tests do.
