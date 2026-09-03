# Forward change control: a concept for a second plugin

Status: concept. Nothing here is committed to a release. This is the
decision-complete argument for a sibling plugin, `forward-change-control`,
so that the choice to build it or not can be made on the evidence rather
than on enthusiasm.

## Goal

Give a network change a lifecycle in NetBox whose gates are answered by
Forward, not by a person asserting they were satisfied. A change names the
devices it touches and the properties that must hold afterwards; the
plugin pins a Forward snapshot before it, pins another after it, runs the
same NQE criteria against both, and merges the NetBox branch describing
the change **only** when the after-snapshot says the criteria hold.

The one-sentence difference from every generic change-control tool: an
approval opens the merge, but it does not complete the change. Evidence
from a real post-change snapshot does.

That claim rests on a snapshot and an NQE query, and nothing else — the
two capabilities every Forward licence tier has and that forward-netbox
has been exercising in production for a year. Forward's predict workflow
is **not part of the load-bearing path**: it is not live, it is
licence-gated, and it is limited in scope today, so it is designed for as
an optional, capability-gated advisory step with a stubbed
implementation. The product is whole without it. See sections 1a and 1b,
and the Decision Log.

## What this is NOT

Stated first, because every section below is shorter for it.

- **Not a ticketing system.** No queues, no assignment, no SLA, no
  priority-as-workflow. A change carries a `ref` string pointing at
  whatever ServiceNow/Jira record the customer already has, and that is
  the whole of the integration.
- **Not a config pusher, orchestrator or runbook engine.** The plugin
  never opens a session to a device and must never claim to have. The
  apply is performed by whatever the customer already uses, and the
  plugin records an attestation that it happened.
- **Not a reimplementation of branching.** No diff engine, no merge
  engine, no conflict resolution, no revert. `netbox_branching` owns all
  of it. The only branching code this plugin writes is one
  `Branch.register_preaction_check(..., 'merge')` callable and a
  `OneToOneField`.
- **Not a fork or a re-skin of `netbox-change-control`.** That plugin is
  the shape reference and nothing more. It declares `max_version =
  '4.6.99'`, so on this runtime it cannot be installed at all, which
  removes the option of building on top of it even if we wanted to.
- **Not a second Forward client.** Credentials, retry, pagination, rate
  limiting, async NQE polling and API-usage accounting all live in
  `forward_netbox.utilities.forward_api_impl` and are consumed, never
  copied.
- **Not a Forward writer**, with one deliberate, opt-in exception:
  publishing a criterion query into the org NQE library so it can be
  diffed. See the Decision Log.
- **Not a drift detector.** forward-netbox already answers "does NetBox
  match Forward". This answers "did the change do what it said, and
  nothing else".
- **Not authoritative inventory.** Like `ForwardDeviceAnalysis`, its rows
  are an operator-facing read model, not a sync target.
- **Not a predict-based tool.** It does not require Forward's predict
  workflow, does not degrade without it, and must never be described as
  needing it. Predict is an optional advisory panel behind a stub.

## Constraints

- **NetBox 4.7 only.** `min_version = "4.7.0"`, `max_version = "4.7.99"`.
  There is no 4.6 lane for this plugin: it does not exist yet, so it has
  no installed base to straddle for.
- **netbox-branching `1.2.0b1`.** 1.2.x requires 4.7 and 1.1.x cannot run
  on it, so the pin follows forward-netbox 3.0 exactly. 1.2.0 final is
  not released and the beta's own notes state that no upgrade path to a
  later release is provided. Design accordingly: see Risks.
- **forward-netbox 3.0 is a hard dependency**, not an optional one. The
  3.0 line is in progress on `feat/netbox-4.7-runtime`. This plugin is
  meaningless without it and must refuse to load rather than degrade.
- **The five optional plugins are unavailable.** `netbox-dlm`,
  `netbox-cisco-aci`, `netbox-peering-manager`, `netbox-routing` and
  `netbox-validity` all declare a `max_version` in the 4.6 series, so
  NetBox refuses to start with them on 4.7. No criterion, model or view
  here may depend on any of them. In particular the config-backup ->
  Validity golden-config path has no consumer on this runtime: the
  backup half still works, the checking half does not exist.
- **`netbox_branching.exempt_models` must list
  `forward_change_control.*`.** Our models are change-logged, so
  branching would track them, and a change record created while a branch
  was active would be written into that branch's schema and become
  invisible from main. This is a required installation step, not a
  tuning knob. `netbox-change-control` records hitting exactly this.
- **`VALIDATED_PLUGIN_APPS` must gain `forward_change_control` in the
  same forward-netbox release.** That set is an exact match that fails
  **closed and silently**: an app in `PLUGINS` but not in the set
  disables COPY/SQL, the set-based merge and the fast baseline in
  forward-netbox with no error, turning a first sync from minutes into
  hours. Installing this plugin without that entry is a ten-times
  slowdown nobody gets an error for.
- **Read-only toward Forward stays the default.** `architecture-flow.md`
  states the property as a contract: the plugin reads from Forward and
  writes only to the local NetBox database. Any Forward write introduced
  here is opt-in, separately permissioned, and named in the docs.
- **Persisted diagnostics carry schema identifiers, never customer
  data.** Evidence rows follow the existing `record_issue` /
  `diagnostic_shape` policy: counts, keys and query identity persist;
  row content does not.
- **Forward's predict workflow may be assumed absent.** It is not live,
  it is licence-gated, and it is limited in scope where it does exist. No
  state transition, verdict, model field or gate may require it. It is
  built as a stub with a real interface (section 1b) so that its arrival
  is additive.
- **Forward licence tiers cannot be pre-flighted.** There is no endpoint
  exposing the tier; `license_tier.py` says so explicitly and warns
  against adding a gate that assumes otherwise. Every capability question
  is answered by asking Forward and reading the refusal, and a refusal is
  reported as "not licensed", never as an error.
- **NQE call volume is a live customer-facing concern.** Forward
  engineering has already objected to unnecessary NQE runs. Every path
  here is accounted through `api_usage_summary()` and the per-change
  execution count is shown on the form before the change is saved.

### NetBox 4.7 specifics that shape the design

| 4.7 change | Consequence here |
|---|---|
| django-mptt replaced by PostgreSQL `ltree`; `lft`/`rght`/`tree_id`/`level` dropped, `level` unusable in `filter()`/`order_by()` | No model proposed here is hierarchical, and none should become one casually. If one ever must, it uses `NestedLtreeGroupModel`, never the deprecated `NestedGroupModel`. |
| Denormalized field maintenance moved into database triggers; `registry['denormalized_fields']` removed | Do not denormalize through the registry. The one denormalized column proposed (`branch_name`) is maintained in `save()`, exactly as the reference plugin does it. |
| `CustomField.objects.get_for_model()` returns a **list**, not a queryset | The change form and detail view must not call `.exists()` or `.order_by()` on it. |
| `django_pglocks` gone; `django_pg_utils.advisory_lock` | The verify job takes an advisory lock keyed on the change pk so two verifies cannot interleave on one change. |
| `extras.signals` has an import-time `apps.get_model()` side effect | Never import a NetBox signal module at module scope from anything `models.py` pulls in; import inside the function. `AppRegistryNotReady` otherwise. |
| Selection custom-field values return `{"value", "label"}` objects in REST/GraphQL | Serializer tests must assert the object form, not the scalar. |
| New `BULK_UPDATE_CHUNK_SIZE` | Evidence rows are written per criterion per phase and can reach the low thousands on a large change. Honour the setting rather than one unbounded `bulk_update`. |
| Background processing for REST API requests | `POST .../verify/` returns a job rather than blocking. This is the native mechanism now; do not hand-roll one. |
| Per-object errors for bulk operations | Bulk criterion edits report which criterion failed to bind, not just that one did. |
| Snapshot-aware event rule conditions | A `VERIFIED_HOLD` event rule can condition on the pre/post state pair, which is how a customer wires a HOLD into their own paging. |

## Touched Surfaces

Inside `forward_netbox/`, as a `change_control` subpackage so the feature is
legible as one thing and its imports read as internal:

- `forward_netbox/change_control/__init__.py`
- `forward_netbox/change_control/choices.py` - `ForwardChangeStateChoices`,
  `ForwardChangeVerdictChoices`, `ForwardCriterionFamilyChoices`,
  `ForwardCriterionExpectationChoices`, `ForwardEvidencePhaseChoices`.
- `forward_netbox/change_control/state_machine.py` - the transitions and their
  entry/exit evidence. Pure functions over the database, no request object, so
  they test in under a second.
- `forward_netbox/change_control/criteria.py` - binding, execution,
  expectation evaluation.
- `forward_netbox/change_control/evidence.py` - recording and the
  regression-flip comparison.
- `forward_netbox/change_control/gates.py` - the collect gate and the verify
  gate.
- `forward_netbox/change_control/predict.py` - the stub and its
  `PredictOutcome`.

Existing files that grow:

- `forward_netbox/models.py` - the six models, beside the ones they reference.
- `forward_netbox/migrations/` - one migration.
- `forward_netbox/views.py`, `tables.py`, `forms.py`, `filtersets.py`,
  `navigation.py`, `templates/` - the UI, following the surrounding idiom.
- `forward_netbox/tests/` - one test module per change_control module.

## Approach

### 1. The workflow state machine

Eleven states, **ten of which are mandatory**. The vocabulary is
deliberately the one the Skyforge change flow already uses in the field —
`STAGED`, `PREDICTED`, `APPROVED`, `APPLIED`, `COLLECTED`,
`VERIFIED_PROCEED`, `VERIFIED_HOLD`, `CLOSED` — because it is a working
reference, and because a HOLD that is a *valid result* rather than an
error is the property most home-grown change gates get wrong.

`PREDICTED` is the optional one. It is skippable by design and by
default: `STAGED -> APPROVED` is a first-class transition, not a
degradation. Section 1a states exactly what changes when it is skipped.

| State | Entry evidence (what must be true to arrive) | Exit evidence (what must be produced to leave) | Forward capability behind it |
|---|---|---|---|
| `DRAFT` | Created by a user. | Nothing. The author's to hold, exactly as `netbox-change-control` treats draft. | none |
| `SCOPED` | Every device in scope resolves to a `ForwardDeviceIdentity` row for the chosen `ForwardSource`, so every device is one Forward actually knows. At least one blocking acceptance criterion. | Every criterion **binds**: its query path resolves to a concrete `query_id` at a concrete `commit_id`. A criterion that cannot bind refuses the whole transition, with the binder's errors. | `resolve_nqe_query_reference()`, `get_nqe_query_history()`, `get_committed_nqe_query()` |
| `BASELINED` | A concrete snapshot id, resolved from the selector and then **pinned** as a literal id. | One `ForwardChangeEvidence` row per criterion at `phase=before`, pass or fail. A criterion already failing here is recorded as failing, not skipped. | `resolve_snapshot_id()`, `get_snapshots()`, `run_nqe_query()`, `get_snapshot_metrics()` |
| `STAGED` | A `netbox_branching.Branch` exists and holds `ChangeDiff` rows describing the intended post-change NetBox state. | `change_explainability_summary(branch)` returns a non-empty per-model, per-field, per-action summary. **Exits to either `PREDICTED` or `APPROVED`.** | none (branching) |
| `PREDICTED` *(optional)* | The staged rows are classified against live NetBox without writing, and — where the capability exists — Forward is asked to predict the change. | Two independent advisory panels: **NetBox impact**, `{creates, updates, unchanged, rejected}` per model or `None` for a model that cannot be compared; and **Forward predict**, a `PredictOutcome` that is `unavailable`, `unsupported` or `answered`. A `None` renders "not measured", never zero. Nothing here is written to `ForwardChangeEvidence`. | `compare_model_rows()` via `PreviewRunner`; `predict_change()` (stubbed, see 1b) |
| `APPROVED` | The policy's rules are satisfied by non-stale reviews. | An approval bound to **both** `branch_last_change_time` and `before_snapshot_id`. Either moving invalidates it. The approval records **which evidence was on the page when it was given** (see 1a). | none (people) |
| `APPLIED` | A human or an external system attests the network was changed. This is **not computed**, and the design says so on the page. | `applied_at`, `applied_by`, `applied_ref`. | none |
| `COLLECTED` | A snapshot exists that is not the baseline and whose per-device collection times all post-date `applied_at` for the devices in scope. The plugin **waits** for one; it has no way to request a collection. | `after_snapshot_id` pinned. Optionally `trigger_snapshot_reachability` completed, when any criterion is reachability-dependent. | `get_snapshots()`, `get_snapshot_metrics()`, `trigger_snapshot_reachability()` |
| `VERIFIED_PROCEED` | Device-set completeness holds; every blocking acceptance criterion passes at `after_snapshot_id` **at the same `commit_id` used at baseline**; every state-preservation family shows no diff outside the declared blast radius. | The verdict, the per-criterion evidence rows at `phase=after`, the regression-flip list, and both snapshot ids. | `run_nqe_query()`, `run_nqe_diff(query_id, before, after)` |
| `VERIFIED_HOLD` | Any of the above fails. **A valid result, not an error.** | The same evidence, with the failing criteria named. A HOLD is fixed and re-verified; it is never closed. | same |
| `CLOSED` | Reachable **only** from `VERIFIED_PROCEED`, **and** the merge must complete without retryable failures. | The branch merges here, through branching's own merge. The change document is frozen. | none |
| `ABANDONED` | Any open state, one-way. | The branch is discarded, not merged. | none |

Two properties of this machine are the whole point:

**The branch merges at CLOSE, not at APPROVE.** Every generic change
control tool — `netbox-change-control` included — merges when the humans
say yes. Here the humans opening the gate is a necessary condition and
the *after-snapshot* is the sufficient one. The NetBox model becomes
truth once Forward has seen the network actually be that way. That single
reordering is what a Forward-backed change control buys, and it is not
available to a plugin without a network model to consult.

**Baseline before staged, deliberately.** A criterion must be evaluated
against the network *before* anyone edits NetBox, or a criterion that was
already failing gets counted as damage the change caused. The
before-phase evidence exists to make the regression-flip computable:
`fail -> pass` is a fix, `pass -> fail` is a regression, `fail -> fail`
is pre-existing and must not block, `pass -> pass` is preserved state.
Only `pass -> fail` and `pass -> pass`-turned-`fail` block.

### 1a. The machine with predict entirely absent

Predict is not live at Forward, it is license-gated when it does ship, and
what exists today is limited in scope. The design therefore assumes it is
absent and treats its arrival as additive. There is no hole to infer:
`STAGED -> APPROVED` is a documented transition with its own tests.

The verdict is unaffected. It is worth saying twice, because it is the
claim the product rests on: **the load-bearing evidence is the
post-change snapshot verification, and that runs on every license tier
Forward sells.** A customer who will never have predict loses an advisory
pre-flight panel and loses nothing else. Nothing in `VERIFIED_PROCEED`,
`VERIFIED_HOLD` or `CLOSED` reads a prediction of any kind.

That separation is structural rather than promised. Predictions are
written to advisory columns on `ForwardChange` (`predict_status`,
`predict_reason`, `predict_pre_verdict`, `netbox_impact`) and **never**
to `ForwardChangeEvidence`. The verify gate reads only
`ForwardChangeEvidence`. A future contributor cannot make the verdict
depend on a prediction without first moving it into the evidence table,
which is a visible, reviewable, migration-shaped act.

What the approver sees, and what their approval therefore means:

| Path taken | Evidence on the approval page | What the approval asserts |
|---|---|---|
| `STAGED -> APPROVED` (predict skipped, or unavailable) | The baseline evidence rows (every criterion's before-state, pass or fail), the bound criteria with their pinned `commit_id`s, the device scope, and the `ChangeDiff` explainability summary. | "I have read what this change asserts and what the network looks like now, and I accept the risk of applying it." This is a **weaker** claim than the one below, and the page says so in those terms. |
| `STAGED -> PREDICTED -> APPROVED`, predict `unavailable` or `unsupported` | The above, plus the NetBox impact counts, plus a panel naming why Forward could not answer. | The same claim. The extra panel narrows the NetBox-side blast radius; it says nothing about the network. The page must not let "we asked and got no answer" read as "we asked and it was fine". |
| `STAGED -> PREDICTED -> APPROVED`, predict `answered` | The above, plus a Forward pre-verdict against a predicted snapshot. | "...and a model of the network says it should hold." Still advisory: a pre-verdict is a prediction, and the change document must not cite it as evidence of network safety. |

Three consequences that must be built, not assumed:

- The change document's provenance block records **which of these three
  paths was taken**, so a reader six months later knows what the approver
  actually had in front of them.
- A `PREDICTED` state whose outcome is `unavailable` is *not* a failed
  predict. It advances exactly as a skip does.
- Approval staleness is unchanged. It is anchored to
  `branch_last_change_time` and `before_snapshot_id`; a prediction is not
  an anchor, because re-running one changes nothing an approver relied on.

### 1b. The predict capability: a stub with a real interface

Not a TODO. A named function with a settled signature and return type,
whose body today is one line. Filling it in must not reshape the state
machine, any model, or any migration.

```python
def predict_change(
    *,
    source,          # ForwardSource: supplies the client and the network id
    snapshot_id,     # the pinned baseline snapshot to predict from
    devices,         # the change scope, as resolved Forward device keys
    proposal,        # the config-level intent, or None when there is none
    criteria,        # the bound criteria to score on the predicted snapshot
) -> PredictOutcome: ...


@dataclass(frozen=True)
class PredictOutcome:
    status: str                 # "unavailable" | "unsupported" | "answered"
    reason: str                 # operator-facing sentence; "" only when answered
    predicted_snapshot_id: str = ""
    criteria: tuple[PredictedCriterion, ...] = ()   # criterion pk, passed, row_count
    pre_verdict: str = ""       # "PROCEED" | "HOLD"; "" unless answered
```

The stub is `return PredictOutcome(status="unavailable", reason=NOT_LIVE)`.

The three statuses exist because the capability has three distinct
absences, and collapsing them is how a customer ends up believing a
question was answered:

| Status | When | What the UI shows |
|---|---|---|
| `unavailable` | The capability is not live, or this organization's licence does not include it, or the source is not configured for it. | An **informational** panel: "Forward predict: not available." plus the reason sentence. Not an error, not a warning, no red, no retry button. Health status `info`, matching how `health_checks.py` already renders "Data-file maps" when there is simply nothing to say. |
| `unsupported` | The capability is live and licensed, but this particular change is outside what predict covers today — it is limited in scope, and will stay limited for some time. | An informational panel: "Forward predict: cannot answer this change." plus the reason naming what it could not model. Explicitly **not** a HOLD and explicitly not a pass. |
| `answered` | A real predicted snapshot and a real pre-verdict. | The pre-verdict, the per-criterion predicted results, and the predicted snapshot id — every one of them labelled advisory, in the same way `change_estimate_kind = "workload_upper_bound"` labels an estimate today. |

**Licence detection follows the existing precedent exactly, including its
central limitation.** `utilities/license_tier.py` records that the tier is
**not readable from the Forward API** — there is no endpoint exposing it,
so the plugin cannot pre-flight against a licence and must not pretend
to. Detection is after-the-fact recognition of a denial: Forward answers
`is not permitted for this organization's license tier`, the client
matches it, records `_record_api_usage("license_tier_denials")`, and
raises `ForwardLicenseTierError` — a distinct exception documented as "a
capability limit, not a fault: retrying, re-authenticating or re-running
the sync cannot clear it".

`predict_change` inherits all of that:

- It catches `ForwardLicenseTierError` and converts it to
  `PredictOutcome(status="unavailable", reason=license_tier_denial_message(...))`.
  A licence limit is **never** an exception that reaches the operator as
  a failure; it is a status with a sentence.
- It adds exactly one counter, `predict_calls`, to the existing
  `api_usage_summary()` payload alongside `nqe_query_calls` and
  `nqe_diff_calls`, so predict volume is budgeted with everything else.
  `license_tier_denials` already exists and needs nothing.
- It reuses the facet vocabulary already in `license_tier.py` — NETWORK
  (`N`, `NP`, `NS`, `NSP`) and SECURITY (`S`, `NS`, `NSP`) — rather than
  inventing a second one.
- It does **not** build a predict-to-facet table. `license_tier.py`
  refuses that for the bundled maps on the grounds that a wrong table
  sends operators to buy the wrong thing, and predict is a capability
  nobody here has seen ship. Report the facet Forward's own denial names,
  and nothing more.

Because the tier cannot be pre-flighted, the first call on a source is
also the discovery. Cache the outcome per source for the life of the
process so an unlicensed org is not asked once per change, and never
persist it — a licence can change, and a stale "not licensed" that
outlives the purchase is its own support case.

### 2. The verify gate, in order

Ordered so the cheapest refusal comes first, following the release-gate
ordering rule (`2026-07-28-gate-ordering-preflight.md`).

**No step below reads a prediction of any kind** — not the Forward
pre-verdict, not the NetBox impact counts. Every input is either a
pinned snapshot id, an evidence row, or a device fact. This is what makes
the product whole on a licence tier that will never include predict.

1. **Snapshot distinctness.** `after_snapshot_id != before_snapshot_id`.
   Verifying against the baseline is the trap that makes a change gate
   report success forever, and it costs one string comparison to refuse.
2. **Device-set completeness.** Every device in scope is present in the
   after-snapshot with collection `result == completed`. A device that
   failed collection is *absent from the model*, not flagged, so a check
   over a snapshot missing the one broken device returns zero violations
   and reads as a pass. This is the memory entry
   `disabled-in-forward-deletes-from-netbox` restated as a gate: absence
   is not evidence. HOLD if any scoped device is missing.
3. **Collection ordering.** Per device, collection time > `applied_at`.
   Where Forward does not expose a per-device collection time for a
   device, the gate reports that device as `unproven`, never as passing.
4. **Acceptance criteria.** Each blocking criterion runs at
   `after_snapshot_id` at the baseline's `commit_id`. Pinning the commit
   is not optional: a criterion whose text moved between baseline and
   verify is a different assertion, and comparing the two answers is
   meaningless.
5. **State preservation.** For each state-preservation family,
   `run_nqe_diff(query_id, before_snapshot_id, after_snapshot_id)`
   returns the rows Forward itself says moved between the two snapshots.
   Rows whose key is inside the declared blast radius are expected; rows
   outside it are the "and nothing else broke" half of the verdict.
6. **Verdict.** PROCEED only if 1-5 all hold. Anything else is HOLD, with
   the failing item named by its criterion, not by a generic message.

And one gate after the verdict, at CLOSE: **a partial merge is not a
close.** forward-netbox already separates a merge's failures into
retryable (`failed_change_count`) and unsatisfiable destination-rule
rejections (`skipped_change_count`), and raises `ForwardPartialMergeError`
rather than completing when retryable failures remain. A change whose
merge lands partially stays at `VERIFIED_PROCEED` with the branch
retryable; it does not become `CLOSED`. The operator escape hatch that
exists there — `accept_reported_failures` — is reachable only by an
explicit human action and must remain so here.

### 3. Data model

Six new models. Every one of them exists because nothing in
forward-netbox holds that fact.

| Model | Base | Purpose |
|---|---|---|
| `ForwardChange` | `PrimaryModel` | The change. `source` FK to `ForwardSource`; `branch` `OneToOneField` to `netbox_branching.Branch` (`SET_NULL`) plus a denormalized `branch_name`; `state`; `verdict`; `ref`; `title`; `requester`; window bounds; `before_snapshot_id`; `after_snapshot_id`; `applied_at`/`applied_by`/`applied_ref`. Plus four **advisory** columns quarantined from the gate: `predict_status`, `predict_reason`, `predict_pre_verdict`, `netbox_impact`. They ship in the first migration even though predict is stubbed, so its arrival needs no schema change. |
| `ForwardChangeDevice` | plain `Model` | The scope. FK to `dcim.Device`, plus the resolved Forward device key copied at SCOPED time so the record stays readable if the device is later removed. Unique on `(change, device)`. |
| `ForwardChangeCriterion` | `NetBoxModel` | One assertion. `family` (acceptance / state-preservation), `expectation`, `blocking`, and the query-identity triple `query_id` / `query_path` / `commit_id` plus `source_sha256`. |
| `ForwardChangeEvidence` | plain `Model` | One criterion's result at one snapshot. `phase` (before/after), `passed`, `row_count`, `snapshot_id`, `commit_id`, `executed_at`, `duration_ms`, and a bounded schema-only `shape` JSON. Unique on `(criterion, phase, snapshot_id)`. |
| `ForwardChangeReview` | `NetBoxModel` | One reviewer's decision. Staleness anchored to `branch_change_time` **and** `baseline_snapshot_id`. |
| `ForwardChangePolicy` / `ForwardChangePolicyRule` | `PrimaryModel` / `NetBoxModel` | Who must approve. Scoped by device role, site or tag — the properties a *network* change has — rather than by NetBox object type. |

#### What it reuses rather than duplicates

This list is the design. Anything on it that gets reimplemented is a bug
in the plan, not a shortcut.

| Capability | Reused from forward-netbox | Why not duplicate |
|---|---|---|
| Forward credentials, base URL, TLS, timeouts | `ForwardSource.parameters`, `encrypt_secret`, `get_masked_parameters()` | A second credential store is a second thing to leak. Forward auth is HTTP Basic on every request — there is no token flow to share — so a second client would mean a second copy of a decrypted password. The change names a `ForwardSource`; it never holds one. |
| The HTTP client, retry, rate limiting, API accounting | `ForwardClient` via `source.get_client()`, `_record_api_usage`, `api_usage_summary()` | Call-volume accounting is a customer commitment; a second unaccounted client silently breaks it. |
| Snapshot resolution and listing | `resolve_snapshot_id()`, `get_snapshots()`, `get_latest_processed_snapshot_id()`, `get_latest_collected_snapshot_id()` | `latestCollected` in particular already contains the "every in-scope device was backfilled" logic that took a release to get right. |
| Snapshot metrics | `get_snapshot_metrics()` | Already the freshness surface. |
| NQE execution, pagination, async polling, page-streak guards | `run_nqe_query()` | The truncation traps here are known and fixed. Re-deriving them is how you get a silent short read. |
| Before/after row diff | `run_nqe_diff(query_id, before, after)` | This *is* the state-preservation primitive. It already exists. |
| NQE query resolution and versioning | `resolve_nqe_query_reference()`, `get_committed_nqe_query()`, `get_nqe_query_history()`, `query_source_sha256()` | Criterion binding is exactly the resolution forward-netbox already does for maps. |
| Publishing a query to the org library | `has_nqe_library_write_permission()`, `add_org_nqe_query()`, `edit_org_nqe_query()`, `commit_org_nqe_queries()` | Opt-in; see Decision Log. |
| Device identity: NetBox device <-> Forward device key | `ForwardDeviceIdentity` | The change scope resolves through it and stores nothing authoritative of its own. |
| Branch creation, staging, diff, merge, conflicts | `netbox_branching` | See "What this is NOT". |
| "What did this branch change" | `change_explainability_summary()` (generalized to a branch) | Already renders per-model, per-field, per-action counts from `ChangeDiff`. That is the change document's body. |
| "What would this actually write" | `compare_model_rows()` / `PreviewRunner` | The write firewall and the priming contract took two releases and a hotfix to get right. Its `None` return — "this model was not compared" — is load-bearing and must be carried through, not coerced to zero. |
| Publishing a criterion query | `publish_builtin_nqe_map_queries()` in `query_binding_resolution.py` | Already stages, skips byte-identical sources, commits once for all changed paths, and re-binds. The `org` repository is the only writable one; `fwd` is read-only. |
| Blast-radius refusal thresholds | `ForwardDriftPolicy` as the pattern | Its `block_on_row_shrink` / `max_deleted_percent` shape is the right shape for a change's declared blast radius. |
| Verbatim device configs, before and after | `run_config_backup()` | Already writes per-device config to a git `DataSource` with object-level dulwich. Pointing it at the two pinned snapshots gives a config diff for free. |
| Diagnostics-safety policy | `record_issue` / `diagnostic_shape` | Evidence rows are support-bundle content. |
| Runtime validation and health blocks | `validated_runtime.py`, `health_checks.py` patterns | The dependency status belongs on a Health page, not in a log line. |

#### What it explicitly does not reuse, and why

- **`ForwardNQEMap`.** Same three query-identity columns, different
  contract: a map binds a query to a NetBox model for ingestion and is
  constrained to `FORWARD_SUPPORTED_MODELS`; a criterion binds a query to
  an assertion and is constrained to nothing. Sharing the table would put
  rows in the sync's query registry that the sync must never execute.
- **`ForwardValidationRun`.** It asserts *query health* — did the query
  run, did it return rows, did the identity contract hold — and gates the
  sync. A criterion asserts *network intent*. The two look alike and are
  not, and conflating them would make a broken query indistinguishable
  from a broken network.
- **`ForwardIngestion`.** A change is not a sync run. It shares
  `before`/`after` snapshot ids and a branch, and nothing else.

### 4. It ships inside forward-netbox

**Decided 2026-09-02: this is not a separate plugin.** It is a feature of
forward-netbox, shipped in 3.0, opt-in per source.

The alternative was a sibling plugin depending on forward-netbox 3.0, and the
reason it lost is the reuse table above. Every capability in it -
`ForwardClient`, `resolve_snapshot_id()`, `run_nqe_diff()`,
`compare_model_rows()`, `PreviewRunner`, `change_explainability_summary()`,
`run_config_backup()` - is private API. A separate plugin consuming those
would couple two independently-versioned packages through internals, which is
exactly the shape that made the netbox-branching 1.2 move expensive: six
private `SquashMergeStrategy` members and raw SQL against its tables, each of
which had to be audited against the new wheel before the runtime could move.
Choosing that deliberately, against ourselves, would have been repeating a
mistake we had just finished paying for.

Shipping in-plugin removes the problem rather than managing it. There is no
published surface to maintain, no version matrix, no `ImproperlyConfigured` on
a missing dependency, and no registry entry. Direct imports are legitimate
within one package.

What that costs, stated plainly:

- Six models and an eleven-state machine land in the plugin that writes
  inventory, so every future forward-netbox release gate carries them.
- The 45-minute gate is built around delete-path safety this feature does not
  touch, and it will now run for changes to it.
- A regression in 3.0 is harder to attribute, because a major runtime
  migration and a large new feature ship together. The release owner weighed
  that and chose it.

What that does NOT change: this feature stages into a branch and merges
through branching like everything else, and it writes nothing to the network.
The safety argument for separation was about blast radius, and the blast
radius here is a NetBox branch either way.

### 5. Genuinely new work vs. thin wrappers

Being honest about this is most of the value of the document.

**Thin wrappers — one to three functions each, no new concepts:**

| Work | Wrapping |
|---|---|
| Pin a snapshot | `resolve_snapshot_id()` then store the literal id |
| List candidate snapshots for the collect gate | `get_snapshots()` |
| Run a criterion | `run_nqe_query(query=..., snapshot_id=..., fetch_all=True)` |
| Before/after state diff | `run_nqe_diff()` |
| Bind a criterion to a version | `resolve_nqe_query_reference()` + `query_source_sha256()` |
| Publish a criterion query | `add_org_nqe_query()` / `commit_org_nqe_queries()` |
| Branch staging, merge, conflict | zero code |
| "What would this write to NetBox" | `compare_model_rows()` |
| Config before/after | `run_config_backup()` at each pinned snapshot |
| Credentials, retry, rate limit, pagination | zero code |
| Reachability precompute | `trigger_snapshot_reachability()` |

**Genuinely new — nothing in forward-netbox does any of this:**

- The state machine and its persistence. forward-netbox has run states
  (`ForwardSyncStatusChoices`, `ForwardMergeAttempt.Status`); it has no
  change lifecycle, no approval, no attestation.
- The **expectation vocabulary**: `rows-empty`, `rows-non-empty`,
  `row-count-equals`, `every-row-field-true`, `row-count-not-increased`.
  This is the piece that turns an NQE query into an assertion, and there
  is no equivalent anywhere in the tree.
- **Fail-closed criterion binding.** `resolve_nqe_query_reference()`
  resolves; it does not refuse a whole document because one criterion
  did not. Borrowing `draft_change_acceptance`'s rule — a criterion that
  cannot bind refuses the draft at draft time, rather than producing a
  best-effort set that fails later at verify — is new behaviour on top of
  an existing call.
- **Evidence rows and the regression-flip comparison.** The four-way
  `pass/fail` x `before/after` classification, and the rule that
  `fail -> fail` never blocks.
- **The state-preservation gate.** The hardest new thing in the design.
  `run_nqe_diff` supplies the rows; deciding which families to watch and
  what counts as inside the blast radius is entirely new judgement.
- **The collect gate** — snapshot distinctness, device-set completeness,
  per-device collection ordering. Each is cheap and each closes a way of
  passing a gate without having tested anything.
- **The change document**, grounded or refused: every claim cites an
  evidence row from *that* verify attempt, a target with no evidence
  reads `unproven` and never `pass`, and only a PROCEED is marked
  verified. This is the deliverable an operator hands to a CAB, and it is
  the piece with no analogue in either reference.
- **Two-anchor approval staleness.** `netbox-change-control` invalidates
  an approval when the branch moves. Here it must also invalidate when
  the baseline snapshot moves, because an approval given against one
  view of the network is not an approval of another.
- **The `predict_change` seam** (section 1b). The interface, the
  three-status outcome type, the advisory columns and the UI renderings
  are new work and are built now. The body is one line and stays that way
  until Forward ships the capability.

**Deliberately not built:** a Forward predict implementation. Not a
wrapper, because there is nothing to wrap — see the Decision Log.

**New client methods required in forward-netbox** (the only place this
design pushes work upstream, and it is optional to the first release):

- per-device collection timestamps for a snapshot, for the collect gate.
  Obtainable today through an NQE query over `snapshotInfo`, which is the
  preferred route precisely because it needs no new endpoint.
- a path-search call, if and when reachability evidence is wanted. Same
  posture as predict: absent today, additive when it arrives.

## Validation

- Unit tests for the state machine: every legal transition, and every
  illegal one refused by name. Specifically `VERIFIED_HOLD -> CLOSED`
  must be refused, and there must be a test that says so in its name.
- **The whole machine, driven end to end with predict absent.** Not a
  variant of the happy path — the default one. `STAGED -> APPROVED ->
  APPLIED -> COLLECTED -> VERIFIED_PROCEED -> CLOSED` with
  `predict_change` returning `unavailable` throughout, asserting a
  `CLOSED` change and a complete change document.
- A test that the verify gate reads no predict field, enforced
  structurally rather than by inspection: assert that
  `ForwardChangeEvidence` is the only model the gate queries, and that
  the four advisory columns appear in no gate code path. The separation
  in section 1a is worth nothing if only a comment holds it.
- All three `PredictOutcome` statuses rendered: `unavailable`,
  `unsupported`, `answered` (the last with a fake outcome, since nothing
  can produce a real one yet). Pin that `unavailable` advances the state
  exactly as a skip does, and that neither `unavailable` nor
  `unsupported` renders as a pass or as a HOLD.
- A licence-denial test: `ForwardLicenseTierError` raised inside
  `predict_change` becomes `status="unavailable"` with the facet sentence
  as its reason, and does not propagate. A licence limit reaching a
  caller as an exception is the bug this test exists to prevent.
- A test that an approval alone does not open the merge gate — the
  reference plugin's `test_approved_status_alone_does_not_open_the_gate`,
  restated for a verdict: `test_approval_without_a_proceed_verdict_does_
  not_merge`.
- Collect-gate tests, one per refusal: same snapshot as baseline; a
  scoped device missing from the after-snapshot; a scoped device present
  but `result != completed`; a device collected before `applied_at`.
- Regression-flip tests: all four before/after combinations, pinning
  that `fail -> fail` does not block and `pass -> fail` does.
- A **structural** contract test against forward-netbox, in the spirit of
  `test_preview_runner_satisfies_the_priming_contract`: assert that every
  dotted path in `required_callables` resolves and that its signature
  still accepts the keywords we pass. Testing one call is what let the
  2.8.7 priming gap ship; test the contract.
- A criterion-binding test that a query path which does not resolve
  refuses the whole SCOPED transition rather than binding the rest.
- A no-negative-control test: a criterion that has never failed cannot be
  marked blocking.
- Full Django suite on NetBox `4.7.0` with branching `1.2.0b1` in the
  forward-netbox development stack. **One stack, one run**: concurrent
  targeted runs corrupt the shared `test_netbox` and the symptoms read as
  code bugs.
- Live validation against a real Forward org: one change through the full
  machine to `CLOSED`, and one deliberately broken change that must reach
  `VERIFIED_HOLD` and must not be closable. A design that has never
  produced a HOLD has not been tested.

## Rollback

Nothing to roll back in forward-netbox beyond two edits: remove
`forward_change_control` from `VALIDATED_PLUGIN_APPS` and revert the
`change_explainability_summary` signature. The plugin itself is removed
from `PLUGINS` and its migrations reversed; it owns no NetBox core data
and holds no `PROTECT` reference to a device, so removing it cannot hold
anything hostage.

The one piece of state that outlives it: any criterion query published
into the customer's Forward org NQE library. Publishing is additive and
opt-in, and the uninstall documentation must name the query paths so an
operator can remove them. Do not delete them automatically — that is a
write to the customer's Forward library performed during an uninstall,
which is the worst possible time to be writing anything.

## Decision Log

- **The branch merges at CLOSE, not at APPROVE.** The alternative is the
  `netbox-change-control` model, where approval opens the merge and the
  network is never consulted. Refused: that plugin already exists, does
  it well, and adding Forward to it would buy one more pre-merge check.
  The reordering is the product.

- **A HOLD is a valid outcome, never an error.** Taken directly from the
  Skyforge flow. A gate that can only pass or crash gets disabled the
  first time it is inconvenient.

- **BASELINE precedes STAGE.** The tempting order is stage-then-baseline,
  because staging is the part that feels like work. Refused: without a
  before-phase measurement, a criterion that was already failing is
  indistinguishable from damage the change caused, and the plugin's first
  false accusation is the last time anyone trusts it.

- **The criterion query text is pinned by `commit_id`, not by path.**
  A query resolved by path at verify time may not be the query that ran
  at baseline. forward-netbox already learned this for sync maps
  (`optin-pinned-query-staleness`); a change gate has strictly less
  excuse.

- **Forward predict is stubbed, deliberately, and the rest is built.**
  This is a decision, not an oversight, and it should not be re-opened as
  one. Three reasons, each sufficient on its own: the predict workflow is
  **not live** at Forward; it is **licence-gated**, so a meaningful share
  of customers will never have it; and what exists today is **limited in
  scope**, so even a licensed customer would find changes it cannot
  model. Building the product on it would have made the whole thing
  undeliverable for most of the people it is for.

  What is built instead is the seam: `predict_change()` with a settled
  signature, a three-status `PredictOutcome`, four advisory columns in
  the first migration, and three defined UI renderings. The body returns
  `unavailable`. Filling it in later changes one function and no schema,
  no state, and no gate.

  The alternative was to make prediction the centre of the design, on the
  strength of what the Skyforge reference achieves with a transient
  Forward network and a digital-twin snapshot. Refused twice over: the
  capability is not there to call, and reaching it that way would mean
  creating and destroying networks in the customer's Forward org — a far
  larger break of the read-only contract than publishing a query, with a
  cost model nobody has measured.

  The verdict was placed on the post-change snapshot for exactly this
  reason. Verification needs a snapshot and an NQE query, which is the
  capability every tier already has and the one this plugin's parent has
  been exercising in production for a year. A design whose load-bearing
  claim rested on predict would have been a demo; this one is a product
  on every tier, and better on some.

- **A licence limit reads as "not licensed", never as an error.**
  `ForwardLicenseTierError` already exists and is documented as "a
  capability limit, not a fault: retrying, re-authenticating or re-running
  the sync cannot clear it". `predict_change` catches it and returns a
  status, so it never reaches an operator as a red banner or a retry
  button. Health renders it `info`, the status `health_checks.py` already
  uses for a thing there is simply nothing to say about.

  Corollary, and the part most likely to be got wrong later: **the tier
  cannot be pre-flighted.** `license_tier.py` records that Forward
  exposes no endpoint for it, and warns in its own header against adding
  a "check the tier first" gate on the assumption one exists. Predict
  availability is discovered the same way everything else is — by asking
  and reading the refusal.

- **A criterion must have failed at least once before it can block.**
  A query returning zero rows because it is wrong is indistinguishable
  from one returning zero because the network is healthy. Skyforge's
  `break` variant is the same idea. Without a negative control, a green
  check is not evidence — and a wall of green checks that prove nothing
  is worse than no checks, because people act on it.

- **Publishing a criterion query to the Forward org library is opt-in and
  separately permissioned.** It is the only Forward write in the design
  and it breaks the read-only property in `architecture-flow.md`. It is
  proposed at all because `run_nqe_diff` requires a `query_id` —
  inline query text cannot be diffed — so state preservation is
  unavailable without a published query. Gated on
  `has_nqe_library_write_permission()`, additive only, never deleting,
  and the plugin works without it at the cost of the state-preservation
  gate. Note also that the org-level token is Basic auth and that a
  customer's own login often lacks library write permission, so this must
  degrade rather than fail.

- **The dependency fails closed, inverting the registry's default.**
  Reusing the registry's shape without its degradation behaviour is
  deliberate and is the single most likely thing for a future reader to
  "fix" back. It is called out in `integration.py`'s docstring for that
  reason.

- **`required_callables`, not just `required_models`.** Content types
  prove a table exists. This plugin consumes functions.

- **No dependency on any of the five optional plugins.** Not a
  preference: they cannot be installed on 4.7. A design that assumed
  Validity for golden-config comparison would have been undeliverable on
  its own runtime.

- **Policies are the piece to cut if scope must shrink.** The
  approval-policy machinery is the least Forward-shaped part of the
  design and the part `netbox-change-control` already does better. A
  first release could ship with a single site-wide "N approvals"
  rule and lose almost nothing that Forward provides.

## Open

These are the risks and the unanswered questions. They are stated as
plainly as possible because a change-control tool that overstates its own
certainty is worse than none.

### 1. A prediction is not a guarantee, even the one we have

`compare_model_rows` predicts what a *sync* would write to *NetBox*. It
says nothing about what a device will do when configured. Three further
limits, each of which matters to a change gate specifically:

**It predicts staging, not merging.** It answers what would be written
into a branch. It cannot answer which of those writes the merge will
reject — `ProtectedError`, unique-constraint clashes, NetBox validation
rules. Those surface only at merge time, which in this machine is
`CLOSE`, the last step. A change can therefore pass PREDICT, pass VERIFY,
and still fail to close. That is the correct ordering (the network is
already correct at that point; only the NetBox record is behind) but the
UI must not present PREDICT as a merge guarantee.

**It has no delete slot.** The four counts are creates, updates,
unchanged and rejected. `PreviewRunner._delete_by_coalesce` always
returns `False`, and a model whose batch contains a delete-classified row
declines the whole model rather than mis-bucketing it. So a change that
principally *removes* things is the change the NetBox-side prediction is
least able to describe.

**A `None` is not a zero.** A model with no comparison returns `None`,
and the drift report's estate-level `in_sync` stays `None` while any
model is uncompared. The recorded incident is exact: a model with 45
Forward rows reached an empty-row shortcut, took the confident zero, and
displayed `In sync: Yes` — the only affirmative claim on the page, made
by the one branch that never looked at NetBox. This plugin inherits that
rule verbatim: unmeasured renders as unmeasured.

And its write firewall is weaker than it looks. `PreviewRunner` is a
duck-typed stand-in for `ForwardSyncRunner` — a null object, not a
transaction, not a permission, not a read-only connection. It prevents
writes by supplying stubs for every method the classification calls. A
method it fails to stub is a real write on a code path that thought it
was previewing. This is not hypothetical: 2.8.7 shipped a preview that
never primed its caches, and 2.8.8 was a hotfix for a priming call
reaching a method `PreviewRunner` lacked, which killed the whole
dependency preview on optional-plugin deployments. The mitigation there
was to test the contract structurally rather than test one model, and
this plugin must inherit that test, not just that class.

### 2. A passing snapshot is not a passing network

Forward models what it collected. A device whose collection failed is
**absent from the model**, not flagged: every bundled query filters to
`snapshotInfo.result == completed`, and a device disabled in Forward
vanishes from `network.devices` and the REST inventory entirely. So
"the criterion found no violations" and "the violating device was not in
the snapshot" produce identical output.

This is why device-set completeness is gate step 2 rather than a nice
report. It is also why the change document says `unproven` for a device
it cannot account for, and why `unproven` must never render as green.

### 3. Snapshot timing is not change timing

A snapshot whose creation time post-dates `applied_at` may still contain
a device that was collected before the change reached it. Forward exposes
per-device collection information and the gate compares per device — but
where that information is unavailable for a device, the honest answer is
`unproven`, and the temptation to fall back to the snapshot-level
timestamp must be resisted. A snapshot-level comparison is exactly the
kind of nearly-right check that produces a confident wrong verdict.

### 4. netbox-branching 1.2.0b1 is a beta with no upgrade path

Its own release notes say not to use it in production and that no upgrade
path to a later release is provided. A branch provisioned under 1.2.0b1
may need re-provisioning under 1.2.0 final.

Design consequence, and it is a real one: a change branch must be
**short-lived and reconstructible**. The durable state of a change is its
criteria, its scope and its evidence — all of which live in this plugin's
own tables. The branch is a staging artifact. A design that treated a
long-open change branch as the record would lose changes on the beta-to-
final move.

### 5. What still needs a human, and always will

- Deciding the change is worth making.
- Writing the criteria. The plugin can refuse a criterion that does not
  bind and a criterion that has never failed; it cannot tell whether a
  criterion asserts the thing the operator meant.
- Performing the apply.
- Attesting that the apply happened, and when. `APPLIED` is the one
  transition in the machine with no evidence behind it at all, and the
  page must say so rather than dress it up.
- Interpreting a HOLD. The gate names the failing criterion; it does not
  know whether the right response is to roll back, to fix forward, or to
  correct the criterion.

### 6. NQE call volume

A change with ten criteria costs ten executions at baseline, ten at
verify, one diff per state-preservation family, plus snapshot polling
during collect — call it twenty-five to forty executions per change,
before retries. Forward engineering has already raised unnecessary NQE
runs as a concern. Every path must be accounted through
`api_usage_summary()`, the per-change execution estimate must be shown on
the form before the change is saved, and the collect poll must back off
rather than run at the sync's polling cadence.

### 7. The state-preservation denominator is undecided

"Nothing else changed" over a whole network is unbounded. The gate needs
a finite list of state families to watch — routing adjacencies, ACL
hit-ability, interface states, reachability between named sentinel pairs
— and a rule for what counts as inside the declared blast radius.

**Open question: which families, scoped how, and who decides — the
policy, the change author, or a shipped default?** A shipped default that
is too broad makes every change HOLD and gets the gate turned off; one
that is too narrow makes the "and nothing else broke" claim a lie. This
is the single largest piece of unbuilt judgement in the design and it
should be settled with a customer before any code is written.

### 8. Attestation is the weakest joint

**Open question: who attests `APPLIED`, and what stops a change being
verified against a snapshot that predates the apply on some devices?**
The collect gate's ordering check is the mitigation, and it is only as
good as Forward's per-device collection times. If those turn out to be
coarse or absent for some platforms, the honest fallback is to require a
minimum interval between `applied_at` and the after-snapshot and to say
plainly that it is a heuristic — not to compute a verdict as though the
question were settled.

### 9. Smaller items

- The `ForwardChangePolicy` scoping vocabulary (role / site / tag) is
  asserted, not validated against a customer. It may want the NetBox
  `ConditionSet` treatment the reference plugin uses instead.
- Config before/after via `run_config_backup` gives a git diff of
  verbatim device configs, which is genuinely valuable and has no
  consumer on 4.7 now that Validity cannot install. Whether to render
  that diff in the change document ourselves, or wait for Validity to
  raise its ceiling, is unresolved.
- Nothing here has been costed against a real estate. The
  device-analysis refresh and the drift preview both turned out to be far
  more expensive than expected on a large deployment (842s to compare
  357,864 interface rows, before priming); a verify over a large scope
  deserves the same suspicion before it is promised as interactive.
