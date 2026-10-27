# Wire change control to Forward Predict (authoring and prediction, advisory)


## Goal

A change in NetBox can carry a proposal (CLI commands per device), ask Forward to predict it on the change's workspace baseline before anything is changed on the network, and show the outcome and predicted snapshot on the change page. The prediction stays advisory: it never feeds the verdict, an approval gate or a closing gate. This replaces the `predict_change` stub in `change_control/predict.py`, whose columns, state (`PREDICTED`) and panel already exist.

## Constraints

- Forward is reached only through the SDK, `client.change_sets` (forward-sdk 0.1.27, all of it unpublished). Every Forward write needs an explicit confirmation from the operator, as `collect_snapshot` does.
- A prediction must not become a decision. The gates read `ForwardChangeEvidence` and validation runs only; the proposal, the predicted snapshot and the predict columns are never read by them. A test proves two otherwise identical changes, one with an answered prediction and one without, get identical approve, close and verify outcomes.
- The change set lives in the change's workspace network, on the workspace's baseline snapshot (`before_snapshot_id`), because Forward stores the base per change set and it must be a processed snapshot of the network the set is created in. A created workspace holds only the change's devices (an adopted one is not checked against the device list), so the prediction speaks about those devices and not about reachability across the rest of the estate; the page says so.
- Proposal commands can hold keys and SNMP communities. They live in their own model that is not change-logged (NetBox's change log serialises every field of a change-logged model), are never written to the journal, a job log, the support bundle or an event, and a test proves it.
- No pre-gate on `enable_predict` or the org property: the SDK saw a licence refusal with `PREDICT_MODELING` on, so availability is found by asking and reading the refusal, as the capability probe already does. `enable_predict` stays the opt-in that decides whether the page offers the action.
- Out for 3.0.9: BGP advertisements (the SDK takes untyped dicts and has never run them live) and PAN-OS (accepted at staging, fails only at prediction with a state-only message, so it is refused up front). Validating the predicted snapshot through validation runs or evidence tables is out: gates count a current completed run, and a run against a prediction is exactly how a prediction becomes a decision. The proposal is stored as typed entries with a `kind` so advertisements can arrive without another migration.
- No customer identifiers in the repo, tests or docs; fixtures are synthetic.

## Touched Surfaces

- `forward_netbox/models.py`, migration `0067`: `ForwardChangeProposal` (change, kind, target, body; not change-logged) and, on `ForwardChange`, `predict_change_set_id`, `predict_snapshot_id`, `predict_digest`, `predict_started_at`.
- `forward_netbox/change_control/predict.py` (the real `predict_change`, error classification, staleness, cleanup), `workspace.py` (API wrapper methods), `actions.py` (transitions, cleanup on close and abandon), `forward_netbox/views.py`, `urls.py`, `forms.py`, `api/` (proposal, predict and refresh-prediction on the change), `templates/forward_netbox/forwardchange.html`.
- `scripts/validate_installed_routes.py` (new routes), user and reference docs, `scripts/release.py` plan text.

## Approach

1. **Proposal.** Edit per-device commands while the change is unapproved and has a workspace with a baseline. Saving stores a digest of all entries; changing the proposal after a prediction marks the prediction stale.
2. **Predict (confirmed, synchronous, no worker held).** Refuse unless the source has `enable_predict` on (enforced server-side, not only on the page), there is a workspace, a baseline still listed in the workspace, a non-empty proposal, no prediction already pending, and no PAN-OS device. Then: delete the superseded change set (a 404 means already gone; any other failure refuses the start and restores the page); `change_sets.create(snapshot_id=baseline, network_id=workspace)`; persist the change-set id at once; `set_commands` per device; `validate_commands` per device, refusing on an error; `commit`; `predict`; persist the predicted snapshot id and `pending` at once. Do not use `predict_and_wait`: it loses the id on a timeout, and an orphaned change set keeps its predicted snapshots out of Forward's retention.
3. **Refresh prediction (a button, like Refresh snapshots).** Read the snapshot's state: processed becomes `answered` and moves no state (the state is in the review anchor, so an answer must not invalidate an open review; `PREDICTED` stays a label an operator sets); a failed state becomes `failed` with the state named (the SDK carries no reason text); anything else stays `pending`. A timeout is never an answer.
4. **Errors.** The plugin's error translation only gains a `status` attribute (set for the maps panel; it carries the SDK error as `__cause__`, which is where `denial.kind` is read). Show four distinct messages: not licensed (`license`, `license_expired`), not allowed (`rbac`), not enabled (`org_setting`), and refused with no stated cause (a 403 with no kind). Any other failure is `failed` with the exception class.
5. **Cleanup.** Best-effort and never blocking a transition: delete the change set when the change is closed or abandoned, when it is re-predicted, through all three close paths (`transition`, the direct close in `close_change`, and the branch-merge signal, which sets CLOSED without `_apply`), and when the change is deleted (a `pre_delete` receiver that captures the ids and deletes in `on_commit`, so a bulk delete makes no Forward calls inside the transaction and a rollback never loses the id).
6. **Page.** The existing panel gains pending, failed and stale states, the proposal editor, the predicted snapshot id, the scope sentence, and "not verified against a live Forward" is documented, not shown.

## Validation

- Unit and integration tests derived from the requirement with the SDK's real change-set models and errors faked at the service boundary: lifecycle order and idempotent persistence of ids, each refusal, the four error messages, stale detection, cleanup on close, abandon, re-predict and delete, no command text in the journal, bundle, job log or change log, and the quarantine test above.
- `invoke harness-check`, `pre-commit run --all-files`, the route fixtures, then the full gate.
- **Not verified:** any of it against a live Forward. The SDK's Predict tests all use fakes and its read-only account has no Predict licence, so a first live run needs a Predict-licensed non-customer org and a Forward write, and the operator's go. Ship it as fixture-tested and say so in the changelog and docs. Also not verified: that Forward accepts a change set in a workspace network with a workspace baseline as its base.

## Rollback

Revert the release. The migration only adds columns (with database defaults, so an older plugin can still insert change rows) and a table that an older plugin ignores; the Forward side holds change sets and predicted snapshots that a revert would orphan, which is why cleanup runs on close, abandon, re-predict and delete.

## Decision Log

- Workspace network, not the source network: the baseline already is a processed snapshot of the workspace network, the prediction cannot touch the production network, and the cost is that it speaks only to the change's devices.
- A separate, non-change-logged proposal model rather than a JSON column on the change, because of the change log.
- Manual refresh rather than a background job, as `collect_snapshot` does: `predict` returns a snapshot id immediately and processing can run for an hour.
- Predicted-snapshot validation deferred deliberately; see Constraints.

## Review Decisions (plan review, 2026-10-27)

- The Predict validation rule stays SKIP and is tested; the change-control guide no longer says it "reports unavailable until Forward answers".
- The proposal digest is an HMAC keyed with the deployment secret, because it sits on a change-logged row and an unkeyed hash of a short command can be confirmed by guessing.
- The automatic STAGED to PREDICTED move was dropped: it changes the review anchor and re-queues validation and notifications.
- The REST actions stay, but the opt-in is enforced in `start_prediction`, so REST cannot write to Forward for a source that never opted in.
- Not verified, by design: whether Forward accepts a change set on a workspace network, whether the workspace's snapshot list includes predicted snapshots, and whether deleting a workspace removes its change sets. The first run answers them.
- The editor rows are built only for a user whose restricted `change` queryset includes the change, so an object-level constraint is honoured; the proposal needs `enable_predict` to be added to.

## Code Review Decisions (2026-10-27)

- A start that dies after claiming no longer strands the page: refresh marks an expired claim with no snapshot as failed, and the editor lists (and can clear) entries for devices that left the change.
- A start only writes while it still owns the claim (same `predict_started_at`, change not terminal); otherwise it deletes the change set it made. The claim restore also restores `predict_started_at`.
- The change-log test runs inside `event_tracking` with a control that logging did record, so it can fail. The save view masks its POST parameters from error reports.
- Not done: deleting by name after a create that timed out (the SDK only retries non-idempotent calls on connect errors, so a timeout can leave a set Forward made; the name `<ref>-<pk>` is deterministic, so this is a follow-up once a live run shows list behaviour), and reading `predicted_snapshots()` from the change set instead of the workspace snapshot list (to be chosen after the first live run).

## Live Check (2026-10-27, a synthetic Forward demo network, not a customer network)

Run with the SDK directly, mirroring the plugin's calls, in a temporary one-device workspace (one-day retention) that was deleted afterwards along with its change set; nothing was left behind.

- Verified: the workspace snapshot list includes the predicted snapshot and reports the same state as the change set (`PROCESSING` then `PROCESSED`), so `refresh_prediction` reading the workspace list is sound. `change_sets.create` returns a string id (`CHG-<n>`). `validate_commands` returned no errors for valid EOS commands. `predict` returned the predicted snapshot id in 0.1 s; the prediction processed in 41 s for one device.
- Found: a new workspace's baseline snapshot is `PROCESSING` for about 30 s, so a prediction needs the baseline `PROCESSED`, not just listed. `start_prediction` now checks that.
- Not checked: the content of the predicted snapshot, a licence refusal (the org is licensed), a PAN-OS device, and a large network.
