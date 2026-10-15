# Make change control operational

## Goal

Re-implement, inside this plugin, the change-request workflow and the pre-change validation that NetBox Labs documents for its Change Management and Validation products, built on `netbox_branching` and tied to Forward: the evidence is Forward's (NQE checks, snapshots, Predict, workspace networks, optional AI), not NetBox data alone. It is the free option, and Forward is what makes it more than a copy. Each phase ships on its own; the change request and validation are one phase.

## Constraints

- **Built from the public documentation, not their code.** NetBox Labs' own plugins use a limited-use licence and are Enterprise and Cloud only (`netboxlabs-netbox-changes` is not on public PyPI). We match the documented shapes (models, REST paths, statuses, event payloads) so scripts and event rules written against them port over, and we use our own names for the products.
- **The merge gate is netbox-branching's `pre_merge`.** `Branch.merge()` sends it with a plain `Signal.send` before it changes anything, so a receiver that raises aborts the merge from every entry point. Checked in the installed netbox-branching 1.2.1.
- Predict is released and works, with limited coverage. It gates where it answers and says so where it does not. It never blocks silently and never passes silently.
- Evidence comes from snapshots and NQE. Predict output and AI text stay apart from it unless a policy explicitly promotes Predict's pre-verdict to a gate.
- Availability is found by asking and reading the refusal (existing rule), using the SDK's typed errors. The licence tier is not read.
- The change-set and AI endpoints are unpublished; the SDK describes their shapes by hand. Every call to them sits behind one adapter with contract tests.
- A step that writes to Forward (a workspace, a change set, an AI chat) is created by a user action, never by a background sync.
- No customer names, network ids or snapshot ids in the repo.

## What we mirror, and what is ours

Documented by NetBox Labs (change requests): `ChangeRequest` has a name (defaults to the branch name), a branch, an owner, a policy, a status (`draft`, `needs-review`, `changes-requested`, `approved`, `completed`, `rejected`), an integer priority (1 low to 5 high, default 3), a summary, `comment_count` and `last_updated`. A policy has rules; a rule sets a minimum number of reviews, reviewer groups and named reviewers, and a policy can require independent review (the owner's reviews do not count). Reviews carry a status and comments; comments can be replied to and resolved (needs `change_comment`). Reviews go stale and approval reverts to needs review when the branch changes; affected requests are re-evaluated when rules, reviewers or group membership change; the default policy is assigned to a new request; a merge needs an approved request. `protect_main` forces changes through a branch, with a bypass permission. REST under `/api/plugins/changes/`: `change-requests`, `comments`, `comment-replies`, `policies`, `policy-rules`, `reviews`, and a `retrigger` action. Webhooks use the standard created, updated and deleted events on the request, with `context.last_branch_change`.

Documented by NetBox Labs (validation): a policy scopes by site, region, site group, device role, platform and device tag, holds rules (`engine`, `category`, `check_name`, `severity`, `parameters`), and can trigger on branch merge, on change request submission, or on a cron schedule. A run executes the rules against matching devices and can be narrowed to sites, devices or tags, or to a branch. Results are per device and check (`pass`, `fail`, `skip`, `warning`, `error`, with `expected`, `actual`, `remediation`). Findings aggregate related failures under a stable key and track `open`, `acknowledged`, `resolved`, `suppressed`. Severities are `critical`, `high`, `medium`, `low`, `info`; run statuses `pending`, `running`, `passed`, `failed`, `error`. The score per device and policy is passed over passed plus failed plus errored, times 100, skips excluded. REST under `/api/plugins/validation/`: policies (clone, export, import), rules, runs (`execute`, `validate-device`), results, findings (`summary`, `bulk-update-status`), compliance, policy packs. Their docs do not say whether a failed run blocks a merge, or how the score threshold is set.

Observed in real use (read-only, on one production Forward organisation): a workspace is a network with a `parent_id`. Of 124 networks, 117 are workspaces, all forked from the one main network, many named for change tickets. They hold only the devices of the change (3, 13, 114, 354 and 628 devices in this week's sample, against about 10,900 in the parent), 1 to 4 snapshots each, and about a third carry a 7-day retention that expires them. Operators already work this way by hand, so the plugin automates and records it.

What is ours, and where Forward comes in:
- **Engines.** Their engines are intent, config and graph. Ours are Forward's: `nqe` (a bound query at a committed version, the existing criterion), `check` (a native Forward intent check on a snapshot: isolation, reachability, exists, NQE) and `predict` (the same checks on a predicted snapshot). Existing `ForwardChangeCriterion` and `ForwardChangeEvidence` are the seed for a rule and a result.
- **Lifecycle.** Their review status and our Forward verification lifecycle are two axes, kept separate: the request has a review `status` and a Forward `verification` state (the existing baseline-to-verified graph).
- **Scope.** Our existing policy rules scope by what a change touches (role, site, tag). Theirs scope by who reviews. Both are wanted: the existing scope rule becomes a policy scope, and a new rule carries the reviewer requirement.
- **Event types.** Their lifecycle event types only create notifications and silently never fire event rules. Ours should fire both, if NetBox's event registry allows plugin event types to drive event rules (to be verified in the installed NetBox source before it is promised).

## Touched Surfaces

- `forward_netbox/change_control/`: a workspace adapter (create, adopt, retention), `actions.py` (transition service), a reviewer-rule evaluator, a Forward adapter, a validation engine package, a `pre_merge` receiver.
- Models: review status, priority, summary, policy and owner on the change request; policy scope, reviewer rules, comments and replies; validation policy, rule, run, result, finding; workspace and advisory fields. Existing records are few (the feature is not wired), so the migration restructures the scope rule and keeps `db_table` names where it can.
- REST API, GraphQL, filtersets, tables, views, navigation, search, notifications, event types, permissions (`change_comment`, bypass, approve).
- `pyproject.toml`: forward-sdk pin (currently 0.1.20; 0.1.21 is out).

## Approach

**Phase 1: the change request, reviews and Forward validation, scoped by a Forward workspace network, with the gate.** These ship together: a change request without validation behind its gate is a checklist, and validation without a request has nothing to gate.
1. **Scope is a Forward workspace network.** A change's devices become a workspace forked from the source's network with `networks.create_workspace(devices=...)`, named from the change's reference and given a retention (so it expires on its own). The change stores the workspace network id, and validation runs against the workspace, not the whole network. A change can also adopt an existing workspace by id, checked to be a child of the source's network (`parent_id`). NetBox devices map to Forward device keys through the existing `ForwardChangeDevice.forward_device_key`. Creating the workspace is a user action on the change, never a background step.
2. Models and REST: change request (documented fields plus the Forward fields), comments, comment replies, policies, policy rules, reviews, at `/api/plugins/forward/...` paths matching the documented ones, with `retrigger`; and validation policy, rule, run, result and finding, with `execute`, `validate-device`, `summary` and `bulk-update-status`.
3. Review workflow: a transition service (`transition(change, target, user)`), journal entries, stale-review detection and approval invalidation when the branch's last change time moves, re-evaluation when rules, reviewers or groups change, default-policy assignment, the independent-review option, owner-only edit of reviews and comments, and the `change_comment` permission.
4. Forward validation: policies scoped by site, region, site group, device role, platform and tag, with the three Forward engines (`nqe`, `check`, and `predict` once Phase 2 exists). A run executes the rules in the change's workspace, against its baseline and after snapshots for the matching devices, and can be narrowed to sites, devices, tags or a branch. It triggers manually, by the API, on request submission, on merge, or on a cron schedule. Results per device and check, findings aggregated under a stable key, and the score as documented (passed over passed plus failed plus errored, skips excluded). Policy import and export, and policy packs of ready-made rule sets (no device lost reachability, no new isolated subnet, interface and routing intent).
5. **The gate.** A `pre_merge` receiver blocks the merge unless the request is approved and, where the policy requires it, the latest validation run passed and the score meets the policy's threshold; the message lists what is missing. Submission runs validation automatically, so a reviewer sees the result before reviewing. Test how the UI, the REST API and the merge job show a refusal.
6. Notifications to the owner and eligible reviewers; webhooks through standard object events with `context.last_branch_change`; custom event types that fire event rules if the registry allows it.
7. Optional `protect_main` equivalent with a bypass permission, after the gate is proven.

**Phase 2: Predict as a gate.** A proposal (CLI text per device, validated with `validate_commands`) feeds a Forward change set based on the workspace's baseline (`latest_processed` already excludes predictions). Commit, `predict_and_wait`, then run the rules and the change set's native checks on the predicted snapshot and show the snapshot diffs (connectivity, loops, vulnerabilities, per-area counts). Three outcomes: unavailable (licence or permission), unsupported (cannot answer this question, for example a PAN-OS firewall, which needs its own path), answered. Policy chooses per change: require, prefer or ignore. An unanswered Predict is shown as "not predicted", never as a pass. Spike first: confirm where change sets and Predict run when given a workspace network id (the workspaces seen in real use hold several snapshots, which suggests predictions live there).

**Phase 3: Forward AI, optional.** Grounded questions on the baseline or predicted snapshot (change summary, blast radius), stored as labelled advisory text. It needs the `AI_ALLOWED` org property; a 403 hides the panel. Forward answers one question at a time per user, so calls use `busy_timeout` and run in a job. AI text never feeds the verdict or the score.

## Validation

- Per phase: the existing state-machine tests keep passing; new tests for each transition and its side effect, stale detection, re-evaluation, independent review, a refused merge through each entry point, an unavailable and an unsupported Predict, and a 403 from AI.
- API parity tests against the documented paths, statuses and event payload shape.
- Contract tests for the unpublished Forward endpoints against recorded responses, plus a live read-only probe that reports which capabilities an org has.
- A live check of the gate against a real branch on a NetBox 4.7 instance. A NetBox Cloud instance with netbox-branching 1.2.2 is available for it.
- A live read-only check that validation queries run against a workspace network and its snapshots, and a spike before Phase 2 on change sets and Predict inside a workspace (duration, coverage).
- `invoke harness-check`, `lint`, `check`, `test`, `ci`.

## Rollback

Each phase is additive behind its own models and views. Phase 1 can be reverted without touching Predict, workspace or AI code, and nothing here changes what a normal sync does. The gate is a signal receiver and can be turned off by policy or removed in one place.

## Decision Log

- Re-implemented from the public documentation rather than integrating the Enterprise plugin: it is not available to customers without Enterprise or Cloud, and a free version tied to Forward is the point.
- Two axes on the request, review status and Forward verification, because the review process and the evidence have different owners and different failure modes.
- The gate sits on `pre_merge`, so it holds for every merge path.
- Do not claim event-rule parity for lifecycle events until the installed NetBox event registry is checked.
- Predict gates but never decides silently.
- A change's device scope is a Forward workspace network, as operators already do by hand: queries run on the few devices the change touches instead of the whole network, the baseline is the workspace's own, and retention cleans up. NetBox scope stays the change's device list; the workspace is the Forward-side mirror of it.
