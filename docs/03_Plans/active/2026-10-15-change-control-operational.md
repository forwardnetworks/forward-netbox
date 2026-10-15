# Make change control operational

## Goal

Make this plugin the Forward evidence and gate provider for **NetBox Change Management**, NetBox Labs' change-management plugin, instead of a second approval system beside it. NetBox Change Management owns the process: change policies (rules for minimum reviews, reviewer groups and named reviewers), change requests raised from a branch, reviews, stale-review detection, and the pre-merge approval gate. This plugin adds what only Forward can answer: criteria checked against before and after snapshots, a verdict, Predict where the org has it, a workspace network to scope the rehearsal, and optional Forward AI text. Without NetBox Change Management installed, the existing standalone approval flow remains. Each phase ships on its own.

## Constraints

- **Do not duplicate NetBox Change Management.** When it is installed, approvals, policies and the merge button are its; this plugin supplies evidence and a gate. The standalone reviews and policies are the fallback when it is absent.
- The two products meet at the NetBox **branch**: a Forward change links to a `netbox_branching` branch, and a change request is raised from that branch. No hard dependency on NetBox Change Management's models until its package is available to read.
- Predict is released and works, with limited coverage. It gates where it answers and says so where it does not. It never blocks silently and never passes silently.
- Evidence comes only from snapshots and NQE (the existing rule). Predict output and AI text stay apart from it unless a policy explicitly promotes Predict's pre-verdict to a gate.
- Availability is found by asking and reading the refusal (existing rule), using the SDK's typed errors (`license`, `license_expired`, `rbac`). The licence tier is not read.
- The change-set and AI endpoints are unpublished; the SDK describes their shapes by hand. Every call to them sits behind one adapter with contract tests.
- A step that writes to Forward (a workspace, a change set, an AI chat) is created by a user action on a change, never by a background sync.
- No customer names, network ids or snapshot ids in the repo.

## What is known and what is not

- Known from NetBox Labs' public documentation: the plugin's config key is `netbox_changes`; it depends on netbox-branching; it is in customer preview for NetBox Enterprise and NetBox Cloud, so it is not on public PyPI. Version 1.1.3 supports NetBox 4.5.2, 4.6.x and 4.7, so it fits this line.
- Its objects: change policies with rules (minimum reviews, reviewer groups, named reviewers, and a per-policy option that excludes the owner's reviews), change requests raised from a branch (status draft, needs review, completed; priority; summary), and reviews with comments. It marks reviews stale and reverts approval when the branch changes, and it requires an approved change request before a merge regardless of `protect_main`. `protect_main` forces changes through a branch; `netbox_changes.bypass_policy` is the bypass permission.
- **Its documented integration surface is NetBox event rules.** The object type is `netbox_changes.changerequest`; use the standard created, updated and deleted events with conditions. Its own lifecycle event types (submitted, approved, rejected, completed) only send in-app notifications and never fire webhooks. The webhook data carries the request's id, url, display, name and branch, and (from 1.0.0) the latest branch change.
- **Not known:** its distribution name and install source, its model fields beyond the above, and its REST and GraphQL shapes. The user guide does not say.
- Known from the installed netbox-branching (1.2.1): `Branch.merge()` sends `pre_merge` with a plain `Signal.send` before it changes anything, so a receiver that raises aborts the merge. That is the native place for our gate, and it applies to every merge entry point, including Change Management's merge button. Its own pre-merge gate and ours both have to pass.
- A different, third-party package (`netbox-branch-review`) is not the same product and is not a substitute.

## Touched Surfaces

- `forward_netbox/change_control/`: a new `actions.py` (transition service), a real `predict.py`, a Forward adapter module, an AI advisory module, a `pre_merge` receiver.
- Models: workspace fields on `ForwardChange`; a per-device proposal model (CLI text); advisory fields; policy flags for Predict and for integrated mode.
- Views, forms, templates, REST API, GraphQL, filtersets, tables, navigation, search, events.
- The optional-plugin registry (`plugin_integrations/registry.py`) gains `netbox_changes` once its package and versions are known.
- `pyproject.toml`: forward-sdk pin (currently 0.1.20; 0.1.21 is out).

## Approach

**Phase 1: the evidence engine and the merge gate, standalone.**
1. A transition service: `transition(change, target, user)` runs the existing entry check, applies the side effect, writes a journal entry and emits a NetBox event. Buttons and a REST action call it. Long steps run as NetBox jobs.
2. Bind and evaluate criteria: call `bind_criterion`, run each bound NQE query against the baseline and the after snapshot, write `ForwardChangeEvidence`, then `verify()` and `compute_verdict`.
3. The branch: create or link a `netbox_branching` branch, keep `branch_last_change_time` current, and invalidate the verdict when the branch changes after it was computed.
4. **The gate:** a receiver on `netbox_branching.signals.pre_merge` blocks the merge when the branch has a Forward change whose policy requires evidence and the verdict is not VERIFIED_PROCEED. The message lists the blockers. Test how the UI, the REST API and the merge job show the refusal.
5. NetBox-native surface: REST API and GraphQL for changes and criteria, filtersets, saved filters, search, a Changes panel on the Device page, approver notifications, and event rules so a state change can post to a ticketing webhook (the `ref` field already points at the ticket).

**Phase 1b: NetBox Change Management as an optional integration.** Register it in the optional-plugin registry (`plugin_integrations/registry.py`) like netbox-dlm and netbox-validity: app label `netbox_changes`, required and supported versions starting at 1.1.3, detected at runtime so the features appear only when it is installed. When present: a Forward panel on the change request page through a NetBox plugin template extension, the request linked to the Forward change through the shared branch, and the verdict and blockers shown there. In integrated mode this plugin does not record approvals; policies, reviews and the merge button are Change Management's. React to request changes in process with a model signal on `changerequest` (to refresh or invalidate our verdict), and document an event rule on `netbox_changes.changerequest` (updated) for ticketing webhooks. Align staleness: its approvals invalidate on a branch change, and our verdict must too. Blocked on getting the package: it needs its distribution name and a copy for the test image.

**Phase 2: workspace networks scope the change.** `networks.create_workspace(devices=...)` forks only the devices the change touches (the SDK documents this use for change windows). The change records the workspace id and retention, and the workspace is deleted when the change closes or is abandoned. Spike first: confirm where change sets and Predict run when given a workspace network.

**Phase 3: Predict as a gate.** A proposal (CLI text per device, validated with `validate_commands`) feeds a Forward change set based on the baseline (`latest_processed` already excludes predictions). Commit, `predict_and_wait`, then run the change's criteria and the change set's native checks (ISOLATION, REACHABILITY, EXISTS, NQE) on the predicted snapshot and show the snapshot diffs (connectivity, loops, vulnerabilities, per-area counts). Three outcomes carry over: unavailable (licence or permission), unsupported (cannot answer this question, for example a PAN-OS firewall, which needs its own path), answered. Policy chooses per change: require Predict (the merge gate also needs it answered PROCEED), prefer it, or ignore it. An unanswered Predict is shown as "not predicted", never as a pass.

**Phase 4: Forward AI, optional.** After the baseline or predicted snapshot exists, ask grounded questions (change summary, blast radius) and store the answer as labelled advisory text. It needs the `AI_ALLOWED` org property; a 403 hides the panel. Forward answers one question at a time per user, so calls use `busy_timeout` and run in a job. AI text never feeds the verdict.

## Validation

- Per phase: the state machine's existing tests still pass; new tests for each transition side effect, a failed merge, an unavailable and an unsupported Predict, and a 403 from AI.
- Contract tests for the unpublished endpoints against recorded responses, plus a live read-only probe that reports which capabilities an org has.
- A live spike on a real snapshot before Phase 2 and Phase 3 (workspace and change-set behaviour, Predict duration and coverage).
- `invoke harness-check`, `lint`, `check`, `test`, `ci`.

## Rollback

Each phase is additive behind its own model fields and views. Phase 1 can be reverted without touching Predict, workspace or AI code. Nothing in Phases 2 to 4 changes what a normal sync does.

## Decision Log

- Predict gates but never decides silently: when it cannot answer, the page says so and policy decides what that means.
- Workspace networks scope the rehearsal, not the NetBox data: NetBox scope stays the change's device list.
- The gate sits on netbox-branching's `pre_merge`, not on NetBox Change Management's internals, so it holds for every merge path and survives changes to that plugin.
- Do not build a second approval system beside NetBox Change Management. The standalone flow stays only as the fallback.
- The earlier assumption that Predict is not live is out of date; the stub in `predict.py` becomes the real call behind the same three outcomes.
- Open: the NetBox Change Management distribution name and install source (Phase 1b is blocked on it; it is a customer-preview product). Also open: whether the SDK needs new helpers. Likely candidates are a firewall Predict path, a workspace-to-parent lookup, and a typed capability probe; none is confirmed until the spike.
