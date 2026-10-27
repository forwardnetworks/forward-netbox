# Change Control

Change control gives a network change a record in NetBox whose gates are
answered by Forward. A change names the devices it touches and the properties
that must hold afterwards, pins a Forward snapshot before it and another after
it, and is meant to merge its NetBox branch only when the after-snapshot shows
the properties hold. An approval opens the merge; Forward's evidence completes
it.

This page says what exists in the plugin today. Parts of the design are built
and tested; parts are not yet connected to anything that runs them. Both are
listed, so nobody plans around a step that does not happen.

## What you can use today

| Page | What it holds |
| --- | --- |
| **Changes** | One record per change: reference, title, source, requester, priority, summary, branch, window, devices, criteria, reviews, comments and its validation runs. |
| **Change Policies** | How many approvals a change needs before it is applied and before it is closed, the scope rules that decide which policy applies, and the reviewer rules. |
| **Validation Policies** | Which checks a change is held to, which changes they apply to, and the score the approval and closing gates want. |
| **Validation Runs** | Every run of those checks, with its score and per-check results. |

A change goes from creation to close without leaving the page:

1. **Create it** with a source, a title, a priority and (optionally) a NetBox
   branch. A branch a Forward sync owns is refused.
2. **Add devices** by NetBox name, then **Resolve Forward names**. One query asks
   the source's latest processed snapshot for its device names; only an exact,
   unique match is accepted, and a device Forward does not know stays unresolved
   and blocks the workspace. The device list is fixed once a workspace exists.
3. **Add criteria**: an NQE query path and what to expect of it. **Bind** each one
   to a committed query version. A criterion cannot change once the change is
   baselined or any evidence exists, and editing a bound criterion's path drops
   its binding.
4. **Create a Forward workspace** holding only the change's devices (or adopt one
   an operator made), **Refresh snapshots** to pin the baseline, and **Record
   baseline evidence**.
5. **Move it on** with the state buttons. Only moves a person may make by hand are
   offered: applying is attested, verification sets the verdict, and closing
   merges the branch.
6. **Review**: reviewers approve or reject at each gate, and comment.
7. **Attest it was applied**, then get a snapshot taken after the change: collect
   in Forward, or use **Collect a snapshot...** on the change. That button opens a
   page that says what a collection does (Forward logs in to each device in the
   workspace through your collector) and whether your Forward account may collect,
   and starts nothing until you tick the confirmation. It follows Forward's role
   table: Workspace Operator, Network Admin and Org Admin may trigger a collection;
   Network Operator, Read-only and Limited Read-only may not, and are refused. A
   role the table does not cover, or one held only on the source network rather
   than the workspace, is left to Forward, whose own refusal is shown. The
   REST route is `workspace/collect` with `confirm: true`. Then
   **Refresh snapshots** and **Verify against Forward**. Verification and validation
   run as background jobs.
8. **Close** merges the branch, once the verdict is PROCEED and the closing
   approvals are in.

Changes, policies, reviewer rules, criteria, reviews, comments and the validation
policies, rules, runs and results are also GraphQL types (`forward_change_list`,
`forward_change_validation_run_list` and so on), filterable and limited by the same
view permissions. Evidence rows are not exposed.

Everything on the page is also in the REST API under `/api/plugins/forward/`
(`changes/`, `change-criterion/`, `change-review/`, `change-comment/`,
`change-validation-policy/`, `change-validation-rule/`,
`change-validation-run/`, `change-validation-result/`). Actions such as
`transition`, `review`, `validate`, `verify` and `close` are POST endpoints on a
change and call the same code as the page, so they refuse the same things.

Approving is its own permission, `Can approve Forward change`
(`forward_netbox.approve_forwardchange`). Someone who may edit a change cannot
thereby sign it off. A reviewer's decision is recorded against the gate the
change is currently at (before apply, or before close), and an approval from the
person who raised the change does not count.

## Who must approve, and comments

A policy decides when it applies. **Reviewer rules** on the policy decide who must
approve: each rule names groups and/or users and a minimum count, and every
enabled policy that applies to a change must have all of its rules met, at the plan
approval and again at the closure approval. Only fresh approvals count. The
requester's own approval, an approval given before the evidence moved, and an
approval from a deactivated account do not. A rule that names nobody blocks every
change it covers, unless it is marked open to any reviewer, so deleting the only
group or user a rule names cannot quietly open it.

Reviewers discuss a change in **Comments** on its page. Anyone with permission can
comment and reply. Only the author edits or deletes their own words, a comment with
replies cannot be deleted, and resolving a comment needs the change permission on
comments. Rules are read live: editing or deleting one affects changes already under
review, and every edit is in the change log.

## The lifecycle

```text
DRAFT -> SCOPED -> BASELINED -> STAGED -> (PREDICTED) -> APPROVED -> APPLIED
      -> COLLECTED -> VERIFIED_PROCEED -> CLOSED
                   -> VERIFIED_HOLD    -> back to APPLIED
(any open state can be ABANDONED)
```

Each step has entry checks, and the **Next steps** section lists exactly what is
missing:

- **Scoped:** at least one device, every device known to Forward for this source,
  and at least one blocking acceptance criterion.
- **Baselined:** every criterion bound to a committed query version (`query_id`
  at a `commit_id`), and a baseline snapshot pinned by id. A criterion that is
  not bound could be judged against whatever the query says later.
- **Staged:** a NetBox branch holding the intended changes, and the baseline.
- **Approved:** enough distinct, current approvals. An approval goes stale if the
  branch or the baseline changed after the reviewer signed it.
- **Closed:** a second approval after verification. This is where the branch is
  meant to merge. A hold can never go straight to closed; it is fixed and
  verified again.

## How it uses NetBox

| NetBox feature | How the change records use it |
| --- | --- |
| Model features | A change and a policy are NetBox primary models, so they carry tags, comments, custom fields, journal entries, bookmarks and the change log like any other NetBox object. Criteria, reviews and policy rules are change-logged. |
| Branching | A change has a one-to-one link to a `netbox_branching` branch, plus fields for the branch name and its last change time so the record stays readable after the branch is merged or removed. A review is anchored to the branch's last change time. Nothing fills these fields in yet (see below). |
| Users | The requester, the person who attested the apply, and each reviewer are NetBox users. |
| Permissions | The usual view, add, change and delete permissions, plus the approve permission. Object permissions with constraints apply as elsewhere. |
| Menu | **Changes** and **Change Policies** appear in the Forward menu, shown only to users who can view them. |
| Forward | Criteria are NQE queries bound to a query id and commit id; baseline and after snapshots are pinned as literal ids, never as selectors that could resolve to a different snapshot later. |

## Validation

A validation policy holds **rules** and says which changes it applies to (by
site, region, site group, device role, platform or tag; no scope applies to every
change). Each rule uses one engine, all of them Forward's:

| Engine | What it does |
| --- | --- |
| **NQE query** | Runs an org query in the change's workspace and applies an expectation (`no-rows` or `some-rows`). Rows naming a device become one result per device. |
| **Forward intent checks** | Reads Forward's own checks (isolation, reachability, existential, predefined, NQE) on the snapshot, optionally narrowed by name or type. A named check Forward does not have is an error. |
| **Forward Predict** | Always reports skipped. A prediction asked for from the change page is advisory and is never read by a validation rule, so it cannot pass, fail or block an approval. |

A run validates the change's after snapshot once there is one, otherwise its
baseline, and records every result as a shape and a count, never a row. A result
Forward could not answer is an **error**, and one nothing measured is a **skip**:
neither is ever a pass. The **score** is 100 minus a penalty per failing or
erroring result (info 0, warning 3, error 10, critical 25); a run that measured
nothing has no score.

A policy can require a current completed run, a minimum score, and a run when the
change is staged. When it does, approving and closing are blocked until the run
exists, is current (the change's snapshots have not moved since), answered every
rule and reached the score. A run counts only for the policies whose rules it ran.

**Install a pack** on the Validation Policies page for a ready-made policy built on
the network's own intent checks, then edit it. Any policy exports to JSON and
imports back (`change-validation-policy/<id>/export/` and `.../import/`), naming
every problem and creating nothing if there is one.

The support bundle has a `change_control` section of counts only: changes by
state, workspaces, criteria and evidence, reviews, and validation runs and results
by status. It holds no titles, device names, query paths, snapshot ids or messages.

## Notifications, events and the branch page

Reviewers named in a policy's reviewer rules are notified (NetBox notifications)
when a change is submitted and when it verifies PROCEED; the requester is told
when it is approved, closed, abandoned, or has changes requested; commenters hear
about new comments; whoever started a validation run hears it finished. Nobody is
told about their own action, inactive users are skipped, and only people who may
view the change are notified. A rule open to any reviewer notifies no one.

The same moments are NetBox event types you can attach event rules to (webhooks,
scripts, notification groups): `forward_change_submitted`, `_approved`,
`_changes_requested`, `_verified`, `_closed`, `_abandoned`, `_comment_added` and
`_validation_completed`, on the Forward Change object type. The payload carries the
change's id, reference, title, state, status, verdict, priority, branch and
requester, and never evidence. Events and notifications go out after the change is
saved, and a failure in either never undoes the change.

A branch tied to a change shows a **Forward change** panel on its own page: the
change, its state and verdict, and why the branch cannot merge yet.

## What Forward allows here

**Ask Forward** on a change page runs two read-only checks through the SDK: whether
the organisation may use Predict, and whether Forward AI is on. The answer, with
Forward's own reason (for example a missing licence or a switched-off setting), is
shown on the page and used wherever Predict is mentioned. No licence table is
consulted. Where Forward does not allow something, the page says so and the rest
of change control works unchanged.

## Forward Predict (advisory, off by default)

Predict asks Forward what a set of CLI commands would do to the network, without
touching a device. It is licensed separately by Forward and does not model every
protocol, so it is off until **Ask Forward to predict change-control changes** is
turned on for the source, and Forward still refuses it where the licence, the
account or the organisation's settings do not allow it. The page names which.

1. Once the change has its workspace baseline, type the commands to try for each
   device in **Proposal** and save.
2. **Ask Forward to predict** creates a change set holding the proposal in the
   change's workspace network, commits it and asks Forward to predict it. This is a
   Forward write, so it asks for confirmation every time. Nothing is sent to a
   device.
3. **Refresh prediction** reads the predicted snapshot's state. It shows the
   predicted snapshot's id once Forward has processed it, or a failure naming
   Forward's state (Forward gives no further reason).

What to expect:

- The prediction covers the devices in the workspace on the workspace's baseline,
  not the rest of the estate. Firewalls staged as CLI text (PAN-OS) are refused
  because Forward cannot predict them that way.
- It is advisory. Approval, closing and the verdict never read it, a validation
  rule for Predict always reports skipped, and an answer does not move the
  change's state. Editing the proposal afterwards marks the prediction stale.
- The commands can hold secrets. They are kept in NetBox for this change only, not
  in the change log, the journal, the support bundle or an event, and the journal
  records only how many devices had commands. They are also held in Forward, in the
  change set, which anyone with access to the workspace can read; closing,
  abandoning or deleting the change deletes the change set and its predicted
  snapshots. A change set Forward could not delete is logged with its ids and, on
  an open change, stays recorded so asking again retries the delete; for a change
  that is already closed or gone it can only be removed in Forward.
- **Checked once against a live Forward**, on a demo network: create, validate,
  stage, commit, predict and delete worked, the predicted snapshot appears in the
  workspace's snapshot list, and a one-device prediction processed in under a
  minute. Not yet checked: what the predicted snapshot says, a licence refusal, a
  firewall, or a large network. An unlisted snapshot stays "in progress" for two
  hours before it is reported failed.

## What is not connected yet

- **Forward AI** is not used, only detected.
- **Scheduled validation runs** are not built: runs start on demand, through the
  API, when a change is staged, or when a gate asks for a current one.
- A workspace's retention is fixed at creation (14 days). A change that outlives
  it loses its workspace.
