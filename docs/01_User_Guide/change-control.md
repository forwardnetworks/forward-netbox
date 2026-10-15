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
| **Changes** | One record per change: reference, title, source, requester, window, description, comments and tags. |
| **Change Policies** | How many approvals a change needs before it is applied and before it is closed, and the scope rules that decide which policy applies. |

A change's detail page shows seven sections: **Change** (reference, source,
state, verdict, requester), **Snapshots** (baseline before, post-change after,
branch), **Applied** (when, who attested, reference), **Evidence**, **Scope**
(the devices), **Reviews**, and **Next steps** (each available transition and
what is still missing for it). A policy page shows the **Policy** settings and
its **Scope rules**.

Approving is its own permission, `Can approve Forward change`
(`forward_netbox.approve_forwardchange`). Someone who may edit a change cannot
thereby sign it off. A reviewer's decision is recorded against the gate the
change is currently at (before apply, or before close), and an approval from the
person who raised the change does not count.

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

## What is not connected yet

- **Nothing advances a change's state.** The state and verdict are not editable
  on the form, and no view or job moves a change between states. The detail page
  shows what each transition would need; it does not perform it.
- **Nothing runs the checks against Forward.** The code that binds a criterion to
  a query version, evaluates it against a snapshot and computes the verdict
  exists and is tested, but no page or job calls it, and the evidence rows it
  reads are not written by the plugin.
- **No branch is created or linked, and none is merged.** Staging needs a branch
  on the change, but the form does not expose it and no code sets it. The design
  says the branch merges at CLOSED; the plugin does not do that merge.
- **No REST API, GraphQL type or list filters** for changes.
- **Predict is a stub.** It is an optional advisory step kept apart from the
  verdict, and it is not live.

Treat a change today as a structured record with approvals and a checklist of
what would be required, not as an automated workflow. If you need a step from
the lists above, say which, so it is built on a real need.
