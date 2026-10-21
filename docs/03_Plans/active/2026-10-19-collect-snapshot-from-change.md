# Collect a snapshot from a change, behind a warning and an access check

## Goal

After a change is applied, verification needs a snapshot of the workspace taken after the change. Today an operator has to start that collection in Forward by hand and then press Refresh snapshots. Let the change page start it, through the SDK's `snapshots.collect`, but only for someone whose Forward account may collect, and only after they have read what a collection does.

## Constraints

- A collection is a Forward write that contacts live devices through the organisation's collector. It is a user action on a change, never background work, and never starts without an explicit confirmation.
- Forward is only reached through the SDK. `snapshots.collect(network_id, wait=False)` already exists in forward-sdk 0.1.22, so there is no SDK gap.
- The access check is read-only (`user_accounts.get_current_user`) and follows Forward's published role table (docs.fwd.app, Settings > Accounts > RBAC): Collection Triggering is Yes for Workspace Operator, Network Admin and Org Admin, and No for Network Operator, Read-only and Limited Read-only. The role on the workspace decides, and roles granted through access-control groups are part of what Forward returns. A role held only on the parent network is not taken as one on the workspace, because Forward copies parent roles onto workspaces only when an organisation setting is on; that case, and any role the table does not cover, is left to Forward, and Forward's own refusal is shown. The table matches Forward's source (TRIGGER_SNAPSHOT_COLLECTION is held by WORKSPACE_OPERATOR and ADMIN only), as confirmed by the Go SDK session.
- No new model fields; the journal records who asked and the collector task id.

## Touched Surfaces

- `forward_netbox/change_control/workspace.py`: `collection_access`, `collect_snapshot`, and two methods on `ForwardWorkspaceApi`.
- `forward_netbox/views.py`: a confirmation page (GET) and the action (POST).
- `forward_netbox/api/change_views.py`: `workspace/collect`, which needs `confirm: true`.
- `forward_netbox/templates/forward_netbox/forwardchange.html`, new `forwardchange_collect.html`.
- Tests and `docs/01_User_Guide`.

## Approach

`collection_access` reads the account's roles and rules on them. `collect_snapshot` locks the change, requires the workspace, an applied state, a confirmation and a non-refused access answer, starts the collection without waiting, and journals it. The change page links to a confirmation page that states what a collection does (live device logins, collector load, devices in the workspace) and the access result; its form posts the confirmation. Refresh snapshots then picks the new snapshot up as the after snapshot, as it does today.

## Validation

- Tests, from the requirement: no collection without confirmation; no collection before the change is applied or without a workspace; a role Forward says cannot collect is refused for each of Network Operator, Read-only and Limited Read-only; an account with no role is refused; ADMIN is allowed; an unconfirmed role needs the confirmation and says so; the journal names the user and task; Forward's refusal is reported, not raised; the page shows the warning on GET and starts nothing; the API requires `confirm`.
- Not tested against Forward: starting a real collection. That is a Forward write and needs an explicit go-ahead on a throwaway workspace.

## Rollback

Revert; verification returns to using a collection started in Forward.

## Decision Log

- A double click can start two collections: the row lock serialises the requests but the second still sees the same applied state. There is no field to record one in flight and adding one needs a migration; the confirmation tick-box makes it unlikely and the journal shows each start.
- Access the table does not settle (an unlisted role, or no role on the workspace itself, including a parent-only admin) proceeds after the confirmation, and Forward's own refusal is shown. Only a role the table says cannot collect, or no role at all, blocks.
- The collector binding is not probed: a missing binding is reported by Forward when the collection starts.
