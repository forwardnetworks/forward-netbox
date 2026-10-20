# Collect a snapshot from a change, behind a warning and an access check

## Goal

After a change is applied, verification needs a snapshot of the workspace taken after the change. Today an operator has to start that collection in Forward by hand and then press Refresh snapshots. Let the change page start it, through the SDK's `snapshots.collect`, but only for someone whose Forward account may collect, and only after they have read what a collection does.

## Constraints

- A collection is a Forward write that contacts live devices through the organisation's collector. It is a user action on a change, never background work, and never starts without an explicit confirmation.
- Forward is only reached through the SDK. `snapshots.collect(network_id, wait=False)` already exists in forward-sdk 0.1.22, so there is no SDK gap.
- The access check is read-only (`user_accounts.get_current_user`). Forward's role table is not published, so the check refuses only what is certain: an account with no role at all. An organisation or network ADMIN is allowed. Any other role is allowed after the same confirmation, with the page saying access could not be confirmed, and Forward's own refusal is shown if it comes.
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

- Tests, from the requirement: no collection without confirmation; no collection before the change is applied or without a workspace; an account with no role is refused; ADMIN is allowed; an unconfirmed role needs the confirmation and says so; the journal names the user and task; Forward's refusal is reported, not raised; the page shows the warning on GET and starts nothing; the API requires `confirm`.
- Not tested against Forward: starting a real collection. That is a Forward write and needs an explicit go-ahead on a throwaway workspace.

## Rollback

Revert; verification returns to using a collection started in Forward.

## Decision Log

- A double click can start two collections: the row lock serialises the requests but the second still sees the same applied state. There is no field to record one in flight and adding one needs a migration; the confirmation tick-box makes it unlikely and the journal shows each start.
- Access that cannot be confirmed (a role Forward does not publish as able to collect, or an account that cannot read its own roles) proceeds after the confirmation, and Forward's own refusal is shown. Only a definite lack of access blocks. The alternative is to block unless the account is an administrator, which would stop legitimate non-administrator collectors with no way past it.
- The collector binding is not probed: a missing binding is reported by Forward when the collection starts.
