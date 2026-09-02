# Stop calling every spent baseline "the baseline"

## Goal

A customer reported three ingestions that will not delete. The refusal told him
each of them "is the baseline for this sync — the durable record of what has
already converged", which is true of at most one, and directed him to "remove
those records first", which is an action the product does not offer for a
superseded baseline. Say what is actually holding each one.

## Contract

- The durable-record wording is reserved for the baseline that actually is one.
- A refusal never claims an action the product does not support.
- Wording only. No PROTECT semantics change, no migration, nothing becomes
  deletable that was not deletable before.

## Constraints

- "Spent" is `status == SUPERSEDED`, NOT `is_current is False`. `is_current`
  defaults to False and `status` to PENDING, so a baseline that has simply not
  been promoted yet is also not current — and calling that one spent would tell
  an operator mid-sync that an intact payload is a dead husk. Only promotion
  sets SUPERSEDED, and it is promotion that clears the relations and empties the
  payload (`contributor_baseline.py:848-864`).
- The refusal path must not raise. It already exists to turn an unhandled
  `ProtectedError` into something readable, so a lookup added to it fails closed
  to the conservative wording.
- `baseline_reference` stays derived from the protecting relations, not from
  this lookup: what blocks the delete is what the database says, and the status
  only chooses how to describe it.

## Touched Surfaces

- `forward_netbox/views.py` — `_ingestion_baseline_state` (new),
  `_ingestion_delete_refusal_detail`
- `forward_netbox/tests/test_ingestion_delete.py`

## Approach

The refusal decided "this is the baseline" by looking for the substring
`baseline` in the protecting model's label, which cannot distinguish the live
baseline from one superseded many syncs ago. Read the row and branch on its
status instead.

The superseded wording states three things and promises nothing: the record is
spent, it still protects the ingestion, and no action will release it today.

## Validation

Two tests. A superseded baseline must not get the durable-record wording and
must not be told to remove records; a pending baseline must still get the
existing wording, which is the case that would have regressed had this keyed off
`is_current`. Both refusals stay `expected=True` so they render as warnings
rather than red errors — the red error was itself reported as a defect twice.

## Rollback

Revert. Wording only.

## Decision Log

- **Wording before behaviour.** Making spent baselines collectable is the actual
  fix and it changes a PROTECT relation on the baseline-promotion path. Shipping
  the honest message first costs nothing, is independently correct, and tells us
  which of the two causes each of the customer's three ingestions has — which
  the current text cannot.
- **No "this will be fixed in a later release" in the message.** A refusal that
  dates itself is wrong the moment it ships. It says the gap exists; it does not
  make a schedule commitment to an operator.

## Open

- ~~The underlying gap is untouched: every ingestion that ever promoted a
  baseline is still permanently undeletable (#45).~~ **Closed.** The
  contributor baseline CASCADEs with its ingestion; a spent one is collected
  and a live one is held by the `pre_delete` receiver instead, which clears on
  the next promotion.
- ~~`ForwardDeviceIdentity` rows for departed source keys are the other cause
  (#46).~~ **Closed.** The provenance stamp is `SET_NULL`: the ownership
  survives, only the pointer to a spent run is dropped.

**Verified and pinned 2026-09-02** by
`forward_netbox/tests/test_ingestion_is_deletable.py`: no PROTECT relation
targets `ForwardIngestion` at all, so neither cause can return silently, and
the two refusals an operator can still meet - the live baseline and the
current ownership evidence - are asserted to be the only two, both temporary.
