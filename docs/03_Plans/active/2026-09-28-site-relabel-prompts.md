# Site-relabel duplicates are never silent

## Goal

A customer's 221 duplicate device pairs sat behind a card on one page. The
card rendered only when a pair was mergeable. Every pair was held, so no page
said anything, and three blocked primary-IP issues named the stale copies
without saying what they were. When the plugin detects a repairable state, it
should say so where the operator already looks, and point at the action that
fixes it.

## Constraints

- Every prompt reads local data only: no Forward call on a page load.
- The per-pair protecting-reference scan (one query per model per pair) is
  skipped on every page load. The merge itself always recomputes with it on,
  so a count shown here never authorizes a delete.
- Issue messages keep naming only primary keys. The support bundle's
  `_issue_references` parses `#N` from these messages.
- A hint must never fail an apply, a page, or the post-sync tag pass.

## Touched Surfaces

- `utilities/scope_reconciliation.py`: `site_relabel_pairs(check_protecting=)`.
- `utilities/health.py`: `_site_relabel_duplicates_check`, carrying `url` and
  `url_label`.
- `templates/forward_netbox/forwardsync_health.html`: check links.
- `views.py` and `templates/forward_netbox/forwardsync.html`: the
  `health_attention` banner.
- `views.py` and `templates/forward_netbox/forwardsync_scope_reconciliation.html`:
  the card renders for held-only estates, with the held-reason breakdown and
  its remedies. The merge button appears only when a pair is mergeable.
- `utilities/sync_ipam.py`: `_site_relabel_partners` and the hint in
  `record_unowned_primary_ip_holder_skip`.
- `views.py` and `templates/forward_netbox/forwardingestionissue.html`: a
  repair link.
- `jobs.py`: `_log_site_relabel_backlog` in the post-sync tag pass.
- Tests: `forward_netbox/tests/test_site_relabel_prompts.py`.

## Approach

1. **A Health check, "Site-relabel duplicates".**
   - Warns with the mergeable count and each held reason with its remedy.
   - Links to Scope Reconciliation.
   - Health checks may now carry an optional `url`/`url_label`, rendered on
     the Health tab.
2. **The sync page.** It already builds the Health summary. Any warning or
   failure check that carries a link is shown as a banner above the sync
   details.
3. **The Scope Reconciliation card.** Renders when anything is mergeable or
   held, lists held counts by reason with the remedy, and offers the merge
   only when a pair is mergeable.
4. **The blocked primary-IP issue.** When a holder is a member of a detected
   duplicate group, the message names its partners and the merge. Group
   membership is computed at most once per run and cached on the runner.
   Runners without a real sync (`_NullSync`, `Mock`) get no hint and no query.
   The issue page adds a button to Scope Reconciliation.
5. **After each sync.** The post-sync tag pass, which has just stored the
   report the repair reads, writes one job-log warning while a backlog
   exists.

## Validation

- `test_site_relabel_prompts.py`:
  - **Health:** no duplicates, no check; mergeable pairs warn and link;
    held-only pairs say why and what to do; the protecting scan is skipped.
  - **Pages:** the sync page shows the banner and link; the scope card renders
    with no mergeable pair and has no merge button; the issue page links to
    the repair.
  - **Hint:** a duplicate holder names its partner and the merge; an ordinary
    holder gets nothing; the lookup runs once per run; a fake sync makes no
    query.
  - **Log:** the post-sync log line appears when there is a backlog, and not
    otherwise.
- Regression: the health, scope-reconciliation view, ingestion-issue and
  sync_ipam tests, then the full `invoke ci`.

## Rollback

Presentation only; no migration. Reverting removes the prompts, and the
repair itself is unaffected.

## Decision Log

- **The banner is generic: any linked warning or failure check.** Only this
  check carries a link today, but the next "operator must act" state gets the
  sync page for free instead of another bespoke alert.
- **Page loads skip the protecting scan.** For the customer's estate the full
  scan is roughly 221 × (related models) queries per render. The count's
  purpose is to send the operator to the page that does the full check.
