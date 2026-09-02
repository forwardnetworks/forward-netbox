# Technical debt

There is no separate tracker. The codebase carries no `TODO`/`FIXME` markers by
policy; every deferral is recorded in the plan that made it, under a trailing
`## Open` heading, with enough context to act on without recovering session
history.

To see what is outstanding:

```sh
grep -l '^## Open' docs/03_Plans/active/*.md
```

and read each section. A plan whose `## Open` says "Nothing" is closed. A
plan's `## Open` is edited - struck through with the closing change named -
when the item ships, so the file that made a deferral is the file that records
its end.

`completed/` is history: its deferral language described a decision at the
time and does not override current code, `ARCHITECTURE.md`, or an active plan.

## Current consolidation

The 2.9.2 release plan gathers every `## Open` item that was still true in
code as of 2026-09-02 into ordered tranches, and names the one exclusion
(NetBox 4.7 runtime support, which needs a beta `netbox-branching`). Items it
lists as closed carry the plan or pull request that closed them.
