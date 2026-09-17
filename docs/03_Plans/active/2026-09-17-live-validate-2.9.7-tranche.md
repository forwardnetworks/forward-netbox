# Live-validate the 2.9.7 tranche; fix what only live execution finds

## Goal

Before calling the six 2.9.7 items resolved for the customer, prove the
three that only had unit-test / source-text coverage against real data:
the SNMP-vendor fix, the new SNMP interfaces/IPs query, and the routing
delete-blocker release. Fix whatever live execution finds that the linter
and mocked tests could not.

## Constraints

- No customer names, identifiers, or scan-matching hostnames in committed
  content, commit messages, or pull-request bodies. The live probes read
  the validation org; none of what they returned is recorded anywhere
  except aggregate counts already shared with the user in chat.
- The live probe is read-only: it runs the shipped/candidate query text
  against the live network and reads results, never writes anything to
  Forward or NetBox.

## Touched Surfaces

- `forward_netbox/queries/forward_ip_addresses_ipv4.nqe` - one-line fix in
  `endpoint_ipv4`.
- `forward_netbox/tests/test_release_foreign_delete_blockers.py` - new
  `RealNetboxRoutingIntegrationTest` (real `netbox_routing` models, not
  mocked stand-ins).

## Approach

**`regexMatches` returns a `List`, not a `Bool` - the linter does not
catch this.** `endpoint_ipv4`'s dotted-quad sanity guard was
`where regexMatches(hostIpRaw, re\`...\`)`. `nqe-lsp-validate` passed it
with no error; the live Forward NQE engine rejected it at execution time:
`NQE_RUNTIME_ERROR: Expression is not a Bool`. `regexMatches`'s real
signature (confirmed against `~/src/fwd/language/nqe`'s own
`RegexMatchesBuiltIn`) is `String -> Regex -> List<Match>` - built for
extracting matches, not testing for one. The fix wraps it in `!isEmpty(...)`,
matching this codebase's own `isEmpty`/`!isEmpty` idiom used everywhere
else for exactly this shape of check. This is the same class of gap
[[local-ci-parity-gotchas]] item 6 already names for a different builtin
(`foreach` used as a value) - the lesson generalizes: **the linter is
necessary but not sufficient for a NEW `.nqe` construct; live-run before
calling a query change done**, which the person building the SNMP
interfaces/IPs feature (me) did not do before merging PR #411.

**The routing delete-blocker feature had no test against a real
`netbox_routing` model.** Every existing test for
`release_foreign_delete_blockers` uses hand-built fake classes (see that
file's own docstring for why - these tables are not branch-aware, so the
loop's OWN decisions are the right unit to test in isolation). That
reasoning does not cover a DIFFERENT risk: that the assumed real on-delete
graph (`OSPFInterface.instance` PROTECT, reached only through a CASCADE
from `Device`, not through any of `Device`'s own direct relations) is
actually what `netbox_routing` declares. `RealNetboxRoutingIntegrationTest`
creates a real `OSPFArea`/`OSPFInstance`/`OSPFInterface` against a plain
Django fixture device, proves the delete really is blocked the assumed
way, then proves the release genuinely clears it and the device then
deletes.

**SNMP vendor fix and SNMP interfaces/IPs: live-verified, no code
change needed.** Ran the actual shipped endpoint branches (extracted
verbatim from the merged files, with `netbox_utilities`'s two helper
functions inlined since ad-hoc probe execution cannot resolve the
plugin's own published-library import path) against the validation org's
latest processed snapshot:
- Endpoint manufacturer: Unknown count dropped from the 84 sysDescr-less
  endpoints found before the fix to 34 after, with 10 resolving via the
  Infoblox profile-name fallback specifically (the exact gap the fix
  targeted). The residual 34 is the expected case (generic profile, no
  SNMP identity data at all).
- `endpoint_interfaces`: 1903 interface rows across 486 endpoint devices,
  sane names (`lo`, `eth0`, `eth1`) and plausible enabled states.
- `endpoint_ipv4` (post-fix): 1297 rows across the same 486 devices, every
  address a clean `/32`, and every `(device, interface)` pair matches a
  real row from `endpoint_interfaces` - zero orphans, proving the ifIndex
  join between the two queries is internally consistent against real
  Forward data, not just individually well-formed.

## Validation

- `invoke ci` on the 4.6 stack.
- `nqe-lsp-validate` on the fixed file: same pre-existing warnings only,
  no new diagnostic.
- `RealNetboxRoutingIntegrationTest`: the real `OSPFInterface.instance`
  PROTECT relation blocks the device delete, and
  `release_foreign_delete_blockers` clears it and the device then deletes.
- Live probes (not automatable, run manually this session): vendor
  Unknown-count reduction, interfaces/IPs row sanity and cross-consistency,
  documented above and in chat - not re-run by CI.

## Rollback

The `.nqe` fix is a one-line, purely additive tightening of an existing
guard (rejects nothing it should not have rejected; the bug made the
guard reject EVERYTHING, so any revert only returns to the fully-broken
state). The new test is additive with no migration or persisted-shape
change.

## Decision Log

- **Fix discovered by live-running, not by a new automated check.** No
  practical unit test catches "the live NQE engine's runtime type
  checking is stricter than the LSP's" for an unbounded set of stdlib
  functions; the durable fix is the practice (live-run before merging a
  new `.nqe` construct), recorded in [[local-ci-parity-gotchas]], not a
  new test that would only cover this one function.
- **A real-model integration test alongside the existing mocked ones,
  not instead of them.** The mocks test the release loop's own decision
  logic precisely and fast; the real-model test is the one thing they
  cannot cover - that the assumed schema shape is the real one.
