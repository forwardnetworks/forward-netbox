# License-Tier Denial

## Goal

When Forward refuses an NQE query because of the organization's license tier,
say so — and say which capability is missing — instead of surfacing a raw HTTP
body among timeouts and auth failures.

Forward is splitting licensing into tiers. Most customers are on the full tier
today, so this is not a live incident; it is groundwork so the failure is
legible the first time someone is not.

## Contract

- A license-tier refusal raises `ForwardLicenseTierError`, naming the denied
  query and the license facet required.
- `ForwardLicenseTierError` subclasses `ForwardClientError`, so every existing
  handler still catches it — a capability limit must not become a crash.
- Every other non-transient HTTP failure keeps its current generic error.
- No behaviour changes when no denial occurs.

## Constraints

- Do not add a pre-flight tier check. Forward does not expose the license tier
  over its API — grep of the `api/` tree for `licenseTier`/`license` returns
  nothing — so there is nothing to read.
- Do not encode a per-map facet table (see Decision Log).
- Do not copy Forward's source into this repository.

## Touched Surfaces

- `forward_netbox/utilities/license_tier.py` (new)
- `forward_netbox/exceptions.py`
- `forward_netbox/utilities/forward_api_impl.py`
- `forward_netbox/tests/test_license_tier.py` (new)
- This plan.

## Approach

Forward's `QueryIdentityPolicy.getAccessDeniedException` produces exactly one
sentence: `Query <name> is not permitted for this organization's license tier`.
That arrives at the client's non-transient HTTP branch and became
`ForwardClientError("... failed with HTTP 403: <body>")`.

Detect the sentence, extract the query name, and raise a dedicated error whose
message states which facet is required. Two facts are asserted, both read from
Forward's policy code rather than inferred:

1. **Org-authored queries require the NETWORK facet.** The policy short-circuits
   on repository type, so a tier without NETWORK denies *every* org query
   regardless of content. This plugin's maps default to the org repository
   (`models.py`: `self.query_repository or "org"`), which makes NETWORK a floor
   for the plugin as a whole — tiers B and S can run none of it.
2. **CVE / CIS / STIG data requires the SECURITY facet**, which N and NP lack.
   That is the clean split inside the N-family: CVE and vulnerability maps need
   NS or NSP; everything else runs on N.

## Validation

- `test_license_tier`: 10 tests — recognition, a typographic apostrophe,
  non-matching Forward errors, query-name extraction, a denial with no query
  name, message content, and the client integration.
- `test_license_tier` + `test_forward_api`: 104 tests, OK.
- **Negative control:** with the client branch disabled
  (`if False and is_license_tier_denial(...)`), the integration test fails as it
  should; restored and re-run green. The first draft of that test re-implemented
  the classification instead of calling the client, so it would have passed
  against an unwired client — replaced.

## Rollback

Revert the commit. The change is additive and inert unless Forward returns a
tier denial, so reverting restores the previous generic error and nothing else.

## Decision Log

- 2026-07-30: **No per-map facet table.** A survey mapping the 52 bundled
  queries to minimum tiers was run (19 any-tier, 25 NETWORK, 5 not-B,
  3 SECURITY) but it matched schema *leaf names* in query text and over-reports
  — a bare `outputs` or `extensions` identifier counts as a hit. Only the CVE
  group and the `networkInstances`-based BGP/OSPF/VRF group are solid. Shipping
  a wrong table would tell an operator to buy the wrong license, which is worse
  than the generic guidance this ships instead.
- 2026-07-30: **Detection only, no pre-flight gate.** The tier is not on the
  API. A test asserts the message says so, to stop a future change adding a
  check on the assumption it exists.
- 2026-07-30: Subclassed `ForwardClientError` rather than introducing a new
  top-level error, so existing `except ForwardClientError` paths keep working.

## Evidence

- Forward policy source (`~/src/fwd`, read-only reference, not copied):
  `cv/licensing/{LicenseTier,LicenseFacet}.java`,
  `cv/nqe/licensing/NqeQueryIdentityPolicyService.java` (which queries),
  `cv/nqe/licensing/NqeLicenseTierPolicyService.java` (which schema paths),
  `cv/access/{NetworkOperation,OrgOperation}.java` (which operations).
- Tier→policy: B and S get `ofFwdQueries(...)` (`permitsOrgQueries=false`);
  N and NP get `ofFwdAndOrgQueries(...)`; NS and NSP get `QueryIdentityPolicy.ALL`.
- Operation gates: `USE_NQE` is BASE, but `VIEW_NQE_DIFFS` and
  `VIEW_NQE_LIBRARY` are NETWORK-or-SECURITY and `WRITE_TO_NQE_LIBRARY` is
  NETWORK — so a B tier cannot use the diff path at all.
