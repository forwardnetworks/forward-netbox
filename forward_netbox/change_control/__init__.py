# Change control: a Forward-backed gate around a NetBox branch.
#
# The one structural idea here is that the branch merges at CLOSE, not at
# APPROVE. Human approval is a necessary condition; the sufficient one is a
# post-change Forward snapshot showing the network is actually the way the
# branch says it is. Every generic change-control tool merges when the humans
# say yes, and cannot do otherwise, because it has no network model to consult.
#
# This ships inside forward_netbox rather than as a sibling plugin because
# every capability it needs - the client, snapshot resolution, run_nqe_diff,
# compare_model_rows, PreviewRunner - is private API. See
# `docs/03_Plans/active/2026-09-02-forward-change-control-concept.md`.
