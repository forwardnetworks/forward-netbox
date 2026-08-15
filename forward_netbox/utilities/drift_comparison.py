"""Count how many objects actually differ, by running the apply classification.

The drift report used to call every fetched row a change, because the dependency
preview had nothing to compare against and said so with
``change_estimate_kind = "workload_upper_bound"``. That made ``In sync``,
``Drifted models`` and ``Total drift`` read "Not measured" on every run for
every deployment, permanently - a promise the UI could not keep.

The comparison here is not a reimplementation. It calls
``bulk_orm_apply_simple_models(..., preview=True)``, which normalises, resolves
and compares exactly as the apply does and returns before it writes. A separate
normaliser would drift from the real one, and the symptom would be a drift
figure wrong in whichever direction is least noticeable - worse than no figure,
because an operator would act on it.
"""


class _PreviewStatistics:
    """Absorb the per-row outcome calls the classification makes."""

    def __init__(self):
        self.outcomes = {}

    def increment_statistics(self, model_string, *, outcome="applied", amount=1):
        amount = max(0, int(amount or 0))
        if amount <= 0:
            return
        self.outcomes[outcome] = self.outcomes.get(outcome, 0) + amount

    def log_warning(self, *args, **kwargs):
        return None


class _PreviewEventsClearer:
    """The classification increments this per row; a preview clears no events."""

    def __init__(self):
        self.count = 0

    def increment(self):
        self.count += 1

    def clear(self):
        return None


class PreviewRunner:
    """A runner that records nothing and writes nothing.

    The classification reports unusable rows through ``_record_issue`` and moves
    per-model counters through ``logger``. Handing it the real runner would file
    ingestion issues and move statistics for a sync that never ran, so it gets
    this instead and the rejected rows are counted rather than persisted.
    """

    def __init__(self, sync=None):
        self.sync = sync
        self.logger = _PreviewStatistics()
        self.events_clearer = _PreviewEventsClearer()
        # Every lookup cache `sync_primitives` reads off a runner, seeded empty.
        # Enumerated from that module rather than discovered one AttributeError
        # at a time; they are all plain memoisation, so an empty one only costs
        # a query.
        self._content_types = {}
        self._asn_by_number_cache = {}
        self._device_by_name_cache = {}
        self._interface_by_device_name_cache = {}
        self._interface_canonical_cache = {}
        self._module_bay_by_device_name_cache = {}
        # The three negative caches are sets - the primitives call `.add` and
        # `.discard` on them, not item assignment.
        self._missing_device_by_name_cache = set()
        self._missing_interface_by_device_name_cache = set()
        self._missing_module_bay_by_device_name_cache = set()
        self._tag_by_name_cache = {}
        self._tag_by_slug_cache = {}
        self._vrf_by_name_cache = {}
        self._vrf_by_rd_cache = {}
        self._unique_lookup_cache = {}
        self._primed_missing_unique_lookup_keys = set()
        self._model_coalesce_fields = {}
        self._conflict_policy = {}
        self.rejected_rows = 0

    def _record_issue(self, *args, **kwargs):
        # Deliberately signature-agnostic. Callers pass different keyword sets
        # (`context`, `exception`, and more as paths are added), and a preview
        # cares only that a row was unusable - not why, since it files nothing.
        self.rejected_rows += 1

    def _content_type_for(self, model):
        # Read-only: a cached ContentType lookup, which the macaddress path
        # needs to compare an existing assignment against the incoming one.
        from .sync_primitives import content_type_for

        return content_type_for(self, model)

    # --- read-only lookups the classification needs -------------------------
    #
    # Each is delegated to the same primitive the real runner uses, so the
    # preview resolves objects exactly as the apply would. They were added one
    # at a time as the classification demanded them; every one was checked for
    # writes first, and the ones that write are why four of the five bespoke
    # paths still report no comparison.

    def _get_unique_or_raise(self, model, lookup):
        from .sync_primitives import get_unique_or_raise

        return get_unique_or_raise(self, model, lookup)

    def _lookup_interface(self, device, interface_name):
        from .sync_primitives import lookup_interface

        return lookup_interface(self, device, interface_name)

    def _get_device_by_name(self, device_name):
        from .sync_primitives import get_device_by_name

        return get_device_by_name(self, device_name)

    def _ipaddress_assignment_skip_reason(self, address):
        from .sync_reporting import ipaddress_assignment_skip_reason

        return ipaddress_assignment_skip_reason(address)

    def _ensure_vrf(self, row, *, update_existing=True):
        """Find the VRF, never create or update it.

        The real runner upserts here, which is why this override exists: the
        method is called from inside the ipaddress classification, so inheriting
        it would have written VRF rows during a read-only preview. The audit
        grep did not catch it, because it is a write behind a runner call rather
        than a direct ORM one - which is the case for the shim being a firewall
        rather than a convenience.

        Returning ``None`` for an absent VRF matches how slice one treats an
        absent dependency: the address cannot already exist in NetBox under a
        VRF NetBox does not have, so it classifies as a create.
        """
        from ipam.models import VRF

        name = (row or {}).get("name")
        rd = (row or {}).get("rd") or None
        if rd:
            match = VRF.objects.filter(rd=rd).order_by("pk").first()
            if match is not None:
                return match
        if not name:
            return None
        return VRF.objects.filter(name=name).order_by("pk").first()

    # --- state the classification reports into, which a preview discards ----

    def _dependency_failed(self, model_string, key):
        # Nothing has been applied, so nothing can have failed as a dependency.
        return False

    def _mark_dependency_failed(self, model_string, row):
        return None

    def _record_aggregated_skip_warning(self, **kwargs):
        return None


def compare_model_rows(sync, model_string, rows):
    """Return ``{"creates", "updates", "unchanged", "rejected"}`` or ``None``.

    ``None`` means this model has no comparison yet - the bespoke bulk paths and
    every adapter-only model - and the caller must keep reporting an upper bound
    for it. Reporting a confident zero for something never compared is the one
    failure mode here with a real consequence, because it would tell an operator
    they are in sync when nothing checked.
    """
    from .apply_engine_bulk import bulk_orm_apply_simple_models

    if not rows:
        return {"creates": 0, "updates": 0, "unchanged": 0, "rejected": 0}
    runner = PreviewRunner(sync=sync)
    counts = bulk_orm_apply_simple_models(
        runner,
        model_string,
        list(rows),
        preview=True,
    )
    if not isinstance(counts, dict):
        # `None` is the documented "no comparison" answer, but the apply path
        # also has plain-bool exits for its own callers. Anything that is not a
        # count mapping is treated as "not compared" rather than coerced, so a
        # path this has not audited can never surface as zero drift.
        return None
    outcomes = runner.logger.outcomes
    return {
        "creates": counts.get("creates", 0),
        "updates": counts.get("updates", 0),
        # Taken from the per-row outcomes the classification already reports
        # rather than recomputed per path. Every path increments "unchanged"
        # for a row it would not write, so this is one definition instead of
        # one per function - and the paths differ enough (skips, per-row
        # rejections, in-memory duplicates) that arithmetic on row totals would
        # quietly disagree with them.
        "unchanged": outcomes.get("unchanged", 0),
        # A row the classification could not use is not a difference between
        # the two systems; it is a defect in the row. Counted separately so it
        # never inflates drift.
        #
        # Read from the outcomes alone, not added to `rejected_rows`: a single
        # unusable row both files an issue and increments "failed", so summing
        # the two counted it twice.
        "rejected": outcomes.get("failed", 0) + outcomes.get("skipped", 0),
    }
