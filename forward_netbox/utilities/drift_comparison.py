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


class _NullSourceParameters:
    parameters = {}


class _NullSync:
    """Stands in for a sync when the caller has none. Carries no parameters."""

    source = _NullSourceParameters()
    pk = None
    name = ""


class PreviewRunner:
    """A runner that records nothing and writes nothing.

    The classification reports unusable rows through ``_record_issue`` and moves
    per-model counters through ``logger``. Handing it the real runner would file
    ingestion issues and move statistics for a sync that never ran, so it gets
    this instead and the rejected rows are counted rather than persisted.
    """

    def __init__(self, sync=None):
        # Some paths read source parameters straight off the sync -
        # `_scope_tags_enabled` does - so a caller with no sync gets a null
        # object rather than an AttributeError. Production always passes the
        # real sync, so this only affects callers that have none, and it
        # degrades to "no opt-in parameters set" rather than guessing.
        self.sync = sync if sync is not None else _NullSync()
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
        self._device_tag_ids_cache = {}
        self._scope_matched_tags = {}
        self._scope_tag_objs = {}
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

    def _ensure_platform(self, row, *, manufacturer_authoritative=False):
        """Find the platform, never create it - and never create a manufacturer.

        The real `_ensure_platform` upserts, and calls `_ensure_manufacturer`,
        which upserts too. Both are reached from inside the device
        classification, so inheriting either would write while measuring. Same
        class of trap as `_ensure_vrf`, and likewise invisible to a grep for ORM
        calls.
        """
        from dcim.models import Platform

        slug = str((row or {}).get("slug") or "").strip()
        name = str((row or {}).get("name") or "").strip()
        if slug:
            match = Platform.objects.filter(slug=slug).order_by("pk").first()
            if match is not None:
                return match
        if not name:
            return None
        return Platform.objects.filter(name=name).order_by("pk").first()

    def _ensure_manufacturer(self, row):
        from dcim.models import Manufacturer

        slug = str((row or {}).get("slug") or "").strip()
        name = str((row or {}).get("name") or "").strip()
        if slug:
            match = Manufacturer.objects.filter(slug=slug).order_by("pk").first()
            if match is not None:
                return match
        if not name:
            return None
        return Manufacturer.objects.filter(name=name).order_by("pk").first()

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

    # No shortcut for an empty row list.
    #
    # There used to be one: no rows, therefore nothing to create or update,
    # therefore a confident zero. It is true arithmetic and the wrong answer,
    # because it answers for models this function cannot compare at all.
    # `netbox_dlm.softwareversion` is adapter-only and has no comparison; a
    # deployment reporting 45 Forward rows for it still reached this line with
    # an empty row list, took the shortcut, and the drift report showed it
    # measured and `In sync: Yes` - the only affirmative claim on the page,
    # made by the one branch that never looked at NetBox.
    #
    # An empty row list is not evidence of agreement. It means either that the
    # model genuinely has nothing incoming, or that its rows never reached this
    # comparison - and those two are indistinguishable from here. So the
    # dispatcher answers instead, exactly as it does for a non-empty list: a
    # zero for a model it can compare, `None` for one it cannot.
    from .sync_primitives import prime_dependency_lookup_caches

    runner = PreviewRunner(sync=sync)
    rows = list(rows)
    # Prime the same dependency caches the apply primes, for the same reason.
    #
    # `apply_model_rows` calls this before handing rows to the classification
    # (`sync_reporting.py`); this function called the classification directly and
    # so never did. The classification then resolved every parent per row against
    # caches that started empty and stayed cold - one query per interface, per
    # MAC, per address - where the apply had already read them in bulk.
    #
    # A deployment measured it: 842s to compare 357,864 `dcim.interface` rows,
    # 58% of a 24-minute preview, against an estate whose real drift was 387
    # rows. The comparison was not slow because it compares; it was slow because
    # it was the only caller doing the resolution one row at a time.
    #
    # Priming is bulk SELECTs into runner-local dicts - no writes, and the
    # `PreviewRunner` seeds every cache this touches - so a preview may do it
    # exactly as the apply does. Comparing against a primed cache and comparing
    # against a cold one must return the same counts; `test_preview_primes_its_
    # lookup_caches` pins that they do, so this stays a cost change only.
    prime_dependency_lookup_caches(runner, model_string, rows)
    counts = bulk_orm_apply_simple_models(
        runner,
        model_string,
        rows,
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
