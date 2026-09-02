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

from rq.timeouts import JobTimeoutException


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

    def is_model_enabled(self, model_string):
        """No model is enabled when there is no sync to ask.

        `apply_dcim_inventoryitem` asks this to decide whether a module-native
        row should be deleted instead of upserted. Production always passes the
        real sync; this only affects callers with none, and it degrades to the
        plain upsert classification rather than guessing that a delete applies.
        """
        return False


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
        self._device_tag_ids_cache = {}
        self._cable_between_cache = {}
        # Per-device claimed component names, for `_unadoptable_component_names`.
        # Safe here and ONLY here: a preview writes nothing, so a device's
        # claimed names cannot change mid-run. The real runner deliberately has
        # no such attribute, because an apply creates modules as it goes.
        self._claimed_component_names_cache = {}
        self._scope_matched_tags = {}
        self._scope_tag_objs = {}
        self.rejected_rows = 0
        # Set by `_upsert_values_from_defaults` for the row it just resolved.
        # False rather than None so a path that reads it without an upsert
        # having run reports "no change" instead of raising - the adapter
        # comparison only reads it after an upsert it made itself.
        self.last_upsert_would_change = False
        # Every upsert outcome recorded since `begin_row`, for the paths where
        # one Forward row means several persisted objects and the last one is
        # not the whole answer. The routing models need it: a `BGPRouter` or
        # `BGPScope` this run would rewrite has no Forward query of its own, so
        # if the peer's verdict ignored it, nothing would ever report it.
        # `last_upsert_would_change` stays exactly as it was for the flat rows
        # that only ever upsert one object.
        self.upsert_outcomes = []

    def begin_row(self):
        """Start recording a new row's upserts. Called once per row."""
        self.upsert_outcomes = []
        self.last_upsert_would_change = False

    def _record_upsert_outcome(self, *, created, changed):
        self.upsert_outcomes.append(
            "creates" if created else ("updates" if changed else "unchanged")
        )

    def _record_issue(self, *args, **kwargs):
        # Deliberately signature-agnostic. Callers pass different keyword sets
        # (`context`, `exception`, and more as paths are added), and a preview
        # cares only that a row was unusable - not why, since it files nothing.
        self.rejected_rows += 1

    def _conflict_policy(self, model_string):
        """The same policy the apply would use for this model.

        This was seeded as an empty DICT, which is what the real runner is not:
        `sync_runner_contracts` defines it as a method and every caller invokes
        it. Nothing read the dict form, so the mistake was invisible - the bulk
        paths write through `bulk_create` and never ask. The adapter models all
        write through `coalesce_upsert`, which asks on every row, so the first
        one wired up would have hit `TypeError: 'dict' object is not callable`.

        Read from the real table rather than defaulted here, because the policy
        decides whether a conflicting row is skipped or raises - and a preview
        that disagreed with the apply about that would classify a row the apply
        refuses as one it would write.
        """
        from .sync import ForwardSyncRunner

        return ForwardSyncRunner.MODEL_CONFLICT_POLICIES.get(model_string, "strict")

    def _record_aggregated_conflict_warning(self, **kwargs):
        return None

    def _model_field_values(self, model, values):
        """Pure: drops values for fields the model does not carry. No queries."""
        from .sync_primitives import model_field_values

        return model_field_values(model, values)

    def _coalesce_sets_for(self, model_string, default_sets):
        """The coalesce sets the APPLY would use, not this call site's defaults.

        `coalesce_sets_for` reads `_model_coalesce_fields`, which the real
        runner fills from the resolved query specs before ingestion
        (`sync_execution.py:115`). A preview never runs that resolution, so the
        dict stayed empty and every lookup silently fell back to the hard-coded
        defaults passed in here.

        That is not cosmetic: an operator who narrows `dcim.inventoryitem`
        coalescing to ("device", "name") because serials are unreliable gets an
        apply that resolves and UPDATES the existing row, and a preview that
        misses it and reports a create - phantom drift on every such row, every
        run. Resolved lazily and memoised, because it costs a spec lookup and
        only the adapter models ask.
        """
        from .sync_primitives import coalesce_sets_for

        if model_string not in self._model_coalesce_fields:
            self._model_coalesce_fields[model_string] = self._resolved_coalesce_fields(
                model_string
            )
        return coalesce_sets_for(self, model_string, default_sets)

    def _resolved_coalesce_fields(self, model_string):
        """What the apply would resolve for this model, or `[]` to fall back."""
        from .model_contracts import architecture_default_coalesce_fields_for_model
        from .query_registry import get_query_specs

        try:
            specs = get_query_specs(model_string)
            if specs and specs[0].coalesce_fields:
                return [list(field_set) for field_set in specs[0].coalesce_fields]
            return architecture_default_coalesce_fields_for_model(model_string) or []
        except JobTimeoutException:
            # A worker boundary must never be swallowed by a lookup.
            raise
        except Exception:  # noqa: BLE001 - fall back to the caller's defaults
            # A preview must not fail because a spec could not be resolved; the
            # caller's defaults are what this method returned before it existed.
            return []

    def _is_module_native_inventory_row(self, row):
        """Read-only, and read from the apply's own table.

        It decides whether `apply_dcim_inventoryitem` deletes the row instead
        of upserting it, so a preview that answered differently would classify
        a deletion as a create.
        """
        from .sync import ForwardSyncRunner

        if row.get("module_component") is True:
            return True
        return (
            row.get("part_type") in ForwardSyncRunner.MODULE_NATIVE_INVENTORY_PART_TYPES
        )

    def _ensure_inventory_item_role(self, row):
        """Find the role, never create it.

        The real one upserts an `InventoryItemRole` through
        `_upsert_row_from_defaults`, reached during classification - the same
        trap as `_ensure_vrf` and `_ensure_platform`, and likewise invisible to
        a grep for ORM calls. Returning `None` for an absent role matches how
        every other absent dependency is treated: the item cannot already exist
        under a role NetBox does not have.
        """
        from dcim.models import InventoryItemRole

        role_name = row.get("role")
        if not role_name:
            return None
        # `row["role_slug"]` is INDEXED, not `.get()`, because the real
        # `_ensure_inventory_item_role` indexes it. A row missing that key
        # raises KeyError there and is counted as a rejected row; a `.get()`
        # here returned None and classified the same row as a create, so the
        # two paths disagreed about a row that is simply malformed.
        slug = str(row["role_slug"] or "").strip()
        if slug:
            match = self._get_unique_or_raise(InventoryItemRole, {"slug": slug})
            if match is not None:
                return match
        return self._get_unique_or_raise(InventoryItemRole, {"name": str(role_name)})

    def _delete_by_coalesce(self, model, lookups):
        """Never delete while measuring.

        Reached from `apply_dcim_inventoryitem`, which deletes a module-native
        row rather than upserting it when `dcim.module` is enabled. Raising
        would be the firewall's usual answer, but this method is a legitimate
        part of that path, so it returns "nothing was deleted" and the
        comparison refuses the model instead - see `_compare_dcim_inventoryitem`.
        """
        return False

    def _lookup_device_by_name(self, device_name):
        from .sync_primitives import lookup_device_by_name

        return lookup_device_by_name(self, device_name)

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

    def _optional_model(self, app_label, model_name, model_string):
        """Resolve a model from an optional plugin, or None when it is absent.

        Read-only, and the only reason it is here is that the priming added in
        2.8.7 reaches it: the routing identity primers ask for `BGPRouter`,
        `BGPScope`, `OSPFInstance` and `OSPFArea` through this. Without it the
        whole dependency preview died with a bare `AttributeError` on any
        deployment that has `netbox_routing` installed - and the tests missed it
        because they exercised `dcim.interface`, whose priming touches only
        caches `PreviewRunner` already seeded.

        `test_preview_runner_satisfies_the_priming_contract` now asserts the
        whole attribute surface rather than one model's slice of it.
        """
        from .sync_primitives import optional_model

        return optional_model(app_label, model_name, model_string)

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

    def _upsert_values_from_defaults(
        self,
        model_string,
        model,
        *,
        values,
        coalesce_sets,
        create_instance_attrs=None,
    ):
        """Find the row, never create or update it, and say whether it differs.

        This is the primitive every adapter-only model writes through, so it is
        the single override that makes them previewable at all. The real one
        creates the row when absent and saves the fields that differ; this one
        resolves through the same coalesce lookups and stops.

        ``last_upsert_would_change`` records whether the apply would have
        written, computed with ``_model_field_value_matches`` - the comparator
        the real upsert uses - so a preview cannot disagree with the apply about
        what counts as a difference. A second comparison written here would be
        wrong in whichever direction is least noticeable, which is the failure
        this whole feature exists to avoid.

        ``create_instance_attrs`` is accepted and ignored on purpose: it exists
        for side effects of ``save()`` (``dcim.Module``'s ``_adopt_components``,
        which makes NetBox core instantiate component rows), and a preview never
        saves.
        """
        from .sync_primitives import _authoritative_update_values
        from .sync_primitives import _dedupe_lookups
        from .sync_primitives import _model_field_value_matches
        from .sync_primitives import coalesce_lookup
        from .sync_primitives import get_unique_or_raise

        lookups = _dedupe_lookups(
            [coalesce_lookup(values, *coalesce_set) for coalesce_set in coalesce_sets]
        )
        usable = [lookup for lookup in lookups if lookup]
        if not usable:
            # Exactly what `coalesce_update_or_create` raises. Falling through
            # to "would create" instead reported drift for a row the apply
            # cannot process at all - a tag row with neither `tag` nor
            # `tag_slug`, say - and that drift never cleared, because every
            # subsequent run raised the same way.
            raise ValueError("At least one coalesce lookup must be provided.")
        obj = None
        for lookup in usable:
            obj = get_unique_or_raise(self, model, lookup)
            if obj is not None:
                break
        if obj is None:
            # Absent, so the apply would create it. Reported as a create by the
            # caller; `would_change` is not the question for a row that is not
            # there at all.
            self.last_upsert_would_change = False
            self._record_upsert_outcome(created=True, changed=False)
            return None, True
        self.last_upsert_would_change = any(
            not _model_field_value_matches(model, obj, field, value)
            for field, value in _authoritative_update_values(
                model._meta.label_lower,
                values,
            ).items()
        )
        self._record_upsert_outcome(
            created=False, changed=self.last_upsert_would_change
        )
        return obj, False

    def _coalesce_update_or_create(
        self,
        model,
        *,
        coalesce_lookups,
        create_values,
        update_values=None,
        conflict_policy="strict",
        return_change=False,
        create_instance_attrs=None,
    ):
        """The other upsert primitive, read-only.

        `_upsert_values_from_defaults` funnels into this one, but several
        adapter paths - FHRP groups and their assignments among them - call it
        directly with explicit lookups rather than coalesce sets. Both have to
        be overridden or the firewall has a hole exactly where the caller was
        most explicit about what it was writing.

        Returns the same shape the real one does, with ``created`` reporting
        what WOULD happen, and records the field comparison on
        ``last_upsert_would_change``.
        """
        from .sync_primitives import _authoritative_update_values
        from .sync_primitives import _model_field_value_matches
        from .sync_primitives import get_unique_or_raise

        usable = [lookup for lookup in (coalesce_lookups or []) if lookup]
        if not usable:
            # Same refusal the real primitive makes; see the sibling override.
            raise ValueError("At least one coalesce lookup must be provided.")
        obj = None
        for lookup in usable:
            obj = get_unique_or_raise(self, model, lookup)
            if obj is not None:
                break
        if obj is None:
            self.last_upsert_would_change = False
            self._record_upsert_outcome(created=True, changed=False)
            if return_change:
                return None, True, True
            return None, True
        values = create_values if update_values is None else update_values
        changed = any(
            not _model_field_value_matches(model, obj, field, value)
            for field, value in _authoritative_update_values(
                model._meta.label_lower,
                values,
            ).items()
        )
        self.last_upsert_would_change = changed
        self._record_upsert_outcome(created=False, changed=changed)
        if return_change:
            return obj, False, changed
        return obj, False

    @property
    def FORWARD_BGP_ADDRESS_FAMILY_ALIASES(self):
        """The apply's own alias table, not a copy.

        `normalize_bgp_address_family` reads it off the runner to fold Forward's
        AFI/SAFI spellings onto NetBox's choices. A second copy here would drift
        from the real one and classify a peer address family as a create under a
        name the apply never writes.
        """
        from .sync import ForwardSyncRunner

        return ForwardSyncRunner.FORWARD_BGP_ADDRESS_FAMILY_ALIASES

    def _ensure_asn(self, asn_value):
        """Find the ASN, never create it - nor the RIR beneath it.

        The real one saves an `ASN` and calls `_ensure_forward_observed_rir`,
        which upserts a `RIR`. Both are reached from inside the BGP peer
        classification, so inheriting either would write while measuring - the
        same trap as `_ensure_vrf` and `_ensure_platform`.

        The validation is kept because the apply rejects those rows too: an
        unparseable or sub-1 ASN raises there, is recorded per row and skipped,
        so a preview that quietly returned `None` would classify a row the apply
        refuses as a create.
        """
        from ipam.models import ASN

        from ..exceptions import ForwardQueryError

        try:
            asn_number = int(asn_value)
        except (TypeError, ValueError) as exc:
            raise ForwardQueryError(f"Invalid BGP ASN value `{asn_value}`.") from exc
        if asn_number < 1:
            raise ForwardQueryError(
                f"Invalid BGP ASN value `{asn_value}`; ASNs must be greater "
                "than or equal to 1."
            )
        existing = self._get_unique_or_raise(ASN, {"asn": asn_number})
        if existing is not None:
            self._asn_by_number_cache[existing.asn] = existing
        return existing

    def _coalesce_upsert(
        self,
        model_string,
        model,
        *,
        coalesce_lookups,
        create_values,
        update_values=None,
        return_change=False,
        create_instance_attrs=None,
    ):
        """The third upsert primitive, read-only.

        `ensure_bgp_scope` calls this one - the model-string-carrying wrapper
        that resolves the conflict policy before delegating to
        `coalesce_update_or_create`. The preview overrides the delegate, but
        the real `coalesce_upsert` would still have been inherited and written,
        because it is defined on the runner rather than reached through one of
        the two methods already shimmed. A hole exactly where a caller was
        explicit about the model it was writing.
        """
        return self._coalesce_update_or_create(
            model,
            coalesce_lookups=coalesce_lookups,
            create_values=create_values,
            update_values=update_values,
            conflict_policy=self._conflict_policy(model_string),
            return_change=return_change,
            create_instance_attrs=create_instance_attrs,
        )

    # --- routing chains, resolved with preview threaded through -------------
    #
    # Each delegates to the same impl function the real runner does, with
    # `preview=True`. The threading exists for the writes that are NOT behind a
    # `runner.` call and so cannot be shimmed: `ensure_bgp_peer_ip` saves an
    # `IPAddress` directly, the same shape as the FHRP virtual IP.

    def _ensure_netbox_routing_bgppeer(self, row):
        from .sync_routing_impl import ensure_netbox_routing_bgppeer

        return ensure_netbox_routing_bgppeer(self, row, preview=True)

    def _ensure_bgp_address_family(self, row):
        from .sync_routing_impl import ensure_bgp_address_family

        return ensure_bgp_address_family(self, row, preview=True)

    def _ensure_bgp_peer_address_family(self, row):
        from .sync_routing_impl import ensure_bgp_peer_address_family

        return ensure_bgp_peer_address_family(self, row, preview=True)

    def _ensure_ospf_instance(self, row):
        from .sync_routing_impl import ensure_ospf_instance

        return ensure_ospf_instance(self, row, preview=True)

    def _ensure_ospf_area(self, row):
        """No `preview` argument, and that is the audit result, not an omission.

        The area resolves through one `_upsert_values_from_defaults` and
        nothing else - no device, no VRF, no interface - so the impl function
        is already read-only under this runner as written.
        """
        from .sync_routing_impl import ensure_ospf_area

        return ensure_ospf_area(self, row)

    def _ensure_ospf_interface(self, row):
        from .sync_routing_impl import ensure_ospf_interface

        return ensure_ospf_interface(self, row, preview=True)

    def _ensure_peering_relationship(self, row):
        """No `preview` argument, and that is the audit result, not an omission.

        Every write in this chain is the one `_upsert_values_from_defaults`
        below it, which is already shimmed, so the impl function is read-only
        under this runner as written.
        """
        from .sync_routing_impl import ensure_peering_relationship

        return ensure_peering_relationship(self, row)

    def _lookup_module_bay(self, device, module_bay_name):
        from .sync_primitives import lookup_module_bay

        return lookup_module_bay(self, device, module_bay_name)

    def _ensure_module_bay(self, device, row):
        """Find the bay, never create it.

        The real one upserts a `ModuleBay` when the device has none, coalescing
        on the CLEANED name from `module_bay_plan_row` rather than the raw one.
        The same cleaning is applied here, or a bay Forward reports as
        ``"Slot 1 "`` would miss a NetBox ``"Slot 1"`` and the row would be
        classified a create against a module that already exists.
        """
        from .module_readiness import module_bay_plan_row

        existing = self._lookup_module_bay(device, row.get("module_bay"))
        if existing is not None:
            return existing
        cleaned = (module_bay_plan_row(row) or {}).get("name")
        if not cleaned or cleaned == row.get("module_bay"):
            return None
        return self._lookup_module_bay(device, cleaned)

    def _ensure_module_type(self, row):
        """Find the module type, never create it - nor a manufacturer under it.

        The real one upserts a `ModuleType` and calls `_ensure_manufacturer`
        beneath it, both reached during classification. Same trap as
        `_ensure_platform`, which nests the same manufacturer write.

        Resolved through `get_unique_or_raise` rather than a raw queryset, for
        two reasons. It reads the cache `prime_dependency_lookup_caches` has
        already filled for this model - a raw `.filter()` per row threw that
        priming away and reintroduced the per-row resolution cost 2.8.7 exists
        to remove. And it RAISES on an ambiguous match, where `.first()` would
        silently take the lowest pk and let a preview succeed on a row the
        apply refuses.
        """
        from dcim.models.modules import ModuleType

        # Indexed, not `.get()`, to match the real `_ensure_module_type`. A row
        # missing `manufacturer`, `manufacturer_slug` or `model` raises
        # KeyError in the apply and is counted rejected; `.get()` here turned
        # the same malformed row into a create.
        manufacturer = self._ensure_manufacturer(
            {"name": row["manufacturer"], "slug": row["manufacturer_slug"]}
        )
        model = row["model"]
        if manufacturer is None or not model:
            return None
        return self._get_unique_or_raise(
            ModuleType,
            {"manufacturer": manufacturer, "model": model},
        )

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


def _compare_adapter_rows(runner, rows, apply_row, *, uncomparable_outcomes=()):
    """Classify adapter-only rows one at a time.

    The bulk models hand a whole batch to a classifier that returns counts. The
    adapter models have no batch - each row is applied on its own - so the loop
    belongs here rather than to any one apply function, and every adapter model
    shares it.

    ``apply_row`` is called with ``preview=True`` and must return one of the
    count keys. The apply functions' own "I declined this row" answers -
    ``False`` for the skip paths, ``None`` where a function falls off its end -
    are counted as rejected, because a row the apply refuses is not a
    difference between the two systems and must never be reported as drift the
    next run would resolve.
    """
    from ..exceptions import ForwardDependencySkipError
    from ..exceptions import ForwardQueryError
    from ..exceptions import ForwardSearchError
    from ..exceptions import ForwardSyncDataError

    counts = {"creates": 0, "updates": 0, "unchanged": 0, "rejected": 0}
    for row in rows:
        # One row, one record of what it would write. The paths where a row
        # means several objects read the whole record back; the flat ones do
        # not, and are unaffected.
        runner.begin_row()
        try:
            outcome = apply_row(runner, row, preview=True)
        except (
            ForwardDependencySkipError,
            ForwardQueryError,
            ForwardSearchError,
            ForwardSyncDataError,
        ):
            # An unusable row is a defect in the row, not a difference between
            # the two systems, so it is counted apart from drift rather than
            # inflating it.
            #
            # `ForwardQueryError` joined this list with the routing models,
            # which are the first to raise it during classification - an
            # unparseable ASN, an empty `afi_safi`, an unsupported address
            # family. It is NOT a subclass of `ForwardDataError`, so it was
            # escaping this loop and would have killed the whole comparison
            # rather than the row. `apply_model_rows` catches it per row
            # alongside the other three and continues, so counting it rejected
            # is what the apply does with the same row.
            counts["rejected"] += 1
            continue
        except (KeyError, ValueError):
            # KeyError: a row missing a key the apply indexes directly
            # (`row["device"]`). ValueError: a row that yields no usable
            # coalesce lookup, which `coalesce_update_or_create` refuses.
            # Both are rows the apply cannot process, so both are defects in
            # the row rather than differences between the two systems -
            # calling either zero drift would be the confident-zero failure
            # this feature exists to prevent.
            counts["rejected"] += 1
            continue
        if outcome in uncomparable_outcomes:
            # This row is a change the count keys cannot express - a delete,
            # for the one path that has them. Declining the whole model keeps
            # its upper bound, which is honest; folding it into creates or
            # updates would be counted twice against the separate delete
            # accounting, and unchanged would be a confident zero.
            return None
        if outcome is False or outcome is None:
            counts["rejected"] += 1
            continue
        if outcome not in counts:
            # An outcome this loop does not recognise must not be silently
            # dropped into "unchanged". Refusing the whole model is the honest
            # answer, and the caller falls back to the upper bound for it.
            return None
        counts[outcome] += 1
    return counts


def _compare_extras_taggeditem(runner, rows):
    from .sync_interface import apply_extras_taggeditem

    return _compare_adapter_rows(runner, rows, apply_extras_taggeditem)


def _compare_dcim_cable(runner, rows):
    from .sync_cable import apply_dcim_cable

    return _compare_adapter_rows(runner, rows, apply_dcim_cable)


def _compare_netbox_dlm(model_string, apply_function):
    """Build a comparison for one netbox-dlm sub-model."""

    def compare(runner, rows):
        return _compare_adapter_rows(runner, rows, apply_function)

    compare.__name__ = f"_compare_{model_string.replace('.', '_')}"
    return compare


def _dlm_comparisons():
    """The netbox-dlm sub-models that can be compared, and those that cannot.

    Five of the seven are wired. `inventoryitemsoftware` and
    `inventoryitemroleplatform` are NOT: they resolve through
    `_lookup_inventory_item` and `ensure_dlm_inventory_item_role_platform`,
    which have their own dependency chains that have not been audited for the
    writes-behind-a-runner-call trap. Absence from the mapping is the
    documented "no comparison" answer, so they keep their upper bound.
    """
    from .sync_dlm import apply_netbox_dlm_cve
    from .sync_dlm import apply_netbox_dlm_devicesoftware
    from .sync_dlm import apply_netbox_dlm_hardwarenotice
    from .sync_dlm import apply_netbox_dlm_softwareversion
    from .sync_dlm import apply_netbox_dlm_vulnerability

    return {
        "netbox_dlm.softwareversion": apply_netbox_dlm_softwareversion,
        "netbox_dlm.hardwarenotice": apply_netbox_dlm_hardwarenotice,
        "netbox_dlm.devicesoftware": apply_netbox_dlm_devicesoftware,
        "netbox_dlm.cve": apply_netbox_dlm_cve,
        "netbox_dlm.vulnerability": apply_netbox_dlm_vulnerability,
    }


def _compare_netbox_routing_peering(model_string, apply_function):
    """Build a comparison for one netbox-routing peering sub-model."""

    def compare(runner, rows):
        return _compare_adapter_rows(runner, rows, apply_function)

    compare.__name__ = f"_compare_{model_string.replace('.', '_')}"
    return compare


def _peering_comparisons():
    """The seven netbox-routing models, which share one row loop.

    They do NOT share one verdict rule, and the split is the substance of these
    two slices - see `preview_routing_outcome` against `preview_leaf_outcome`.

    Named for peering because that is the slice that built it.

    All four funnel into `ensure_netbox_routing_bgppeer` or the address-family
    pair beneath it, and every write in those chains is either behind a
    `runner.` call the preview overrides or behind the `preview` argument
    threaded through `sync_routing_impl`. So they classify identically and
    share one builder rather than four that could drift apart.

    `netbox_peering_manager.peeringsession` belongs here rather than with its
    own plugin: it has no chain of its own, only the BGP peer beneath it.
    """
    from .sync_routing_impl import apply_netbox_peering_manager_peeringsession
    from .sync_routing_impl import apply_netbox_routing_bgpaddressfamily
    from .sync_routing_impl import apply_netbox_routing_bgppeer
    from .sync_routing_impl import apply_netbox_routing_bgppeeraddressfamily
    from .sync_routing_impl import apply_netbox_routing_ospfarea
    from .sync_routing_impl import apply_netbox_routing_ospfinstance
    from .sync_routing_impl import apply_netbox_routing_ospfinterface

    return {
        "netbox_routing.bgppeer": apply_netbox_routing_bgppeer,
        "netbox_routing.bgpaddressfamily": apply_netbox_routing_bgpaddressfamily,
        "netbox_routing.bgppeeraddressfamily": (
            apply_netbox_routing_bgppeeraddressfamily
        ),
        "netbox_peering_manager.peeringsession": (
            apply_netbox_peering_manager_peeringsession
        ),
        "netbox_routing.ospfinstance": apply_netbox_routing_ospfinstance,
        "netbox_routing.ospfarea": apply_netbox_routing_ospfarea,
        "netbox_routing.ospfinterface": apply_netbox_routing_ospfinterface,
    }


def _compare_dcim_module(runner, rows):
    from .sync_inventory_module import apply_dcim_module

    return _compare_adapter_rows(runner, rows, apply_dcim_module)


def _compare_ipam_fhrpgroup(runner, rows):
    from .sync_ipam import apply_ipam_fhrpgroup

    return _compare_adapter_rows(runner, rows, apply_ipam_fhrpgroup)


def _compare_dcim_inventoryitem(runner, rows):
    """Compare inventory items, unless the batch contains a deletion.

    A module-native row on a deployment with `dcim.module` enabled is DELETED
    by the apply, not upserted, and this comparison's contract has no slot for
    a delete - the report reads drift as `creates + updates` and accounts for
    deletes separately. Rather than fold it into either bucket, one such row
    declines the whole model, which keeps its honest upper bound.

    The refusal is scoped to batches that actually contain one, so deployments
    without module-native inventory still get a real measurement. That is the
    whole reason it is detected per row rather than per deployment.
    """
    from .sync_inventory_module import MODULE_NATIVE_ROW_NOT_COMPARABLE
    from .sync_inventory_module import apply_dcim_inventoryitem

    counts = _compare_adapter_rows(
        runner,
        rows,
        apply_dcim_inventoryitem,
        uncomparable_outcomes=(MODULE_NATIVE_ROW_NOT_COMPARABLE,),
    )
    return counts


# Adapter-only models that can be compared, and the function that does it.
#
# Absence from this mapping is the "no comparison" answer for a model, which is
# what every adapter model gave before this: the caller keeps its upper-bound
# estimate rather than reporting a zero nothing measured. Adding a model here
# means auditing its `runner.` calls, not just grepping it for ORM writes - the
# writes that matter in these paths hide behind `_ensure_*` and `_upsert_*`,
# and in `dcim.cable`'s case inside NetBox core's own `Cable.save()`.
_ADAPTER_COMPARISONS = {
    "extras.taggeditem": _compare_extras_taggeditem,
    "dcim.cable": _compare_dcim_cable,
    "dcim.inventoryitem": _compare_dcim_inventoryitem,
    "ipam.fhrpgroup": _compare_ipam_fhrpgroup,
    "dcim.module": _compare_dcim_module,
}


# netbox-dlm is registered lazily: its apply functions import the optional
# plugin's models, so importing them at module scope would break every
# deployment that does not have it installed.
def _register_dlm_comparisons():
    for model_string, apply_function in _dlm_comparisons().items():
        _ADAPTER_COMPARISONS.setdefault(
            model_string, _compare_netbox_dlm(model_string, apply_function)
        )


# netbox-routing is registered lazily for the same reason netbox-dlm is.
def _register_peering_comparisons():
    for model_string, apply_function in _peering_comparisons().items():
        _ADAPTER_COMPARISONS.setdefault(
            model_string,
            _compare_netbox_routing_peering(model_string, apply_function),
        )


def _aci_comparisons():
    """The eight netbox-cisco-aci models, which share one row loop.

    Slice nine, and the one #206 never named: the ACI maps postdate it. Every
    write in these chains is behind `runner._upsert_values_from_defaults`,
    which the preview overrides, and every lookup is `_get_unique_or_raise` or
    `_lookup_device_by_name`, which only read - the audit is
    `grep -n "objects\\.\\(create\\|get_or_create\\|update_or_create\\)\\|\\.save()"
    sync_aci.py` returning nothing. The verdict rule is the LEAF rule: each of
    these models has its own query and its own rows, so a parent create is the
    parent model's drift, not the child's.

    What the chains needed was a guard, not a shim: a parent the preview
    reports as absent must short-circuit the child to a create, because the
    coalesce lookup drops the `None` parent and would otherwise match a sibling
    under another tenant. See `_parent_absent` in `sync_aci`.
    """
    from .sync_aci import apply_netbox_cisco_aci_acibridgedomain
    from .sync_aci import apply_netbox_cisco_aci_acifabric
    from .sync_aci import apply_netbox_cisco_aci_acifilter
    from .sync_aci import apply_netbox_cisco_aci_acil3out
    from .sync_aci import apply_netbox_cisco_aci_acinode
    from .sync_aci import apply_netbox_cisco_aci_acipod
    from .sync_aci import apply_netbox_cisco_aci_acitenant
    from .sync_aci import apply_netbox_cisco_aci_acivrf

    return {
        "netbox_cisco_aci.acifabric": apply_netbox_cisco_aci_acifabric,
        "netbox_cisco_aci.acipod": apply_netbox_cisco_aci_acipod,
        "netbox_cisco_aci.acinode": apply_netbox_cisco_aci_acinode,
        "netbox_cisco_aci.acitenant": apply_netbox_cisco_aci_acitenant,
        "netbox_cisco_aci.acivrf": apply_netbox_cisco_aci_acivrf,
        "netbox_cisco_aci.acibridgedomain": apply_netbox_cisco_aci_acibridgedomain,
        "netbox_cisco_aci.acifilter": apply_netbox_cisco_aci_acifilter,
        "netbox_cisco_aci.acil3out": apply_netbox_cisco_aci_acil3out,
    }


# netbox-cisco-aci is registered lazily for the same reason the others are.
# The builder is the peering one: it is a plain row loop with no verdict logic
# of its own, and the ACI apply functions carry the leaf rule themselves.
def _register_aci_comparisons():
    for model_string, apply_function in _aci_comparisons().items():
        _ADAPTER_COMPARISONS.setdefault(
            model_string,
            _compare_netbox_routing_peering(model_string, apply_function),
        )


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
    # `netbox_dlm.softwareversion` was adapter-only and had no comparison when
    # this was written (it has since been wired up); a deployment reporting 45
    # Forward rows for it still reached this line with an empty row list, took
    # the shortcut, and the drift report showed it measured and `In sync: Yes`
    # - the only affirmative claim on the page, made by the one branch that
    # never looked at NetBox. The `netbox_cisco_aci.*` models are in that
    # position now.
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
    if model_string.startswith("netbox_dlm.") and model_string not in (
        _ADAPTER_COMPARISONS
    ):
        _register_dlm_comparisons()
    if (
        model_string.startswith("netbox_routing.")
        or model_string.startswith("netbox_peering_manager.")
    ) and model_string not in _ADAPTER_COMPARISONS:
        _register_peering_comparisons()
    if model_string.startswith("netbox_cisco_aci.") and model_string not in (
        _ADAPTER_COMPARISONS
    ):
        _register_aci_comparisons()
    adapter_comparison = _ADAPTER_COMPARISONS.get(model_string)
    if adapter_comparison is not None:
        return adapter_comparison(runner, rows)
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
