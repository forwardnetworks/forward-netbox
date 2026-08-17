from ..exceptions import ForwardQueryError
from .full_removal_reconciliation import diff_removals_allowed
from .sync_contracts import canonical_cable_endpoint_identity
from .sync_contracts import row_coalesce_field_is_complete
from .sync_reporting import (
    emit_aggregated_conflict_warning_summaries as sync_emit_aggregated_conflict_warning_summaries,
)
from .sync_reporting import (
    emit_aggregated_skip_warning_summaries as sync_emit_aggregated_skip_warning_summaries,
)
from .sync_reporting import (
    ipaddress_assignment_skip_reason as sync_ipaddress_assignment_skip_reason,
)
from .sync_reporting import (
    record_aggregated_conflict_warning as sync_record_aggregated_conflict_warning,
)
from .sync_reporting import (
    record_aggregated_skip_warning as sync_record_aggregated_skip_warning,
)


class ForwardSyncRunnerContractMixin:
    def _record_aggregated_conflict_warning(
        self, *, model_string, reason, warning_message
    ):
        return sync_record_aggregated_conflict_warning(
            self,
            model_string=model_string,
            reason=reason,
            warning_message=warning_message,
        )

    def _emit_aggregated_conflict_warning_summaries(self, model_string):
        return sync_emit_aggregated_conflict_warning_summaries(self, model_string)

    def _record_aggregated_skip_warning(
        self, *, model_string, reason, warning_message, sample=None
    ):
        return sync_record_aggregated_skip_warning(
            self,
            model_string=model_string,
            reason=reason,
            warning_message=warning_message,
            sample=sample,
        )

    def _emit_aggregated_skip_warning_summaries(self, model_string):
        return sync_emit_aggregated_skip_warning_summaries(self, model_string)

    def _conflict_policy(self, model_string):
        return self.MODEL_CONFLICT_POLICIES.get(model_string, "strict")

    def _is_module_native_inventory_row(self, row):
        if row.get("module_component") is True:
            return True
        return row.get("part_type") in self.MODULE_NATIVE_INVENTORY_PART_TYPES

    def _ipaddress_assignment_skip_reason(self, address):
        return sync_ipaddress_assignment_skip_reason(address)

    def _first_complete_coalesce_set(self, row, coalesce_sets):
        for field_set in coalesce_sets:
            if all(
                row_coalesce_field_is_complete(
                    "",
                    row,
                    field,
                )
                for field in field_set
            ):
                return tuple(field_set)
        return None

    def _coalesce_identity(self, model_string, row, coalesce_sets):
        if model_string == "dcim.cable":
            canonical_identity = canonical_cable_endpoint_identity(row)
            if canonical_identity is not None:
                return ("canonical_cable_endpoints", canonical_identity)
        field_set = self._first_complete_coalesce_set_for_model(
            model_string,
            row,
            coalesce_sets,
        )
        if field_set is None:
            return None
        return (
            field_set,
            tuple((field, row.get(field)) for field in field_set),
        )

    def _first_complete_coalesce_set_for_model(self, model_string, row, coalesce_sets):
        for field_set in coalesce_sets:
            if all(
                row_coalesce_field_is_complete(model_string, row, field)
                for field in field_set
            ):
                return tuple(field_set)
        return None

    def _split_diff_rows(self, model_string, diff_rows):
        coalesce_sets = self._model_coalesce_fields.get(model_string, [])
        upsert_rows = []
        # `(row, superseded)` pairs. `superseded` marks a delete produced
        # because a MODIFIED row's identity key changed, which the removal
        # policy below treats differently from a plain DELETED row - and which
        # nothing downstream could work out on its own.
        delete_entries = []

        for diff_row in diff_rows:
            change_type = diff_row.get("type")
            before = diff_row.get("before")
            after = diff_row.get("after")

            if change_type == "ADDED":
                if not isinstance(after, dict):
                    raise ForwardQueryError(
                        f"Forward diff row for {model_string} was missing `after` data for ADDED."
                    )
                upsert_rows.append(after)
                continue

            if change_type == "DELETED":
                if not isinstance(before, dict):
                    raise ForwardQueryError(
                        f"Forward diff row for {model_string} was missing `before` data for DELETED."
                    )
                delete_entries.append((before, False))
                continue

            if change_type == "MODIFIED":
                if not isinstance(before, dict) or not isinstance(after, dict):
                    raise ForwardQueryError(
                        f"Forward diff row for {model_string} was missing `before`/`after` data for MODIFIED."
                    )
                upsert_rows.append(after)
                if self._coalesce_identity(
                    model_string, before, coalesce_sets
                ) != self._coalesce_identity(model_string, after, coalesce_sets):
                    delete_entries.append((before, True))
                continue

            raise ForwardQueryError(
                f"Forward diff row for {model_string} had unsupported type `{change_type}`."
            )

        # Never delete an entity we are simultaneously upserting in the same diff
        # batch. When several Forward rows collapse onto one NetBox identity (e.g.
        # ipam.fhrpgroup: many (device, interface, state) rows map to one
        # (protocol, group_id, address, vrf) group), a volatile non-identity field
        # like HSRP state flapping active<->standby makes the snapshot diff emit an
        # ADDED variant and a DELETED variant for the SAME group. Routing the
        # DELETED side to a delete would drop the group we just re-created, causing
        # perpetual create/delete churn run after run. Drop those deletes.
        if upsert_rows and delete_entries:
            upsert_identities = {
                identity
                for row in upsert_rows
                if (
                    identity := self._coalesce_identity(
                        model_string, row, coalesce_sets
                    )
                )
                is not None
            }
            if upsert_identities:
                delete_entries = [
                    (row, superseded)
                    for row, superseded in delete_entries
                    if self._coalesce_identity(model_string, row, coalesce_sets)
                    not in upsert_identities
                ]

        # The removal allowlist, applied where the rows are produced - the same
        # place the baseline path applies its own. Both call sites of this
        # function get the filter, and so does any third one.
        #
        # Only DELETED rows are subject to it. A `MODIFIED` row whose identity
        # key changed is a RENAME, and the two are not the same claim at all:
        #
        #   DELETED   - the row was in the before-snapshot result and is not in
        #               the after-snapshot result. A device disabled in Forward,
        #               a failed collection and a narrowed query all produce
        #               this, so it is not evidence the object is gone. This is
        #               the one that reached `delete()` on a customer's sites.
        #   MODIFIED  - Forward reports the SAME object under a new identity,
        #     (rename)  and the after-side is being upserted in this very batch.
        #               The before-row is superseded by a row we are writing.
        #               Refusing it does not preserve anything; it strands a
        #               duplicate that nothing will ever collect, which is the
        #               orphaning this machinery exists to prevent.
        #
        # Getting this wrong was caught by an end-to-end test that renames a
        # site and asserts the old row is gone. Blanket refusal left it behind
        # for good.
        refused = not diff_removals_allowed(model_string)
        held_back = (
            [row for row, superseded in delete_entries if not superseded]
            if refused
            else []
        )
        delete_rows = [
            row for row, superseded in delete_entries if superseded or not refused
        ]

        # Held back, not dropped quietly: an operator who expects a stale row to
        # disappear needs to be told why it did not, and "the removal policy
        # refuses this model" is the answer. The message names the model rather
        # than any row, so it carries no customer data.
        if held_back:
            self.logger.log_warning(
                f"Held back {len(held_back)} Forward-diff delete(s) for "
                f"{model_string}: the removal policy does not auto-delete this "
                "model when Forward stops reporting a row. Devices and sites "
                "are removed through Scope Reconciliation -> Prune orphans; "
                "shared catalogues and global IPAM are not removed by a sync at "
                "all. The rows stay in NetBox and remain visible as drift. "
                "Renames are unaffected.",
                obj=self.sync,
            )

        return upsert_rows, delete_rows
