from ..exceptions import ForwardQueryError
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
        delete_rows = []

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
                delete_rows.append(before)
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
                    delete_rows.append(before)
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
        if upsert_rows and delete_rows:
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
                delete_rows = [
                    row
                    for row in delete_rows
                    if self._coalesce_identity(model_string, row, coalesce_sets)
                    not in upsert_identities
                ]

        return upsert_rows, delete_rows
