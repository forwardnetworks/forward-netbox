# Version-pinned set-based merge for the Forward MAC assignment contract.
# This module is deliberately model-specific, not a generic raw-DML merge
# framework: anything outside the exact dcim.macaddress contract returns to the
# existing ObjectChange/bulk merge path before mutation.
import hashlib
import json
import logging
from dataclasses import dataclass
from dataclasses import field as dataclass_field
from importlib.metadata import PackageNotFoundError
from importlib.metadata import version as distribution_version

from django.conf import settings
from django.contrib.contenttypes.fields import GenericRel
from django.contrib.contenttypes.fields import GenericRelation
from django.db import connections
from django.db import DEFAULT_DB_ALIAS
from django.db import transaction
from psycopg.types.json import Jsonb
from rq.timeouts import JobTimeoutException

from .bulk_delete import lock_related_writes_for_delete
from .version_series import series_matches
from .validated_runtime import VALIDATED_OPTIONAL_DISTRIBUTIONS
from .validated_runtime import VALIDATED_PLUGIN_APPS

logger = logging.getLogger("forward_netbox.bulk_merge")


SET_BASED_MERGE_MODEL_SPEC_VERSIONS = {"dcim.macaddress": 1}
SET_BASED_MERGE_ALLOWED_MODELS = frozenset(SET_BASED_MERGE_MODEL_SPEC_VERSIONS)
SET_BASED_MERGE_SUPPORTED_NETBOX_SERIES = "4.6"
SET_BASED_MERGE_SUPPORTED_BRANCHING_SERIES = "1.1"
# Derived from the single validated-runtime declaration; see
# `validated_runtime` for why these are no longer written out per engine.
SET_BASED_MERGE_SUPPORTED_OPTIONAL_DISTRIBUTIONS = VALIDATED_OPTIONAL_DISTRIBUTIONS
SET_BASED_MERGE_SUPPORTED_PLUGIN_APPS = VALIDATED_PLUGIN_APPS
SET_BASED_MAC_MODEL = "dcim.macaddress"
_MAC_PAYLOAD_FIELDS = frozenset(
    {
        "custom_fields",
        "description",
        "comments",
        "owner",
        "mac_address",
        "assigned_object_type",
        "assigned_object_id",
        "tags",
    }
)
_MAC_FAST_UPDATE_FIELDS = frozenset({"assigned_object_type", "assigned_object_id"})
_EXPECTED_MAC_SIGNAL_RECEIVERS = {
    "pre_save": frozenset(),
    "post_save": frozenset(
        {
            "core.signals.handle_changed_object",
            "extras.signals.notify_object_changed",
            "netbox.denormalized.update_denormalized_fields",
            "netbox.search.backends.SearchBackend.caching_handler",
        }
    ),
    "pre_delete": frozenset(
        {
            "core.signals.handle_deleted_object",
            "extras.signals.notify_object_changed",
        }
    ),
    "post_delete": frozenset({"netbox.search.backends.SearchBackend.removal_handler"}),
}


class SetBasedMergeRollbackInvariantError(RuntimeError):
    """A failed SQL attempt changed observable target or evidence state."""


@dataclass(frozen=True)
class SetBasedMergeDecision:
    enabled: bool
    reason_code: str
    context: dict


@dataclass(frozen=True)
class SetBasedMergeRangeResult:
    applied: tuple
    fallback: tuple
    operation_counts: dict
    fallback_reason_counts: dict = dataclass_field(default_factory=dict)


@dataclass(frozen=True)
class _PreparedMACChange:
    row_ord: int
    collapsed: object
    object_id: int
    action: str
    pre_clean: dict | None
    post_clean: dict | None
    desired_mac: str | None
    desired_content_type_id: int | None
    desired_object_id: int | None
    change_content_type: bool
    change_object_id: bool
    payload_hash: str


def _inject_set_based_merge_fault(stage):
    """No-op test hook for transaction-boundary fault injection."""


def _enabled(parameters, key):
    value = parameters.get(key, False)
    if isinstance(value, str):
        return value.strip().lower() in {"true", "1", "yes", "on"}
    return bool(value)


def _kill_switches(parameters):
    values = parameters.get("set_based_merge_kill_switches") or []
    if isinstance(values, str):
        values = [part.strip() for part in values.split(",")]
    if not isinstance(values, (list, tuple, set, frozenset)):
        return frozenset()
    return frozenset(str(value).strip() for value in values if str(value).strip())


def _installed_distribution_version(distribution):
    try:
        return distribution_version(distribution)
    except PackageNotFoundError:
        return None


def _receiver_identities(signal, sender):
    sync_receivers, async_receivers = signal._live_receivers(sender)
    return frozenset(
        f"{receiver.__module__}.{receiver.__qualname__}"
        for receiver in (*sync_receivers, *async_receivers)
    )


def set_based_merge_runtime_version_tuple():
    """Return the exact runtime tuple consumed by merge selection."""
    release = getattr(settings, "RELEASE", None)
    netbox_version = getattr(release, "version", None) or getattr(
        settings, "VERSION", ""
    )
    return (
        str(netbox_version or ""),
        _installed_distribution_version("netboxlabs-netbox-branching"),
        tuple(
            (distribution, _installed_distribution_version(distribution))
            for distribution in sorted(SET_BASED_MERGE_SUPPORTED_OPTIONAL_DISTRIBUTIONS)
        ),
    )


def _runtime_tuple_decision():
    netbox_version, branching_version, optional_versions = (
        set_based_merge_runtime_version_tuple()
    )
    if not series_matches(netbox_version, SET_BASED_MERGE_SUPPORTED_NETBOX_SERIES):
        return SetBasedMergeDecision(
            False,
            "unsupported_netbox_version",
            {
                "expected": f"{SET_BASED_MERGE_SUPPORTED_NETBOX_SERIES}.x",
                "actual": netbox_version,
            },
        )
    if not series_matches(
        branching_version, SET_BASED_MERGE_SUPPORTED_BRANCHING_SERIES
    ):
        return SetBasedMergeDecision(
            False,
            "unsupported_branching_version",
            {
                "expected": f"{SET_BASED_MERGE_SUPPORTED_BRANCHING_SERIES}.x",
                "actual": branching_version,
            },
        )
    for distribution, actual in optional_versions:
        expected = SET_BASED_MERGE_SUPPORTED_OPTIONAL_DISTRIBUTIONS[distribution]
        if actual not in expected:
            return SetBasedMergeDecision(
                False,
                "unsupported_optional_plugin_version",
                {
                    "distribution": distribution,
                    "expected": expected,
                    "actual": actual,
                },
            )
    actual_apps = frozenset(getattr(settings, "PLUGINS", ()) or ())
    if actual_apps != SET_BASED_MERGE_SUPPORTED_PLUGIN_APPS:
        return SetBasedMergeDecision(
            False,
            "unsupported_plugin_app_tuple",
            {
                "expected": sorted(SET_BASED_MERGE_SUPPORTED_PLUGIN_APPS),
                "actual": sorted(actual_apps),
            },
        )
    return SetBasedMergeDecision(
        True,
        "supported_exact_runtime_tuple",
        {
            "netbox": netbox_version,
            "branching": branching_version,
            "optional_plugins": dict(optional_versions),
        },
    )


def set_based_merge_decision(*, sync, branch, model_string=SET_BASED_MAC_MODEL):
    """Fail-closed selection and runtime-hook preflight for one merge."""
    parameters = dict(getattr(sync, "parameters", {}) or {})
    if not _enabled(parameters, "enable_set_based_merge"):
        return SetBasedMergeDecision(False, "disabled_by_default", {})
    if model_string not in SET_BASED_MERGE_ALLOWED_MODELS:
        return SetBasedMergeDecision(
            False,
            "model_not_allowlisted",
            {"allowlist": sorted(SET_BASED_MERGE_ALLOWED_MODELS)},
        )
    if model_string in _kill_switches(parameters):
        return SetBasedMergeDecision(False, "model_kill_switch", {})
    runtime = _runtime_tuple_decision()
    if not runtime.enabled:
        return runtime
    if branch is None or getattr(branch, "pk", None) is None:
        return SetBasedMergeDecision(False, "provisioned_branch_required", {})

    connection = connections[DEFAULT_DB_ALIAS]
    if connection.vendor != "postgresql":
        return SetBasedMergeDecision(False, "postgresql_required", {})

    from core.models import ObjectChange
    from dcim.models import MACAddress
    from django.contrib.contenttypes.models import ContentType
    from django.db.models.signals import post_delete
    from django.db.models.signals import post_save
    from django.db.models.signals import pre_delete
    from django.db.models.signals import pre_save
    from extras.models import CustomField
    from extras.models import EventRule
    from netbox.config import get_config
    from netbox.denormalized import registry as denormalized_registry
    from netbox.search import get_indexer

    expected_columns = {
        "id",
        "created",
        "last_updated",
        "custom_field_data",
        "description",
        "comments",
        "mac_address",
        "assigned_object_id",
        "assigned_object_type_id",
        "owner_id",
    }
    actual_columns = {
        field.column for field in MACAddress._meta.concrete_fields if field.column
    }
    if actual_columns != expected_columns:
        return SetBasedMergeDecision(
            False,
            "model_schema_mismatch",
            {"expected": sorted(expected_columns), "actual": sorted(actual_columns)},
        )
    content_type = ContentType.objects.get_for_model(MACAddress)
    if CustomField.objects.get_for_model(MACAddress).exists():
        return SetBasedMergeDecision(False, "custom_field_definition_present", {})
    config = get_config()
    validators = getattr(config, "CUSTOM_VALIDATORS", {}) or getattr(
        settings, "CUSTOM_VALIDATORS", {}
    )
    if any(
        str(name).lower() == model_string and configured
        for name, configured in getattr(validators, "items", lambda: ())()
    ):
        return SetBasedMergeDecision(False, "dynamic_custom_validator_present", {})
    protection_rules = getattr(config, "PROTECTION_RULES", {}) or {}
    if any(
        str(name).lower() == model_string and configured
        for name, configured in getattr(protection_rules, "items", lambda: ())()
    ):
        return SetBasedMergeDecision(False, "dynamic_protection_rule_present", {})
    object_change_type = ContentType.objects.get_for_model(ObjectChange)
    if EventRule.objects.filter(
        enabled=True,
        object_types__in=(content_type, object_change_type),
    ).exists():
        return SetBasedMergeDecision(False, "enabled_event_rule_present", {})
    if (getattr(branch, "migrators", {}) or {}).get(model_string):
        return SetBasedMergeDecision(False, "objectchange_field_migrator_present", {})
    signal_receivers = {
        "pre_save": _receiver_identities(pre_save, MACAddress),
        "post_save": _receiver_identities(post_save, MACAddress),
        "pre_delete": _receiver_identities(pre_delete, MACAddress),
        "post_delete": _receiver_identities(post_delete, MACAddress),
    }
    if signal_receivers != _EXPECTED_MAC_SIGNAL_RECEIVERS:
        return SetBasedMergeDecision(
            False,
            "unexpected_model_signal_receivers",
            {
                "expected": {
                    name: sorted(receivers)
                    for name, receivers in _EXPECTED_MAC_SIGNAL_RECEIVERS.items()
                },
                "actual": {
                    name: sorted(receivers)
                    for name, receivers in signal_receivers.items()
                },
            },
        )
    if denormalized_registry["denormalized_fields"].get(MACAddress):
        return SetBasedMergeDecision(False, "denormalized_fields_present", {})
    try:
        indexer = get_indexer(MACAddress)
    except KeyError:
        return SetBasedMergeDecision(False, "expected_search_index_missing", {})
    indexer_identity = f"{indexer.__module__}.{indexer.__qualname__}"
    if indexer_identity != "dcim.search.MACAddressIndex" or tuple(indexer.fields) != (
        ("mac_address", 100),
        ("description", 500),
    ):
        return SetBasedMergeDecision(
            False,
            "search_index_contract_mismatch",
            {
                "indexer": indexer_identity,
                "fields": list(indexer.fields),
            },
        )
    return SetBasedMergeDecision(
        True,
        "set_based_mac_spec_enabled",
        {
            **runtime.context,
            "model": model_string,
            "model_spec_version": SET_BASED_MERGE_MODEL_SPEC_VERSIONS[model_string],
        },
    )


def _clean_payload(value):
    if not value:
        return None
    return {
        key: item
        for key, item in dict(value).items()
        if key not in {"created", "last_updated"}
    } or None


def _changed_fields(pre_clean, post_clean):
    pre_clean = pre_clean or {}
    post_clean = post_clean or {}
    return {
        key
        for key in set(pre_clean) | set(post_clean)
        if pre_clean.get(key) != post_clean.get(key)
    }


def _int_or_none(value):
    if value in (None, ""):
        return None
    if isinstance(value, bool):
        raise ValueError("Boolean is not a valid object identity.")
    return int(value)


def _prepare_mac_change(collapsed, *, row_ord, interface_content_type_id, mac_field):
    action = getattr(collapsed.final_action, "value", collapsed.final_action)
    if action not in {"create", "update", "delete"}:
        return None
    pre_clean = _clean_payload(collapsed.prechange_data)
    post_clean = _clean_payload(collapsed.postchange_data)
    desired = post_clean or {}
    desired_mac = desired.get("mac_address") or (pre_clean or {}).get("mac_address")
    change_content_type = False
    change_object_id = False

    if action == "create":
        if not desired or set(desired) - _MAC_PAYLOAD_FIELDS:
            return None
        if desired.get("custom_fields") not in (None, {}) or desired.get(
            "tags"
        ) not in (
            None,
            [],
        ):
            return None
        if desired.get("owner") is not None:
            return None
        if (desired.get("description") or "") or (desired.get("comments") or ""):
            return None
        if not desired_mac:
            return None
        try:
            desired_mac = str(mac_field.clean(desired_mac, mac_field.model())).upper()
        except JobTimeoutException:
            raise
        except Exception:
            return None
        change_content_type = True
        change_object_id = True
    elif action == "update":
        changed = _changed_fields(pre_clean, post_clean)
        if changed - _MAC_FAST_UPDATE_FIELDS:
            return None
        change_content_type = "assigned_object_type" in changed
        change_object_id = "assigned_object_id" in changed
    else:
        desired = pre_clean or {}

    try:
        desired_content_type_id = _int_or_none(desired.get("assigned_object_type"))
        desired_object_id = _int_or_none(desired.get("assigned_object_id"))
    except (TypeError, ValueError):
        return None
    if (desired_content_type_id is None) != (desired_object_id is None):
        return None
    if (
        desired_content_type_id is not None
        and desired_content_type_id != interface_content_type_id
    ):
        return None

    encoded = json.dumps(
        {
            "object_id": collapsed.key[1],
            "action": action,
            "pre": pre_clean,
            "post": post_clean,
        },
        sort_keys=True,
        separators=(",", ":"),
        default=str,
    ).encode()
    return _PreparedMACChange(
        row_ord=row_ord,
        collapsed=collapsed,
        object_id=int(collapsed.key[1]),
        action=action,
        pre_clean=pre_clean,
        post_clean=post_clean,
        desired_mac=desired_mac,
        desired_content_type_id=desired_content_type_id,
        desired_object_id=desired_object_id,
        change_content_type=change_content_type,
        change_object_id=change_object_id,
        payload_hash=hashlib.sha256(encoded).hexdigest(),
    )


def _datetime_json_sql(column):
    return (
        "CASE WHEN {column} IS NULL THEN NULL ELSE "
        "to_char({column} AT TIME ZONE 'UTC', 'YYYY-MM-DD\"T\"HH24:MI:SS') || "
        "CASE WHEN mod(EXTRACT(MICROSECONDS FROM {column})::bigint, 1000000) = 0 "
        "THEN '' ELSE '.' || to_char({column} AT TIME ZONE 'UTC', 'MS') END || "
        "'Z' END"
    ).format(column=column)


def _mac_payload_sql(*, target, content_type_id, skip_last_updated=False):
    created = _datetime_json_sql(f"{target}.created")
    last_updated = _datetime_json_sql(f"{target}.last_updated")
    payload = f"""
        jsonb_build_object(
            'created', {created},
            'last_updated', {last_updated},
            'custom_fields', {target}.custom_field_data,
            'description', {target}.description,
            'comments', {target}.comments,
            'owner', {target}.owner_id,
            'mac_address', upper({target}.mac_address::text),
            'assigned_object_type', {target}.assigned_object_type_id,
            'assigned_object_id', {target}.assigned_object_id,
            'tags', COALESCE((
                SELECT jsonb_agg(t.name ORDER BY t.name)
                FROM extras_taggeditem ti
                JOIN extras_tag t ON t.id = ti.tag_id
                WHERE ti.content_type_id = {int(content_type_id)}
                  AND ti.object_id = {target}.id
            ), '[]'::jsonb)
        )
    """
    if skip_last_updated:
        return f"({payload}) - 'last_updated'"
    return payload


def _clean_payload_sql(full_payload):
    return f"({full_payload}) - ARRAY['created', 'last_updated']"


def _copy_stage(cursor, prepared):
    cursor.execute(
        """
        CREATE TEMPORARY TABLE fnb_merge_mac_stage (
            row_ord bigint PRIMARY KEY,
            object_id bigint NOT NULL,
            action char(1) NOT NULL,
            pre_clean jsonb,
            post_clean jsonb,
            desired_mac text,
            desired_content_type_id bigint,
            desired_object_id bigint,
            change_content_type boolean NOT NULL,
            change_object_id boolean NOT NULL,
            payload_hash char(64) NOT NULL,
            disposition text NOT NULL DEFAULT 'accept',
            reason_code text
        ) ON COMMIT DROP
        """
    )
    with cursor.copy(
        """
        COPY fnb_merge_mac_stage (
            row_ord, object_id, action, pre_clean, post_clean, desired_mac,
            desired_content_type_id, desired_object_id, change_content_type,
            change_object_id, payload_hash
        ) FROM STDIN
        """
    ) as copy:
        for item in prepared:
            copy.write_row(
                (
                    item.row_ord,
                    item.object_id,
                    {"create": "I", "update": "U", "delete": "D"}[item.action],
                    Jsonb(item.pre_clean) if item.pre_clean is not None else None,
                    Jsonb(item.post_clean) if item.post_clean is not None else None,
                    item.desired_mac,
                    item.desired_content_type_id,
                    item.desired_object_id,
                    item.change_content_type,
                    item.change_object_id,
                    item.payload_hash,
                )
            )


def _lock_and_classify(
    cursor,
    *,
    branch_id,
    content_type_id,
    interface_content_type_id,
    mac_model,
):
    cursor.execute(
        "SELECT pg_advisory_xact_lock(hashtextextended(%s, 0))",
        [f"{branch_id}:{SET_BASED_MAC_MODEL}"],
    )
    cursor.execute(
        """
        SELECT cd.id
        FROM netbox_branching_changediff cd
        JOIN fnb_merge_mac_stage s
          ON s.object_id = cd.object_id
        WHERE cd.branch_id = %s AND cd.object_type_id = %s
        ORDER BY cd.id
        FOR UPDATE
        """,
        [branch_id, content_type_id],
    )
    cursor.execute(
        """
        WITH evidence AS MATERIALIZED (
            SELECT
                s.row_ord,
                count(cd.id)::bigint AS evidence_count,
                bool_and(
                    cd.action = CASE s.action
                        WHEN 'I' THEN 'create'
                        WHEN 'U' THEN 'update'
                        WHEN 'D' THEN 'delete'
                    END
                    AND cd.original IS NOT DISTINCT FROM s.pre_clean
                    AND cd.modified IS NOT DISTINCT FROM s.post_clean
                ) AS exact_match
            FROM fnb_merge_mac_stage s
            LEFT JOIN netbox_branching_changediff cd
              ON cd.branch_id = %s
             AND cd.object_type_id = %s
             AND cd.object_id = s.object_id
            GROUP BY s.row_ord
        )
        UPDATE fnb_merge_mac_stage s
        SET disposition = 'fallback', reason_code = 'source_changediff_mismatch'
        FROM evidence e
        WHERE e.row_ord = s.row_ord
          AND (e.evidence_count <> 1 OR e.exact_match IS NOT TRUE)
        """,
        [branch_id, content_type_id],
    )
    cursor.execute(
        """
        SELECT m.id
        FROM dcim_macaddress m
        WHERE m.id IN (
            SELECT object_id FROM fnb_merge_mac_stage
            WHERE disposition = 'accept'
        )
        ORDER BY m.id
        FOR UPDATE
        """
    )
    cursor.execute(
        """
        UPDATE fnb_merge_mac_stage s
        SET disposition = 'fallback', reason_code = CASE
            WHEN s.action = 'I' THEN 'existing_create_requires_lineage'
            ELSE 'concurrent_main_delete'
        END
        WHERE s.disposition = 'accept'
          AND ((s.action = 'I' AND EXISTS (
              SELECT 1 FROM dcim_macaddress m WHERE m.id = s.object_id
          )) OR (s.action = 'U' AND NOT EXISTS (
              SELECT 1 FROM dcim_macaddress m WHERE m.id = s.object_id
          )))
        """
    )
    cursor.execute(
        """
        SELECT i.id
        FROM dcim_interface i
        WHERE i.id IN (
            SELECT desired_object_id
            FROM fnb_merge_mac_stage
            WHERE disposition = 'accept'
              AND desired_content_type_id = %s
        )
        ORDER BY i.id
        FOR KEY SHARE
        """,
        [interface_content_type_id],
    )
    cursor.execute(
        """
        UPDATE fnb_merge_mac_stage s
        SET disposition = 'fallback', reason_code = 'missing_gfk_dependency'
        WHERE s.disposition = 'accept'
          AND s.desired_content_type_id = %s
          AND NOT EXISTS (
              SELECT 1 FROM dcim_interface i WHERE i.id = s.desired_object_id
          )
        """,
        [interface_content_type_id],
    )

    cursor.execute(
        """
        SELECT object_id
        FROM fnb_merge_mac_stage
        WHERE disposition = 'accept'
          AND (action = 'D' OR (
              action = 'U' AND (change_content_type OR change_object_id)
          ))
        ORDER BY object_id
        """
    )
    update_or_delete_ids = [row[0] for row in cursor.fetchall()]
    if update_or_delete_ids:
        from dcim.models import Interface
        from virtualization.models import VMInterface

        # This exact write barrier covers primary-MAC races and prevents a new
        # Subscription from appearing after the notification-safety check.
        lock_related_writes_for_delete(mac_model, using=DEFAULT_DB_ALIAS)
        cursor.execute(
            """
            UPDATE fnb_merge_mac_stage s
            SET disposition = 'fallback',
                reason_code = 'notification_side_effects'
            WHERE s.disposition = 'accept'
              AND s.action = 'U'
              AND EXISTS (
                  SELECT 1
                  FROM extras_subscription subscription
                  WHERE subscription.object_type_id = %s
                    AND subscription.object_id = s.object_id
              )
            """,
            [content_type_id],
        )
        primary_ids = set()
        for interface_model in (Interface, VMInterface):
            primary_ids.update(
                interface_model._base_manager.using(DEFAULT_DB_ALIAS)
                .filter(primary_mac_address_id__in=update_or_delete_ids)
                .values_list("primary_mac_address_id", flat=True)
            )
        if primary_ids:
            cursor.execute(
                """
                UPDATE fnb_merge_mac_stage
                SET disposition = 'fallback', reason_code = 'primary_mac_semantics'
                WHERE disposition = 'accept' AND object_id = ANY(%s)
                """,
                [list(primary_ids)],
            )

    cursor.execute(
        """
        SELECT object_id
        FROM fnb_merge_mac_stage
        WHERE disposition = 'accept' AND action = 'D'
        ORDER BY object_id
        """
    )
    delete_ids = [row[0] for row in cursor.fetchall()]
    if delete_ids:
        bound_ids = _relation_bound_delete_ids(
            model=mac_model,
            content_type_id=content_type_id,
            target_ids=delete_ids,
        )
        if bound_ids:
            cursor.execute(
                """
                UPDATE fnb_merge_mac_stage
                SET disposition = 'fallback', reason_code = 'delete_side_effects'
                WHERE disposition = 'accept' AND object_id = ANY(%s)
                """,
                [list(bound_ids)],
            )


def _relation_bound_delete_ids(*, model, content_type_id, target_ids):
    target_ids = set(target_ids)
    bound = set()
    for field in model._meta.private_fields:
        if not isinstance(field, GenericRelation):
            continue
        queryset = field.related_model._base_manager.using(DEFAULT_DB_ALIAS).filter(
            **{
                f"{field.content_type_field_name}_id": content_type_id,
                f"{field.object_id_field_name}__in": target_ids,
            }
        )
        bound.update(
            queryset.values_list(field.object_id_field_name, flat=True).distinct()
        )
    for relation in model._meta.related_objects:
        # Reverse GenericRelation metadata describes the Interface/VMInterface
        # owner of this MAC's GFK. Deleting the MAC does not mutate that owner;
        # the concrete GFK columns are already captured in the MAC payload.
        if isinstance(relation, GenericRel):
            continue
        relation_field = getattr(relation, "field", None)
        if relation_field is None or not getattr(relation_field, "attname", None):
            continue
        queryset = relation.related_model._base_manager.using(DEFAULT_DB_ALIAS).filter(
            **{f"{relation_field.attname}__in": target_ids}
        )
        bound.update(queryset.values_list(relation_field.attname, flat=True).distinct())
    return bound


def _cascade_dependency_fallback(cursor, *, prepared, python_fallback):
    """Prevent an accepted row from overtaking a rejected dependency."""
    cursor.execute(
        """
        SELECT row_ord
        FROM fnb_merge_mac_stage
        WHERE disposition = 'fallback'
        """
    )
    fallback_ords = {row[0] for row in cursor.fetchall()}
    fallback_keys = {change.key for change in python_fallback} | {
        item.collapsed.key for item in prepared if item.row_ord in fallback_ords
    }
    dependency_fallback_ords = set()
    changed = True
    while changed:
        changed = False
        for item in prepared:
            if item.row_ord in fallback_ords | dependency_fallback_ords:
                continue
            if set(getattr(item.collapsed, "depends_on", ()) or ()) & fallback_keys:
                dependency_fallback_ords.add(item.row_ord)
                fallback_keys.add(item.collapsed.key)
                changed = True
    if dependency_fallback_ords:
        cursor.execute(
            """
            UPDATE fnb_merge_mac_stage
            SET disposition = 'fallback',
                reason_code = 'dependency_requires_current_path'
            WHERE disposition = 'accept' AND row_ord = ANY(%s)
            """,
            [list(dependency_fallback_ords)],
        )


def _create_delta(cursor, *, content_type_id, skip_last_updated):
    pre_full = _mac_payload_sql(
        target="m",
        content_type_id=content_type_id,
        skip_last_updated=skip_last_updated,
    )
    pre_clean = _clean_payload_sql(pre_full)
    cursor.execute(
        f"""
        CREATE TEMPORARY TABLE fnb_merge_mac_delta ON COMMIT DROP AS
        SELECT
            s.*,
            CASE
                WHEN s.action = 'I' THEN 'I'
                WHEN s.action = 'D' AND m.id IS NOT NULL THEN 'D'
                WHEN s.action = 'D' THEN 'N'
                WHEN s.action = 'U' AND (
                    (s.change_content_type AND
                     m.assigned_object_type_id IS DISTINCT FROM
                        s.desired_content_type_id) OR
                    (s.change_object_id AND
                     m.assigned_object_id IS DISTINCT FROM s.desired_object_id)
                ) THEN 'U'
                ELSE 'N'
            END::char(1) AS operation,
            CASE WHEN m.id IS NOT NULL THEN {pre_full} ELSE NULL END AS pre_full,
            CASE WHEN m.id IS NOT NULL THEN {pre_clean} ELSE NULL END AS pre_state,
            NULL::jsonb AS post_full,
            NULL::jsonb AS post_state,
            left(COALESCE(upper(m.mac_address::text), s.desired_mac), 200)::text
                AS object_repr
        FROM fnb_merge_mac_stage s
        LEFT JOIN dcim_macaddress m ON m.id = s.object_id
        WHERE s.disposition = 'accept'
        """
    )
    cursor.execute("ALTER TABLE fnb_merge_mac_delta ADD PRIMARY KEY (row_ord)")
    cursor.execute("CREATE INDEX ON fnb_merge_mac_delta (object_id)")


def _apply_target_dml(
    cursor,
    *,
    content_type_id,
    skip_last_updated,
):
    cursor.execute(
        """
        INSERT INTO dcim_macaddress (
            id, created, last_updated, custom_field_data, description, comments,
            owner_id, mac_address, assigned_object_type_id, assigned_object_id
        )
        SELECT
            object_id, statement_timestamp(), statement_timestamp(), '{}'::jsonb,
            '', '', NULL, desired_mac::macaddr,
            desired_content_type_id, desired_object_id
        FROM fnb_merge_mac_delta
        WHERE operation = 'I'
        ORDER BY row_ord
        """
    )
    cursor.execute(
        """
        UPDATE dcim_macaddress m
        SET assigned_object_type_id = CASE
                WHEN d.change_content_type THEN d.desired_content_type_id
                ELSE m.assigned_object_type_id
            END,
            assigned_object_id = CASE
                WHEN d.change_object_id THEN d.desired_object_id
                ELSE m.assigned_object_id
            END,
            last_updated = statement_timestamp()
        FROM fnb_merge_mac_delta d
        WHERE d.operation = 'U' AND m.id = d.object_id
        """
    )
    cursor.execute(
        """
        DELETE FROM dcim_macaddress m
        USING fnb_merge_mac_delta d
        WHERE d.operation = 'D' AND m.id = d.object_id
        """
    )
    _inject_set_based_merge_fault("after_target_dml")
    post_full = _mac_payload_sql(
        target="m",
        content_type_id=content_type_id,
        skip_last_updated=skip_last_updated,
    )
    post_clean = _clean_payload_sql(post_full)
    cursor.execute(
        f"""
        UPDATE fnb_merge_mac_delta d
        SET post_full = {post_full},
            post_state = {post_clean},
            object_repr = left(upper(m.mac_address::text), 200)
        FROM dcim_macaddress m
        WHERE d.operation IN ('I', 'U') AND m.id = d.object_id
        """
    )


def _insert_audit_and_lineage(
    cursor,
    *,
    branch_id,
    request,
    content_type_id,
):
    # Preserve the exact 4.6.5/1.1.1 current-path contract: MAC creates and
    # material updates emit destination ObjectChange/AppliedChange rows, while
    # generic ObjectChange.apply() MAC deletes do not. The delete still updates
    # competing ChangeDiff evidence in this same transaction below.
    action_sql = (
        "CASE operation WHEN 'I' THEN 'create' WHEN 'U' THEN 'update' "
        "WHEN 'D' THEN 'delete' END"
    )
    cursor.execute(
        f"""
        WITH inserted AS (
            INSERT INTO core_objectchange (
                time, user_id, user_name, request_id, action,
                changed_object_type_id, changed_object_id,
                related_object_type_id, related_object_id,
                object_repr, message, prechange_data, postchange_data
            )
            SELECT
                statement_timestamp(), %s, %s, %s, {action_sql},
                %s, object_id, NULL, NULL,
                left(object_repr, 200), '', pre_full, post_full
            FROM fnb_merge_mac_delta
            WHERE operation IN ('I', 'U')
            ORDER BY row_ord
            RETURNING id
        )
        INSERT INTO netbox_branching_appliedchange (change_id, branch_id)
        SELECT id, %s FROM inserted
        """,
        [
            request.user.pk,
            getattr(request.user, "username", "") or "",
            request.id,
            content_type_id,
            branch_id,
        ],
    )
    _inject_set_based_merge_fault("after_audit_lineage")


def _sync_search_cache(cursor, *, content_type_id):
    """Mirror the exact MAC search signal effects of the current merge paths."""
    cursor.execute(
        """
        DELETE FROM extras_cachedvalue cached
        USING fnb_merge_mac_delta d
        WHERE cached.object_type_id = %s
          AND cached.object_id = d.object_id
          AND (d.action = 'U' OR d.operation = 'D')
        """,
        [content_type_id],
    )
    cursor.execute(
        """
        INSERT INTO extras_cachedvalue (
            id, timestamp, object_type_id, object_id,
            field, type, value, weight
        )
        SELECT
            gen_random_uuid(), statement_timestamp(), %s, m.id,
            fields.field, 'str', fields.value, fields.weight
        FROM fnb_merge_mac_delta d
        JOIN dcim_macaddress m ON m.id = d.object_id
        CROSS JOIN LATERAL (
            VALUES
                ('mac_address'::text, upper(m.mac_address::text), 100::smallint),
                ('description'::text, m.description, 500::smallint)
        ) AS fields(field, value, weight)
        WHERE d.action = 'U' AND fields.value <> ''
        ORDER BY d.row_ord, fields.field
        """,
        [content_type_id],
    )


def _sync_other_ready_change_diffs(
    cursor,
    *,
    source_branch_id,
    content_type_id,
):
    cursor.execute(
        """
        UPDATE netbox_branching_changediff cd
        SET current = d.post_state,
            last_updated = statement_timestamp()
        FROM fnb_merge_mac_delta d,
             netbox_branching_branch b
        WHERE cd.branch_id = b.id
          AND b.status = 'ready'
          AND cd.branch_id <> %s
          AND cd.object_type_id = %s
          AND cd.object_id = d.object_id
          AND d.operation IN ('I', 'U')
        """,
        [source_branch_id, content_type_id],
    )
    _inject_set_based_merge_fault("during_change_diff_update")


def _fingerprint(*, branch, content_type, target_ids, request_id):
    from core.models import ObjectChange
    from dcim.models import MACAddress
    from extras.models import CachedValue
    from netbox_branching.models import AppliedChange
    from netbox_branching.models import ChangeDiff

    target_ids = sorted(set(target_ids))
    targets = list(MACAddress.objects.filter(pk__in=target_ids).order_by("pk").values())
    related = {}
    for field in MACAddress._meta.private_fields:
        if not isinstance(field, GenericRelation):
            continue
        related[field.related_model._meta.label_lower] = list(
            field.related_model._base_manager.filter(
                **{
                    f"{field.content_type_field_name}_id": content_type.pk,
                    f"{field.object_id_field_name}__in": target_ids,
                }
            )
            .order_by("pk")
            .values()
        )
    related[CachedValue._meta.label_lower] = list(
        CachedValue.objects.filter(
            object_type=content_type,
            object_id__in=target_ids,
        )
        .order_by("pk")
        .values()
    )
    object_changes = list(
        ObjectChange.objects.filter(
            request_id=request_id,
            changed_object_type=content_type,
            changed_object_id__in=target_ids,
        )
        .order_by("pk")
        .values()
    )
    applied = list(
        AppliedChange.objects.filter(
            branch=branch,
            change__changed_object_type=content_type,
            change__changed_object_id__in=target_ids,
        )
        .order_by("pk")
        .values()
    )
    diffs = list(
        ChangeDiff.objects.filter(
            object_type=content_type,
            object_id__in=target_ids,
        )
        .order_by("pk")
        .values()
    )
    payload = json.dumps(
        {
            "targets": targets,
            "related": related,
            "object_changes": object_changes,
            "applied": applied,
            "diffs": diffs,
        },
        sort_keys=True,
        default=str,
        separators=(",", ":"),
    )
    return hashlib.sha256(payload.encode()).hexdigest()


def apply_set_based_mac_range(*, branch, collapsed_changes, request, decision):
    """Apply one ordered MAC range and return committed vs fallback changes."""
    collapsed_changes = list(collapsed_changes)
    if not collapsed_changes or not decision.enabled:
        return SetBasedMergeRangeResult((), tuple(collapsed_changes), {})
    if request is None or getattr(request, "user", None) is None:
        return SetBasedMergeRangeResult((), tuple(collapsed_changes), {})
    if getattr(request, "id", None) is None:
        return SetBasedMergeRangeResult((), tuple(collapsed_changes), {})

    from dcim.models import Interface
    from dcim.models import MACAddress
    from django.contrib.contenttypes.models import ContentType
    from netbox.config import get_config

    content_type = ContentType.objects.get_for_model(MACAddress)
    interface_content_type = ContentType.objects.get_for_model(Interface)
    mac_field = MACAddress._meta.get_field("mac_address")
    prepared = []
    python_fallback = []
    for row_ord, collapsed in enumerate(collapsed_changes, 1):
        item = _prepare_mac_change(
            collapsed,
            row_ord=row_ord,
            interface_content_type_id=interface_content_type.pk,
            mac_field=mac_field,
        )
        if item is None:
            python_fallback.append(collapsed)
        else:
            prepared.append(item)
    if not prepared:
        return SetBasedMergeRangeResult((), tuple(python_fallback), {})

    target_ids = [item.object_id for item in prepared]
    before = _fingerprint(
        branch=branch,
        content_type=content_type,
        target_ids=target_ids,
        request_id=request.id,
    )
    sql_fallback_ords = set()
    operation_counts = {}
    fallback_reason_counts = {}
    connection = connections[DEFAULT_DB_ALIAS]
    try:
        with transaction.atomic(using=DEFAULT_DB_ALIAS):
            with connection.cursor() as cursor:
                _copy_stage(cursor, prepared)
                _lock_and_classify(
                    cursor,
                    branch_id=branch.pk,
                    content_type_id=content_type.pk,
                    interface_content_type_id=interface_content_type.pk,
                    mac_model=MACAddress,
                )
                _cascade_dependency_fallback(
                    cursor,
                    prepared=prepared,
                    python_fallback=python_fallback,
                )
                _create_delta(
                    cursor,
                    content_type_id=content_type.pk,
                    skip_last_updated=bool(get_config().CHANGELOG_SKIP_EMPTY_CHANGES),
                )
                _apply_target_dml(
                    cursor,
                    content_type_id=content_type.pk,
                    skip_last_updated=bool(get_config().CHANGELOG_SKIP_EMPTY_CHANGES),
                )
                _sync_search_cache(
                    cursor,
                    content_type_id=content_type.pk,
                )
                _insert_audit_and_lineage(
                    cursor,
                    branch_id=branch.pk,
                    request=request,
                    content_type_id=content_type.pk,
                )
                _sync_other_ready_change_diffs(
                    cursor,
                    source_branch_id=branch.pk,
                    content_type_id=content_type.pk,
                )
                cursor.execute(
                    """
                    SELECT operation, count(*)
                    FROM fnb_merge_mac_delta
                    GROUP BY operation
                    """
                )
                operation_counts = {
                    operation: count for operation, count in cursor.fetchall()
                }
                cursor.execute(
                    """
                    SELECT row_ord
                    FROM fnb_merge_mac_stage
                    WHERE disposition = 'fallback'
                    ORDER BY row_ord
                    """
                )
                sql_fallback_ords = {row[0] for row in cursor.fetchall()}
                cursor.execute(
                    """
                    SELECT reason_code, count(*)
                    FROM fnb_merge_mac_stage
                    WHERE disposition = 'fallback'
                    GROUP BY reason_code
                    ORDER BY reason_code
                    """
                )
                fallback_reason_counts = {
                    reason: count for reason, count in cursor.fetchall()
                }
    except JobTimeoutException:
        raise
    except Exception as exc:  # noqa: BLE001 - verified rollback before fallback
        after = _fingerprint(
            branch=branch,
            content_type=content_type,
            target_ids=target_ids,
            request_id=request.id,
        )
        if after != before:
            raise SetBasedMergeRollbackInvariantError(
                "Set-based MAC merge failed and rollback fingerprint changed; "
                "current-path fallback is blocked."
            ) from exc
        logger.warning(
            "Set-based merge transaction for dcim.macaddress rolled back cleanly "
            "after %s; replaying the range through the current path.",
            type(exc).__name__,
        )
        return SetBasedMergeRangeResult(
            (),
            tuple(collapsed_changes),
            {},
        )

    applied = []
    fallback = list(python_fallback)
    for item in prepared:
        if item.row_ord in sql_fallback_ords:
            fallback.append(item.collapsed)
        else:
            applied.append(item.collapsed)
    order = {id(change): index for index, change in enumerate(collapsed_changes)}
    fallback.sort(key=lambda change: order[id(change)])
    logger.info(
        "Set-based merge committed dcim.macaddress operations=%s; "
        "fallback=%d reasons=%s.",
        operation_counts,
        len(fallback),
        fallback_reason_counts,
    )
    return SetBasedMergeRangeResult(
        tuple(applied),
        tuple(fallback),
        operation_counts,
        fallback_reason_counts,
    )
