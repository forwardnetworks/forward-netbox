# Version-pinned COPY/SQL reconciliation for branch-staged MAC addresses.
# This is intentionally the executable dcim.macaddress contract, not a generic
# raw-DML framework. Conditions outside the exact contract in
# apply_engine_decision are routed to the existing engine first.
import hashlib
import json
import logging
from collections.abc import Iterable

from django.conf import settings
from django.contrib.contenttypes.fields import GenericRelation
from django.db import connections
from django.db import transaction
from netbox_branching.contextvars import active_branch
from psycopg.types.json import Jsonb
from rq.timeouts import JobTimeoutException

from .apply_engine_bulk import _canonical_mac


logger = logging.getLogger("forward_netbox.sync")

COPY_SQL_MODEL_STRING = "dcim.macaddress"
_CONFLICT_FIELD_ORDER = (
    "owner",
    "description",
    "comments",
    "mac_address",
    "assigned_object_type",
    "assigned_object_id",
    "custom_fields",
    "tags",
)


class CopySQLRollbackInvariantError(RuntimeError):
    """Raised when a failed COPY/SQL attempt left observable database state."""


def _inject_copy_sql_fault(stage):
    """No-op test hook for transaction-boundary fault injection."""


def _quote(connection, value):
    return connection.ops.quote_name(value)


def _qualified(connection, schema, table):
    return f"{_quote(connection, schema)}.{_quote(connection, table)}"


def _datetime_json_sql(column):
    return (
        "CASE WHEN {column} IS NULL THEN NULL ELSE "
        "to_char({column} AT TIME ZONE 'UTC', 'YYYY-MM-DD\"T\"HH24:MI:SS') || "
        "CASE WHEN mod(EXTRACT(MICROSECONDS FROM {column})::bigint, 1000000) = 0 "
        "THEN '' ELSE '.' || to_char({column} AT TIME ZONE 'UTC', 'MS') END || "
        "'Z' END"
    ).format(column=column)


def _mac_payload_sql(
    *, target, tagged_item, tag, content_type_id, skip_last_updated=False
):
    created = _datetime_json_sql(f"{target}.created")
    last_updated = _datetime_json_sql(f"{target}.last_updated")
    tags = (
        "COALESCE((SELECT jsonb_agg(t.name ORDER BY t.name) "
        f"FROM {tagged_item} ti JOIN {tag} t ON t.id = ti.tag_id "
        f"WHERE ti.content_type_id = {int(content_type_id)} "
        f"AND ti.object_id = {target}.id), '[]'::jsonb)"
    )
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
            'tags', {tags}
        )
    """
    if skip_last_updated:
        return f"({payload}) - 'last_updated'"
    return payload


def _clean_payload_sql(full_payload):
    return f"({full_payload}) - ARRAY['created', 'last_updated']"


def _runtime_preflight(*, branch, content_type):
    from core.models import ObjectChange
    from django.contrib.contenttypes.models import ContentType
    from extras.models import CustomField
    from extras.models import EventRule
    from netbox.config import get_config
    from netbox.context import current_request

    request = current_request.get()
    if request is None or getattr(request, "user", None) is None:
        return False, "request_with_user_required"
    if getattr(request, "id", None) is None:
        return False, "request_id_required"
    config = get_config()
    validators = getattr(config, "CUSTOM_VALIDATORS", {}) or getattr(
        settings, "CUSTOM_VALIDATORS", {}
    )
    for model_name, configured in getattr(validators, "items", lambda: ())():
        if str(model_name).lower() == COPY_SQL_MODEL_STRING and configured:
            return False, "dynamic_custom_validator_present"
    protection_rules = getattr(config, "PROTECTION_RULES", {}) or {}
    for model_name, configured in getattr(protection_rules, "items", lambda: ())():
        if str(model_name).lower() == COPY_SQL_MODEL_STRING and configured:
            return False, "dynamic_protection_rule_present"
    # NetBox 4.7 returns a list here, not a queryset: no `.exists()`.
    if CustomField.objects.get_for_model(content_type.model_class()):
        return False, "custom_field_definition_present"
    object_change_type = ContentType.objects.get_for_model(ObjectChange)
    if EventRule.objects.filter(
        enabled=True,
        object_types__in=(content_type, object_change_type),
    ).exists():
        return False, "enabled_event_rule_present"
    connection = connections[branch.connection_name]
    if connection.vendor != "postgresql":
        return False, "postgresql_required"
    return True, "eligible"


def _copy_stage(cursor, upsert_rows, delete_rows):
    cursor.execute(
        """
        CREATE TEMPORARY TABLE fnb_stage_raw (
            row_ord bigint PRIMARY KEY,
            requested_action char(1) NOT NULL,
            raw_row jsonb NOT NULL,
            device_name text,
            interface_name text,
            mac_text text,
            normalized_mac text
        ) ON COMMIT DROP
        """
    )
    copy_sql = """
        COPY fnb_stage_raw (
            row_ord, requested_action, raw_row, device_name,
            interface_name, mac_text, normalized_mac
        ) FROM STDIN
    """
    with cursor.copy(copy_sql) as copy:
        row_ord = 0
        for action, rows in (("U", upsert_rows), ("D", delete_rows)):
            for row in rows:
                row_ord += 1
                raw = dict(row)
                mac_value = raw.get("mac") or raw.get("mac_address")
                normalized = (
                    _canonical_mac(mac_value) if mac_value not in (None, "") else None
                )
                copy.write_row(
                    (
                        row_ord,
                        action,
                        Jsonb(raw),
                        raw.get("device"),
                        raw.get("interface"),
                        mac_value,
                        normalized,
                    )
                )


def _create_resolved_stage(cursor, *, interface_content_type_id):
    cursor.execute(
        """
        CREATE TEMPORARY TABLE fnb_stage_resolved ON COMMIT DROP AS
        WITH device_matches AS MATERIALIZED (
            SELECT
                s.row_ord,
                count(d.id)::bigint AS match_count,
                min(d.id) AS target_id
            FROM fnb_stage_raw s
            LEFT JOIN dcim_device d
              ON s.requested_action = 'U' AND d.name = s.device_name
            GROUP BY s.row_ord
        ),
        interface_matches AS MATERIALIZED (
            SELECT
                s.row_ord,
                count(i.id)::bigint AS match_count,
                min(i.id) AS target_id
            FROM fnb_stage_raw s
            JOIN device_matches d ON d.row_ord = s.row_ord
            LEFT JOIN dcim_interface i
              ON s.requested_action = 'U'
             AND d.match_count = 1
             AND i.device_id = d.target_id
             AND i.name = s.interface_name
            GROUP BY s.row_ord
        ),
        requested_macs AS MATERIALIZED (
            SELECT DISTINCT normalized_mac
            FROM fnb_stage_raw
            WHERE normalized_mac IS NOT NULL
        ),
        mac_matches AS MATERIALIZED (
            SELECT
                requested.normalized_mac,
                count(m.id)::bigint AS match_count,
                min(m.id) AS target_id
            FROM requested_macs requested
            LEFT JOIN dcim_macaddress m
              ON upper(m.mac_address::text) = requested.normalized_mac
            GROUP BY requested.normalized_mac
        )
        SELECT
            s.*,
            COALESCE(d.match_count, 0)::bigint AS device_match_count,
            d.target_id AS device_id,
            COALESCE(i.match_count, 0)::bigint AS interface_match_count,
            i.target_id AS interface_id,
            COALESCE(m.match_count, 0)::bigint AS target_match_count,
            m.target_id,
            (COALESCE(m.match_count, 0) = 1) AS target_existed,
            CASE
                WHEN s.normalized_mac IS NULL THEN 'adapter'
                WHEN s.requested_action = 'U' AND (
                    s.device_name IS NULL OR s.device_name = '' OR
                    s.interface_name IS NULL OR s.interface_name = ''
                ) THEN 'adapter'
                WHEN s.requested_action = 'U' AND COALESCE(d.match_count, 0) <> 1
                    THEN 'adapter'
                WHEN s.requested_action = 'U' AND COALESCE(i.match_count, 0) <> 1
                    THEN 'adapter'
                WHEN COALESCE(m.match_count, 0) > 1 THEN 'adapter'
                ELSE 'accept'
            END::text AS disposition,
            CASE
                WHEN s.normalized_mac IS NULL THEN 'missing_mac_identity'
                WHEN s.requested_action = 'U' AND (
                    s.device_name IS NULL OR s.device_name = '' OR
                    s.interface_name IS NULL OR s.interface_name = ''
                ) THEN 'missing_required_field'
                WHEN s.requested_action = 'U' AND COALESCE(d.match_count, 0) = 0
                    THEN 'missing_device'
                WHEN s.requested_action = 'U' AND COALESCE(d.match_count, 0) > 1
                    THEN 'ambiguous_device'
                WHEN s.requested_action = 'U' AND COALESCE(i.match_count, 0) = 0
                    THEN 'missing_interface'
                WHEN s.requested_action = 'U' AND COALESCE(i.match_count, 0) > 1
                    THEN 'ambiguous_interface'
                WHEN COALESCE(m.match_count, 0) > 1 THEN 'ambiguous_identity'
                ELSE NULL
            END::text AS reason_code
        FROM fnb_stage_raw s
        JOIN device_matches d ON d.row_ord = s.row_ord
        JOIN interface_matches i ON i.row_ord = s.row_ord
        LEFT JOIN mac_matches m ON m.normalized_mac = s.normalized_mac
        """
    )
    cursor.execute("ALTER TABLE fnb_stage_resolved ADD PRIMARY KEY (row_ord)")
    cursor.execute(
        "CREATE INDEX ON fnb_stage_resolved (normalized_mac, requested_action)"
    )
    # Duplicate planner identities must stay in one native-engine bucket.  The
    # durable planner normally removes them; this guard preserves semantics for
    # direct/native-diff callers instead of choosing an arbitrary row in SQL.
    cursor.execute(
        """
        UPDATE fnb_stage_resolved r
        SET disposition = 'adapter', reason_code = 'duplicate_stage_identity'
        FROM (
            SELECT normalized_mac
            FROM fnb_stage_resolved
            WHERE normalized_mac IS NOT NULL
            GROUP BY normalized_mac
            HAVING count(*) > 1
        ) duplicate
        WHERE r.normalized_mac = duplicate.normalized_mac
        """
    )
    cursor.execute(
        """
        UPDATE fnb_stage_resolved
        SET disposition = 'adapter', reason_code = 'wrong_interface_content_type'
        WHERE requested_action = 'U' AND disposition = 'accept'
          AND %s IS NULL
        """,
        [interface_content_type_id],
    )


def _relation_bound_delete_ids(*, model, content_type, target_ids, using):
    """Return target IDs whose ORM delete has any observable related work."""
    target_ids = set(target_ids)
    if not target_ids:
        return set()
    bound = set()
    for field in model._meta.private_fields:
        if not isinstance(field, GenericRelation):
            continue
        queryset = field.related_model._base_manager.using(using).filter(
            **{
                f"{field.content_type_field_name}_id": content_type.pk,
                f"{field.object_id_field_name}__in": target_ids,
            }
        )
        bound.update(
            queryset.values_list(field.object_id_field_name, flat=True).distinct()
        )
    for relation in model._meta.related_objects:
        relation_field = getattr(relation, "field", None)
        if (
            relation_field is None
            or isinstance(relation_field, GenericRelation)
            or not getattr(relation_field, "attname", None)
        ):
            continue
        queryset = relation.related_model._base_manager.using(using).filter(
            **{f"{relation_field.attname}__in": target_ids}
        )
        bound.update(queryset.values_list(relation_field.attname, flat=True).distinct())
    bound.update(_primary_mac_ids(target_ids=target_ids, using=using))
    return bound


def _primary_mac_ids(*, target_ids, using):
    """Return IDs protected by either interface model's primary-MAC field."""
    from dcim.models import Interface
    from virtualization.models import VMInterface

    bound = set()
    for interface_model in (Interface, VMInterface):
        bound.update(
            interface_model._base_manager.using(using)
            .filter(primary_mac_address_id__in=target_ids)
            .values_list("primary_mac_address_id", flat=True)
            .distinct()
        )
    return bound


def _validate_accepted_rows(
    cursor,
    *,
    using,
    mac_model,
    interface_content_type,
):
    cursor.execute(
        """
        SELECT r.target_id
        FROM fnb_stage_resolved r
        JOIN dcim_macaddress m ON m.id = r.target_id
        WHERE r.disposition = 'accept' AND r.requested_action = 'U'
          AND (
              m.assigned_object_type_id IS DISTINCT FROM %s
              OR m.assigned_object_id IS DISTINCT FROM r.interface_id
          )
        """,
        [interface_content_type.pk],
    )
    primary_ids = _primary_mac_ids(
        target_ids=[row[0] for row in cursor.fetchall()],
        using=using,
    )
    if primary_ids:
        # MACAddress.clean() rejects reassignment of a primary MAC. Route those
        # rows through the native engine so it reproduces the exact row issue.
        cursor.execute(
            """
            UPDATE fnb_stage_resolved
            SET disposition = 'adapter', reason_code = 'primary_mac_reassignment'
            WHERE target_id = ANY(%s) AND requested_action = 'U'
            """,
            [list(primary_ids)],
        )
    cursor.execute(
        """
        SELECT r.row_ord, r.raw_row, r.target_id, r.interface_id, r.normalized_mac
        FROM fnb_stage_resolved r
        LEFT JOIN dcim_macaddress m ON m.id = r.target_id
        WHERE r.disposition = 'accept' AND r.requested_action = 'U'
          AND (
              r.target_id IS NULL
              OR m.assigned_object_type_id IS DISTINCT FROM %s
              OR m.assigned_object_id IS DISTINCT FROM r.interface_id
          )
        ORDER BY row_ord
        """,
        [interface_content_type.pk],
    )
    rows = cursor.fetchall()
    mac_field = mac_model._meta.get_field("mac_address")
    invalid_row_ords = []
    for row_ord, _raw_row, _target_id, _interface_id, normalized_mac in rows:
        # The exact allowlisted model has no unique/model constraints. Its
        # persisted-state validation consists of custom fields and primary-MAC
        # reassignment, both resolved above or in preflight. Other writable
        # columns are engine-owned constants/dependencies. Run the authoritative
        # MACAddressField validation for every actual create/update candidate.
        instance = mac_model(mac_address=normalized_mac)
        try:
            mac_field.clean(normalized_mac, instance)
        except JobTimeoutException:
            raise
        except Exception:  # validation details are reproduced by native fallback
            invalid_row_ords.append(row_ord)
    if invalid_row_ords:
        cursor.execute(
            """
            UPDATE fnb_stage_resolved
            SET disposition = 'adapter', reason_code = 'model_validation'
            WHERE row_ord = ANY(%s)
            """,
            [invalid_row_ords],
        )


def _route_relation_bound_deletes(cursor, *, model, content_type, using):
    cursor.execute(
        """
        SELECT DISTINCT target_id
        FROM fnb_stage_resolved
        WHERE disposition = 'accept' AND requested_action = 'D'
          AND target_id IS NOT NULL
        """
    )
    target_ids = [row[0] for row in cursor.fetchall()]
    bound_ids = _relation_bound_delete_ids(
        model=model,
        content_type=content_type,
        target_ids=target_ids,
        using=using,
    )
    if bound_ids:
        cursor.execute(
            """
            UPDATE fnb_stage_resolved
            SET disposition = 'adapter', reason_code = 'delete_side_effects'
            WHERE target_id = ANY(%s) AND requested_action = 'D'
            """,
            [list(bound_ids)],
        )


def _create_delta(
    cursor,
    *,
    connection,
    main_schema,
    content_type_id,
    interface_content_type_id,
    skip_last_updated,
):
    target = _qualified(connection, main_schema, "dcim_macaddress")
    cursor.execute("SELECT pg_get_serial_sequence(%s, 'id')", [target])
    sequence_name = cursor.fetchone()[0]
    if not sequence_name:
        raise RuntimeError("Unable to resolve dcim.macaddress primary-key sequence.")
    cursor.execute(
        """
        UPDATE fnb_stage_resolved
        SET target_id = nextval(%s), target_existed = FALSE
        WHERE disposition = 'accept' AND requested_action = 'U'
          AND target_id IS NULL
        """,
        [sequence_name],
    )
    branch_tagged_item = _quote(connection, "extras_taggeditem")
    branch_tag = _quote(connection, "extras_tag")
    pre_full = _mac_payload_sql(
        target="m",
        tagged_item=branch_tagged_item,
        tag=branch_tag,
        content_type_id=content_type_id,
        skip_last_updated=skip_last_updated,
    )
    pre_clean = _clean_payload_sql(pre_full)
    cursor.execute(
        f"""
        CREATE TEMPORARY TABLE fnb_delta ON COMMIT DROP AS
        SELECT
            r.row_ord,
            r.requested_action,
            CASE
                WHEN r.requested_action = 'D' AND r.target_existed THEN 'D'
                WHEN r.requested_action = 'D' THEN 'N'
                WHEN NOT r.target_existed THEN 'I'
                WHEN m.assigned_object_type_id IS DISTINCT FROM %s
                  OR m.assigned_object_id IS DISTINCT FROM r.interface_id THEN 'U'
                ELSE 'N'
            END::char(1) AS operation,
            r.target_id,
            r.interface_id,
            r.normalized_mac,
            CASE WHEN r.target_existed THEN {pre_full} ELSE NULL END AS pre_full,
            CASE WHEN r.target_existed THEN {pre_clean} ELSE NULL END AS pre_clean,
            NULL::jsonb AS post_full,
            NULL::jsonb AS post_clean,
            left(COALESCE(upper(m.mac_address::text), r.normalized_mac), 200)::text
                AS object_repr
        FROM fnb_stage_resolved r
        LEFT JOIN dcim_macaddress m ON m.id = r.target_id AND r.target_existed
        WHERE r.disposition = 'accept'
        """,
        [interface_content_type_id],
    )
    cursor.execute("ALTER TABLE fnb_delta ADD PRIMARY KEY (row_ord)")
    cursor.execute("CREATE INDEX ON fnb_delta (target_id)")


def _apply_target_dml(
    cursor,
    *,
    connection,
    content_type_id,
    interface_content_type_id,
    skip_last_updated,
):
    cursor.execute(
        """
        INSERT INTO dcim_macaddress (
            id, created, last_updated, custom_field_data, description, comments,
            owner_id, mac_address, assigned_object_type_id, assigned_object_id
        )
        SELECT
            target_id, statement_timestamp(), statement_timestamp(), '{}'::jsonb,
            '', '', NULL, normalized_mac::macaddr, %s, interface_id
        FROM fnb_delta
        WHERE operation = 'I'
        ORDER BY row_ord
        """,
        [interface_content_type_id],
    )
    cursor.execute(
        """
        UPDATE dcim_macaddress m
        SET assigned_object_type_id = %s,
            assigned_object_id = d.interface_id
        FROM fnb_delta d
        WHERE d.operation = 'U' AND m.id = d.target_id
        """,
        [interface_content_type_id],
    )
    cursor.execute(
        """
        DELETE FROM dcim_macaddress m
        USING fnb_delta d
        WHERE d.operation = 'D' AND m.id = d.target_id
        """
    )
    _inject_copy_sql_fault("after_target_dml")
    post_full = _mac_payload_sql(
        target="m",
        tagged_item=_quote(connection, "extras_taggeditem"),
        tag=_quote(connection, "extras_tag"),
        content_type_id=content_type_id,
        skip_last_updated=skip_last_updated,
    )
    post_clean = _clean_payload_sql(post_full)
    cursor.execute(
        f"""
        UPDATE fnb_delta d
        SET post_full = {post_full},
            post_clean = {post_clean},
            object_repr = left(upper(m.mac_address::text), 200)
        FROM dcim_macaddress m
        WHERE d.operation IN ('I', 'U') AND m.id = d.target_id
        """
    )


def _insert_object_changes(
    cursor,
    *,
    request,
    content_type_id,
):
    action_sql = (
        "CASE operation WHEN 'I' THEN 'create' WHEN 'U' THEN 'update' "
        "WHEN 'D' THEN 'delete' END"
    )
    cursor.execute(
        f"""
        INSERT INTO core_objectchange (
            time, user_id, user_name, request_id, action,
            changed_object_type_id, changed_object_id,
            related_object_type_id, related_object_id,
            object_repr, message, prechange_data, postchange_data
        )
        SELECT
            statement_timestamp(), %s, %s, %s, {action_sql},
            %s, target_id, NULL, NULL,
            left(object_repr, 200), '', pre_full, post_full
        FROM fnb_delta
        WHERE operation IN ('I', 'U', 'D')
        ORDER BY CASE operation WHEN 'I' THEN 1 WHEN 'U' THEN 2 ELSE 3 END,
                 row_ord
        """,
        [
            request.user.pk,
            getattr(request.user, "username", "") or "",
            request.id,
            content_type_id,
        ],
    )
    _inject_copy_sql_fault("after_object_changes")


def _conflicts_sql(alias="cd"):
    values = ", ".join(
        f"('{field}', {index})" for index, field in enumerate(_CONFLICT_FIELD_ORDER)
    )
    original_value = f"COALESCE({alias}.original -> fields.key, 'null'::jsonb)"
    modified_value = f"COALESCE({alias}.modified -> fields.key, 'null'::jsonb)"
    current_value = f"COALESCE({alias}.current -> fields.key, 'null'::jsonb)"
    changed_in_branch = f"{original_value} IS DISTINCT FROM {modified_value}"
    return f"""
        CASE
            WHEN {alias}.original IS NULL THEN {alias}.conflicts
            WHEN {alias}.action = 'update' AND {alias}.current IS NULL THEN (
                SELECT array_agg(fields.key ORDER BY fields.ord)
                FROM (VALUES {values}) AS fields(key, ord)
                WHERE {changed_in_branch}
            )
            WHEN {alias}.action = 'update' THEN (
                SELECT array_agg(fields.key ORDER BY fields.ord)
                FROM (VALUES {values}) AS fields(key, ord)
                WHERE {changed_in_branch}
                  AND {original_value} IS DISTINCT FROM {current_value}
                  AND {modified_value} IS DISTINCT FROM {current_value}
            )
            WHEN {alias}.action = 'delete' AND {alias}.current IS NULL
                THEN {alias}.conflicts
            WHEN {alias}.action = 'delete' THEN (
                SELECT array_agg(fields.key ORDER BY fields.ord)
                FROM (VALUES {values}) AS fields(key, ord)
                WHERE {original_value} IS DISTINCT FROM {current_value}
            )
            ELSE NULL
        END
    """


def _sync_change_diffs(
    cursor,
    *,
    connection,
    main_schema,
    branch_id,
    content_type_id,
    skip_last_updated,
):
    change_diff = _qualified(connection, main_schema, "netbox_branching_changediff")
    main_mac = _qualified(connection, main_schema, "dcim_macaddress")
    main_tagged_item = _qualified(connection, main_schema, "extras_taggeditem")
    main_tag = _qualified(connection, main_schema, "extras_tag")
    main_full = _mac_payload_sql(
        target="main_m",
        tagged_item=main_tagged_item,
        tag=main_tag,
        content_type_id=content_type_id,
        skip_last_updated=skip_last_updated,
    )
    main_clean = _clean_payload_sql(main_full)
    cursor.execute(
        "SELECT pg_advisory_xact_lock(hashtextextended(%s, 0))",
        [f"{branch_id}:{COPY_SQL_MODEL_STRING}"],
    )
    cursor.execute(
        f"""
        SELECT cd.id
        FROM {change_diff} cd
        JOIN fnb_delta d ON d.target_id = cd.object_id
        WHERE cd.branch_id = %s AND cd.object_type_id = %s
          AND d.operation IN ('I', 'U', 'D')
        FOR UPDATE
        """,
        [branch_id, content_type_id],
    )
    cursor.execute(
        f"""
        UPDATE {change_diff} cd
        SET object_repr = left(d.object_repr, 200),
            action = CASE
                WHEN cd.action = 'create' THEN cd.action
                WHEN d.operation = 'I' THEN 'create'
                WHEN d.operation = 'U' THEN 'update'
                WHEN d.operation = 'D' THEN 'delete'
            END,
            modified = d.post_clean,
            last_updated = statement_timestamp()
        FROM fnb_delta d
        WHERE cd.branch_id = %s AND cd.object_type_id = %s
          AND cd.object_id = d.target_id
          AND d.operation IN ('I', 'U', 'D')
        """,
        [branch_id, content_type_id],
    )
    _inject_copy_sql_fault("during_change_diff_update")
    cursor.execute(
        f"""
        INSERT INTO {change_diff} (
            branch_id, last_updated, object_type_id, object_id, object_repr,
            action, original, modified, current, conflicts
        )
        SELECT
            %s, statement_timestamp(), %s, d.target_id,
            left(d.object_repr, 200),
            CASE d.operation WHEN 'I' THEN 'create'
                 WHEN 'U' THEN 'update' WHEN 'D' THEN 'delete' END,
            d.pre_clean,
            d.post_clean,
            CASE
                WHEN d.operation = 'I' OR main_m.id IS NULL THEN NULL
                ELSE {main_clean}
            END,
            NULL
        FROM fnb_delta d
        LEFT JOIN {main_mac} main_m ON main_m.id = d.target_id
        WHERE d.operation IN ('I', 'U', 'D')
          AND NOT EXISTS (
              SELECT 1 FROM {change_diff} existing
              WHERE existing.branch_id = %s
                AND existing.object_type_id = %s
                AND existing.object_id = d.target_id
          )
        ORDER BY d.row_ord
        """,
        [
            branch_id,
            content_type_id,
            branch_id,
            content_type_id,
        ],
    )
    conflicts = _conflicts_sql("cd")
    cursor.execute(
        f"""
        UPDATE {change_diff} cd
        SET conflicts = {conflicts}
        WHERE cd.branch_id = %s AND cd.object_type_id = %s
          AND EXISTS (
              SELECT 1 FROM fnb_delta d
              WHERE d.target_id = cd.object_id
                AND d.operation IN ('I', 'U', 'D')
          )
        """,
        [branch_id, content_type_id],
    )
    _inject_copy_sql_fault("after_change_diffs")


def _fingerprint(*, branch, content_type, normalized_macs, request_id):
    from core.models import ObjectChange
    from dcim.models import MACAddress
    from netbox_branching.models import ChangeDiff

    using = branch.connection_name
    target_queryset = MACAddress.objects.using(using)
    if normalized_macs:
        # Bypass MACAddressField RHS coercion so deliberately invalid planner
        # rows can still participate in the rollback fingerprint.
        target_queryset = target_queryset.extra(
            where=["upper(dcim_macaddress.mac_address::text) = ANY(%s)"],
            params=[list(normalized_macs)],
        )
    else:
        target_queryset = target_queryset.none()
    targets = list(target_queryset.order_by("pk").values())
    target_ids = [row["id"] for row in targets]
    related = {}
    if target_ids:
        for field in MACAddress._meta.private_fields:
            if not isinstance(field, GenericRelation):
                continue
            rows = list(
                field.related_model._base_manager.using(using)
                .filter(
                    **{
                        f"{field.content_type_field_name}_id": content_type.pk,
                        f"{field.object_id_field_name}__in": target_ids,
                    }
                )
                .order_by("pk")
                .values()
            )
            related[field.related_model._meta.label_lower] = rows
    object_changes = list(
        ObjectChange.objects.using(using)
        .filter(request_id=request_id, changed_object_type=content_type)
        .order_by("pk")
        .values()
    )
    change_diffs = list(
        ChangeDiff.objects.using(using)
        .filter(branch=branch, object_type=content_type)
        .order_by("pk")
        .values()
    )
    payload = json.dumps(
        {
            "targets": targets,
            "related": related,
            "object_changes": object_changes,
            "change_diffs": change_diffs,
        },
        sort_keys=True,
        default=str,
        separators=(",", ":"),
    )
    return hashlib.sha256(payload.encode()).hexdigest()


def _apply_copy_sql_mac(
    *,
    runner,
    upsert_rows,
    delete_rows,
    fallback_engine,
):
    from dcim.models import Interface
    from dcim.models import MACAddress
    from netbox.context import current_request
    from netbox.plugins import get_plugin_config

    from .sync_reporting import _increment_ingestion_delete_totals

    branch = active_branch.get()
    using = branch.connection_name
    connection = connections[using]
    content_type = runner._content_type_for(MACAddress)
    interface_content_type = runner._content_type_for(Interface)
    eligible, reason = _runtime_preflight(
        branch=branch,
        content_type=content_type,
    )
    if not eligible:
        runner.logger.log_info(
            f"COPY/SQL runtime preflight rejected dcim.macaddress ({reason}); "
            "using the existing engine.",
            obj=runner.sync,
        )
        return fallback_engine.apply_plan_item(
            runner, COPY_SQL_MODEL_STRING, upsert_rows, delete_rows
        )

    main_schema = get_plugin_config("netbox_branching", "main_schema") or "public"
    from netbox.config import get_config

    skip_last_updated = bool(get_config().CHANGELOG_SKIP_EMPTY_CHANGES)
    request = current_request.get()
    normalized_macs = {
        _canonical_mac(row.get("mac") or row.get("mac_address"))
        for row in [*upsert_rows, *delete_rows]
        if (row.get("mac") or row.get("mac_address")) not in (None, "")
    }
    before = _fingerprint(
        branch=branch,
        content_type=content_type,
        normalized_macs=normalized_macs,
        request_id=request.id,
    )
    try:
        with transaction.atomic(using=using):
            with connection.cursor() as cursor:
                cursor.execute("SELECT current_schema()")
                if cursor.fetchone()[0] != branch.schema_name:
                    raise RuntimeError("Branch connection search_path is not active.")
                _copy_stage(cursor, upsert_rows, delete_rows)
                cursor.execute(
                    """
                    SELECT m.id
                    FROM dcim_macaddress m
                    WHERE upper(m.mac_address::text) IN (
                        SELECT normalized_mac FROM fnb_stage_raw
                        WHERE normalized_mac IS NOT NULL
                    )
                    FOR UPDATE
                    """
                )
                _create_resolved_stage(
                    cursor,
                    interface_content_type_id=interface_content_type.pk,
                )
                _validate_accepted_rows(
                    cursor,
                    using=using,
                    mac_model=MACAddress,
                    interface_content_type=interface_content_type,
                )
                _route_relation_bound_deletes(
                    cursor,
                    model=MACAddress,
                    content_type=content_type,
                    using=using,
                )
                _create_delta(
                    cursor,
                    connection=connection,
                    main_schema=main_schema,
                    content_type_id=content_type.pk,
                    interface_content_type_id=interface_content_type.pk,
                    skip_last_updated=skip_last_updated,
                )
                _apply_target_dml(
                    cursor,
                    connection=connection,
                    content_type_id=content_type.pk,
                    interface_content_type_id=interface_content_type.pk,
                    skip_last_updated=skip_last_updated,
                )
                _insert_object_changes(
                    cursor,
                    request=request,
                    content_type_id=content_type.pk,
                )
                _sync_change_diffs(
                    cursor,
                    connection=connection,
                    main_schema=main_schema,
                    branch_id=branch.pk,
                    content_type_id=content_type.pk,
                    skip_last_updated=skip_last_updated,
                )
                cursor.execute(
                    """
                    SELECT requested_action, operation, count(*)
                    FROM fnb_delta
                    GROUP BY requested_action, operation
                    """
                )
                operation_counts = {
                    (requested_action, operation): count
                    for requested_action, operation, count in cursor.fetchall()
                }
                cursor.execute(
                    """
                    SELECT requested_action, raw_row
                    FROM fnb_stage_resolved
                    WHERE disposition = 'adapter'
                    ORDER BY row_ord
                    """
                )
                fallback_rows = [
                    (
                        action,
                        (
                            json.loads(raw_row)
                            if isinstance(raw_row, (str, bytes, bytearray))
                            else raw_row
                        ),
                    )
                    for action, raw_row in cursor.fetchall()
                ]
    except JobTimeoutException:
        raise
    except Exception as exc:  # noqa: BLE001 - atomic rollback then verified fallback
        after = _fingerprint(
            branch=branch,
            content_type=content_type,
            normalized_macs=normalized_macs,
            request_id=request.id,
        )
        if after != before:
            raise CopySQLRollbackInvariantError(
                "COPY/SQL failed and rollback fingerprint changed; fallback blocked."
            ) from exc
        runner.logger.log_warning(
            "COPY/SQL dcim.macaddress transaction rolled back cleanly "
            f"({type(exc).__name__}); using the existing engine.",
            obj=runner.sync,
        )
        request_token = current_request.set(request)
        try:
            return fallback_engine.apply_plan_item(
                runner, COPY_SQL_MODEL_STRING, upsert_rows, delete_rows
            )
        finally:
            current_request.reset(request_token)

    applied = int(operation_counts.get(("U", "I"), 0)) + int(
        operation_counts.get(("U", "U"), 0)
    )
    unchanged = int(operation_counts.get(("U", "N"), 0))
    deleted = int(operation_counts.get(("D", "D"), 0))
    skipped_deletes = int(operation_counts.get(("D", "N"), 0))
    if applied:
        runner.logger.increment_statistics(
            COPY_SQL_MODEL_STRING, outcome="applied", amount=applied
        )
    if unchanged:
        runner.logger.increment_statistics(
            COPY_SQL_MODEL_STRING, outcome="unchanged", amount=unchanged
        )
    if deleted:
        runner.logger.increment_statistics(
            COPY_SQL_MODEL_STRING, outcome="applied", amount=deleted
        )
        _increment_ingestion_delete_totals(runner, deleted)
    if skipped_deletes:
        runner.logger.increment_statistics(
            COPY_SQL_MODEL_STRING, outcome="skipped", amount=skipped_deletes
        )
    for _ in range(applied + deleted + skipped_deletes):
        runner.events_clearer.increment()
    runner.events_clearer.clear()

    fallback_upserts = [row for action, row in fallback_rows if action == "U"]
    fallback_deletes = [row for action, row in fallback_rows if action == "D"]
    logger.info(
        "COPY/SQL dcim.macaddress committed operations=%s; native fallback "
        "upserts=%s deletes=%s.",
        operation_counts,
        len(fallback_upserts),
        len(fallback_deletes),
    )
    if fallback_upserts or fallback_deletes:
        request_token = current_request.set(request)
        try:
            fallback_engine.apply_plan_item(
                runner,
                COPY_SQL_MODEL_STRING,
                fallback_upserts,
                fallback_deletes,
            )
        finally:
            current_request.reset(request_token)
    return True


def copy_sql_apply_plan_item(
    *,
    runner,
    model_string,
    upsert_rows: Iterable[dict],
    delete_rows: Iterable[dict],
    fallback_engine,
):
    """Apply exactly the planner-approved upsert/delete delta."""
    upsert_rows = list(upsert_rows)
    delete_rows = list(delete_rows)
    if model_string != COPY_SQL_MODEL_STRING:
        return fallback_engine.apply_plan_item(
            runner, model_string, upsert_rows, delete_rows
        )
    if active_branch.get() is None:
        return fallback_engine.apply_plan_item(
            runner, model_string, upsert_rows, delete_rows
        )
    if not upsert_rows and not delete_rows:
        return True
    return _apply_copy_sql_mac(
        runner=runner,
        upsert_rows=upsert_rows,
        delete_rows=delete_rows,
        fallback_engine=fallback_engine,
    )
