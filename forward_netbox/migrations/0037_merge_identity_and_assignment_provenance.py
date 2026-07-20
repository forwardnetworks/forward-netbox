import django.db.models.deletion
import django.utils.timezone
from django.db import migrations
from django.db import models


SUPPORTED_NQE_MODELS = (
    "dcim.site",
    "dcim.manufacturer",
    "dcim.devicerole",
    "dcim.platform",
    "dcim.devicetype",
    "dcim.device",
    "dcim.virtualchassis",
    "extras.taggeditem",
    "dcim.interface",
    "dcim.cable",
    "dcim.macaddress",
    "ipam.vlan",
    "ipam.vrf",
    "ipam.prefix",
    "ipam.ipaddress",
    "ipam.fhrpgroup",
    "dcim.inventoryitem",
    "dcim.module",
    "netbox_routing.bgppeer",
    "netbox_routing.bgpaddressfamily",
    "netbox_routing.bgppeeraddressfamily",
    "netbox_routing.ospfinstance",
    "netbox_routing.ospfarea",
    "netbox_routing.ospfinterface",
    "netbox_peering_manager.peeringsession",
    "netbox_cisco_aci.acifabric",
    "netbox_cisco_aci.acipod",
    "netbox_cisco_aci.acinode",
    "netbox_cisco_aci.acitenant",
    "netbox_cisco_aci.acivrf",
    "netbox_cisco_aci.acibridgedomain",
    "netbox_cisco_aci.acifilter",
    "netbox_cisco_aci.acil3out",
    "netbox_dlm.softwareversion",
    "netbox_dlm.hardwarenotice",
    "netbox_dlm.devicesoftware",
    "netbox_dlm.cve",
    "netbox_dlm.vulnerability",
)


def supported_nqe_model_choices():
    choices = models.Q()
    for model_string in SUPPORTED_NQE_MODELS:
        app_label, model_name = model_string.split(".")
        choices |= models.Q(app_label=app_label, model=model_name)
    return choices


fake_on_branch = True


def initialize_provenance_and_remove_obsolete_state(apps, schema_editor):
    from forward_netbox.utilities.crypto import encrypt_secret
    from forward_netbox.utilities.crypto import is_encrypted

    ContentType = apps.get_model("contenttypes", "ContentType")
    ForwardSource = apps.get_model("forward_netbox", "ForwardSource")
    ForwardSync = apps.get_model("forward_netbox", "ForwardSync")
    ForwardIngestion = apps.get_model("forward_netbox", "ForwardIngestion")
    ForwardNQEMap = apps.get_model("forward_netbox", "ForwardNQEMap")
    Job = apps.get_model("core", "Job")

    sync_content_type = ContentType.objects.filter(
        app_label="forward_netbox", model="forwardsync"
    ).first()
    schedule_intervals = {}
    if sync_content_type is not None:
        schedule_rows = Job.objects.filter(
            object_type_id=sync_content_type.pk,
            name__in=["validation", "dependency preview"],
            status__in=["pending", "scheduled", "running"],
            interval__gt=0,
        ).order_by("object_id", "name", "-created", "-pk")
        for sync_id, name, interval in schedule_rows.values_list(
            "object_id", "name", "interval"
        ):
            schedule_intervals.setdefault((sync_id, name), int(interval))

    changed = []
    obsolete_keys = {
        "_branch_run",
        "_execution_progress",
        "execution_backend",
        "multi_branch",
        "scheduler_overlap",
        "bulk_orm_models",
        "enable_branch_budget_split",
        "branch_budget_enforcement",
        "auto_tag_backfilled",
        "auto_prune_orphans",
        "max_changes_per_branch",
        "netbox_cisco_aci.aciappprofile",
        "netbox_cisco_aci.aciendpointgroup",
        "netbox_cisco_aci.acicontract",
        "netbox_cisco_aci.acistaticportbinding",
    }
    for sync in (
        ForwardSync.objects.all().only("pk", "parameters", "user_id").iterator()
    ):
        parameters = dict(sync.parameters or {})
        owner_changed = False
        if sync.user_id is None and sync_content_type is not None:
            owner_id = (
                Job.objects.filter(
                    object_type_id=sync_content_type.pk,
                    object_id=sync.pk,
                    user_id__isnull=False,
                )
                .order_by("-created", "-pk")
                .values_list("user_id", flat=True)
                .first()
            )
            if owner_id is not None:
                sync.user_id = owner_id
                owner_changed = True
        legacy_budget = parameters.get("max_changes_per_branch")
        if (
            "max_changes_per_staging_item" not in parameters
            and legacy_budget is not None
        ):
            parameters["max_changes_per_staging_item"] = legacy_budget
        for key in obsolete_keys:
            parameters.pop(key, None)
        for parameter_key, job_name in (
            ("validation_schedule_interval", "validation"),
            ("preview_schedule_interval", "dependency preview"),
        ):
            parameters.setdefault(
                parameter_key,
                schedule_intervals.get((sync.pk, job_name), 0),
            )
        if parameters != (sync.parameters or {}) or owner_changed:
            sync.parameters = parameters
            changed.append(sync)
    if changed:
        ForwardSync.objects.bulk_update(
            changed,
            ["parameters", "user"],
            batch_size=2000,
        )

    changed_sources = []
    marker = "scope_endpoints_by_include_tags_configured"
    for source in ForwardSource.objects.all().only("pk", "parameters").iterator():
        parameters = dict(source.parameters or {})
        password = parameters.get("password")
        if password and not is_encrypted(password):
            parameters["password"] = encrypt_secret(password)
        include_tags = parameters.get("device_tag_include_tags") or []
        if not include_tags and parameters.get("device_tag_include"):
            include_tags = [parameters["device_tag_include"]]
            parameters["device_tag_include_tags"] = include_tags
        exclude_tags = parameters.get("device_tag_exclude_tags") or []
        if not exclude_tags and parameters.get("device_tag_exclude"):
            parameters["device_tag_exclude_tags"] = [
                parameters["device_tag_exclude"]
            ]
        parameters.pop("device_tag_include", None)
        parameters.pop("device_tag_exclude", None)
        has_include_scope = any(str(tag).strip() for tag in include_tags)
        explicitly_configured = parameters.pop(marker, None) is True
        if has_include_scope and not explicitly_configured:
            parameters["scope_endpoints_by_include_tags"] = True
        else:
            parameters["scope_endpoints_by_include_tags"] = bool(
                parameters.get("scope_endpoints_by_include_tags", True)
            )
        if parameters != (source.parameters or {}):
            source.parameters = parameters
            changed_sources.append(source)
    if changed_sources:
        ForwardSource.objects.bulk_update(
            changed_sources,
            ["parameters"],
            batch_size=2000,
        )

    # The bundled virtual-chassis query is a custom-map contract template and
    # intentionally returns no rows. Do not execute it on every 2.6 sync;
    # customer-authored maps remain untouched and can opt into the model.
    ForwardNQEMap.objects.filter(
        built_in=True,
        name="Forward Virtual Chassis",
    ).update(enabled=False)

    finalized_at = django.utils.timezone.now()
    for ingestion in ForwardIngestion.objects.select_related("branch").iterator():
        if ingestion.failed_change_count:
            # Older releases could persist baseline-ready beside failed merge
            # rows. That state is not a completed merge and must not gain 2.6
            # attestation merely because the historical branch says merged.
            if ingestion.baseline_ready:
                ingestion.baseline_ready = False
                ingestion.save(update_fields=["baseline_ready"])
            continue
        branch = ingestion.branch
        branch_merged = branch is not None and str(branch.status) == "merged"
        if not ingestion.baseline_ready and not branch_merged:
            continue
        merged_at = getattr(branch, "merged_time", None) or finalized_at
        ingestion.merge_applied_at = merged_at
        if ingestion.baseline_ready:
            ingestion.merge_finalized_at = merged_at
        ingestion.save(update_fields=["merge_applied_at", "merge_finalized_at"])


class Migration(migrations.Migration):
    dependencies = [
        ("forward_netbox", "0036_protect_ownership_provenance"),
    ]

    operations = [
        migrations.AddField(
            model_name="forwardingestion",
            name="merge_applied_at",
            field=models.DateTimeField(blank=True, db_index=True, null=True),
        ),
        migrations.AddField(
            model_name="forwardingestion",
            name="merge_finalized_at",
            field=models.DateTimeField(blank=True, db_index=True, null=True),
        ),
        migrations.CreateModel(
            name="ForwardDeviceIdentity",
            fields=[
                (
                    "id",
                    models.BigAutoField(
                        auto_created=True,
                        primary_key=True,
                        serialize=False,
                    ),
                ),
                ("source_device_key", models.CharField(max_length=255)),
                (
                    "ingestion",
                    models.ForeignKey(
                        db_column="generation",
                        on_delete=django.db.models.deletion.PROTECT,
                        related_name="+",
                        to="forward_netbox.forwardingestion",
                    ),
                ),
                (
                    "snapshot_id",
                    models.CharField(blank=True, default="", max_length=100),
                ),
                (
                    "device",
                    models.ForeignKey(
                        on_delete=django.db.models.deletion.PROTECT,
                        related_name="+",
                        to="dcim.device",
                    ),
                ),
                (
                    "sync",
                    models.ForeignKey(
                        on_delete=django.db.models.deletion.PROTECT,
                        related_name="device_identities",
                        to="forward_netbox.forwardsync",
                    ),
                ),
            ],
            options={
                "verbose_name": "Forward Device Identity",
                "verbose_name_plural": "Forward Device Identities",
                "db_table": "forward_netbox_device_identity",
                "ordering": ("sync__name", "source_device_key"),
            },
        ),
        migrations.CreateModel(
            name="ForwardPreservedDeviceTagAssignment",
            fields=[
                (
                    "id",
                    models.BigAutoField(
                        auto_created=True,
                        primary_key=True,
                        serialize=False,
                    ),
                ),
                (
                    "recorded_at",
                    models.DateTimeField(default=django.utils.timezone.now),
                ),
                (
                    "device",
                    models.ForeignKey(
                        on_delete=django.db.models.deletion.CASCADE,
                        related_name="+",
                        to="dcim.device",
                    ),
                ),
                (
                    "tag",
                    models.ForeignKey(
                        on_delete=django.db.models.deletion.CASCADE,
                        related_name="+",
                        to="extras.tag",
                    ),
                ),
            ],
            options={
                "verbose_name": "Forward Preserved Device Tag Assignment",
                "verbose_name_plural": "Forward Preserved Device Tag Assignments",
                "db_table": "forward_netbox_preserved_device_tag_assignment",
                "ordering": ("tag__name", "device__name"),
            },
        ),
        migrations.AddConstraint(
            model_name="forwarddeviceidentity",
            constraint=models.UniqueConstraint(
                fields=("sync", "source_device_key"),
                name="forward_device_identity_source_key",
            ),
        ),
        migrations.AddConstraint(
            model_name="forwarddeviceidentity",
            constraint=models.UniqueConstraint(
                fields=("sync", "device"),
                name="forward_device_identity_device",
            ),
        ),
        migrations.AddConstraint(
            model_name="forwardpreserveddevicetagassignment",
            constraint=models.UniqueConstraint(
                fields=("device", "tag"),
                name="forward_preserved_device_tag_assignment_identity",
            ),
        ),
        migrations.AlterField(
            model_name="forwardnqemap",
            name="netbox_model",
            field=models.ForeignKey(
                limit_choices_to=supported_nqe_model_choices(),
                on_delete=django.db.models.deletion.PROTECT,
                related_name="+",
                to="contenttypes.contenttype",
            ),
        ),
        migrations.RunPython(
            initialize_provenance_and_remove_obsolete_state,
            reverse_code=migrations.RunPython.noop,
        ),
    ]
