from django.db import migrations


fake_on_branch = True


def remove_redundant_nqe_settings(apps, schema_editor):
    ForwardSource = apps.get_model("forward_netbox", "ForwardSource")
    changed_sources = []
    for source in ForwardSource.objects.all().only("parameters").iterator():
        parameters = dict(source.parameters or {})
        parameters.pop("query_preflight_enabled", None)
        parameters.pop("query_preflight_row_limit", None)
        if parameters == (source.parameters or {}):
            continue
        source.parameters = parameters
        changed_sources.append(source)
    if changed_sources:
        ForwardSource.objects.bulk_update(
            changed_sources,
            ["parameters"],
            batch_size=2000,
        )

    ForwardSync = apps.get_model("forward_netbox", "ForwardSync")
    changed_syncs = []
    for sync in ForwardSync.objects.all().only("parameters").iterator():
        parameters = dict(sync.parameters or {})
        parameters.pop("skip_unchanged_snapshot", None)
        if parameters == (sync.parameters or {}):
            continue
        sync.parameters = parameters
        changed_syncs.append(sync)
    if changed_syncs:
        ForwardSync.objects.bulk_update(
            changed_syncs,
            ["parameters"],
            batch_size=2000,
        )


class Migration(migrations.Migration):
    dependencies = [
        ("forward_netbox", "0039_forwardingestion_catchup_state"),
    ]

    operations = [
        migrations.RunPython(
            remove_redundant_nqe_settings,
            reverse_code=migrations.RunPython.noop,
        ),
    ]
