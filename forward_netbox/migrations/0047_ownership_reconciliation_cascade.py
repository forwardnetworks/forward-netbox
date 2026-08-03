import django.db.models.deletion
from django.db import migrations
from django.db import models


# Ownership is a main-schema control plane. Branch schemas must never receive
# independent claim constraints or foreign keys.
fake_on_branch = True


class Migration(migrations.Migration):
    """Let an ingestion take its own reconciliation rows with it.

    `ForwardOwnershipReconciliation.ingestion` was PROTECT, like the three
    sibling provenance models. Those three describe live NetBox objects and must
    outlive an ingestion; a reconciliation row describes only "this sync
    finished this domain at this ingestion" and means nothing without it. No UI,
    API, or management command deletes these rows, so the protection refused a
    delete while naming records the operator had no way to remove.

    Reversible: the reverse restores PROTECT. It can fail only if an ingestion
    was deleted while this migration was applied and its rows went with it -
    which is the point of applying it - and no reverse can resurrect those rows.
    Nothing else in the schema changes, so downgrading is otherwise safe.
    """

    dependencies = [
        ("forward_netbox", "0046_alter_forwardnqemap_netbox_model"),
    ]

    operations = [
        migrations.AlterField(
            model_name="forwardownershipreconciliation",
            name="ingestion",
            field=models.ForeignKey(
                db_column="generation",
                on_delete=django.db.models.deletion.CASCADE,
                related_name="+",
                to="forward_netbox.forwardingestion",
            ),
        ),
    ]
