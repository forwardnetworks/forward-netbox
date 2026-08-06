import django.db.models.deletion
from django.db import migrations
from django.db import models


# Contributor baselines are main-schema convergence evidence, like the ownership
# rows in 0047. Branch schemas must not receive independent constraints for them.
fake_on_branch = True


class Migration(migrations.Migration):
    """Let an ingestion take its spent contributor baseline with it.

    `ForwardContributorBaseline.ingestion` was PROTECT, and nothing anywhere
    deletes a baseline. Promotion marks the previous generation SUPERSEDED,
    deletes its relations and empties its payload — but keeps the row, which
    goes on protecting its ingestion. So every ingestion that ever promoted
    became permanently undeletable and the backlog grew by one per successful
    sync; a customer reported three at once, and the refusal named a record he
    had no way to remove.

    The live baseline must still be kept, and CASCADE alone would not keep it.
    That guarantee moves from the database to `refuse_ingestion_delete_with_live
    _baseline` in `signals.py`, a `pre_delete` receiver, which fires for
    querysets as well as instances and so covers every path the PROTECT
    constraint covered. It is a deliberate trade: PROTECT could not express
    "keep this one, collect that one", and expressing it in Python is what makes
    the spent rows collectable at all.

    Reversible: the reverse restores PROTECT. It can fail only if an ingestion
    was deleted while this was applied and its spent baseline went with it —
    which is the point of applying it — and no reverse resurrects those rows.
    """

    dependencies = [
        ("forward_netbox", "0049_drift_policy_row_shrink_floor"),
    ]

    operations = [
        migrations.AlterField(
            model_name="forwardcontributorbaseline",
            name="ingestion",
            field=models.OneToOneField(
                on_delete=django.db.models.deletion.CASCADE,
                related_name="contributor_baseline",
                to="forward_netbox.forwardingestion",
            ),
        ),
    ]
