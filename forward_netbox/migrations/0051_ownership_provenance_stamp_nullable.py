import django.db.models.deletion
from django.db import migrations
from django.db import models


# Ownership evidence is main-schema convergence state, like the rows in 0047 and
# the baseline in 0050. Branch schemas must not receive independent constraints
# for them.
fake_on_branch = True


class Migration(migrations.Migration):
    """Stop a provenance stamp behaving like a dependency.

    `ForwardDeviceIdentity`, `ForwardDeviceTagClaim` and
    `ForwardVirtualParentClaim` each carry an `ingestion` FK recording which run
    last asserted the evidence. It was PROTECT, so that stamp pinned the run.

    Evidence is only re-pointed for what the current run sees: identities for
    the current candidate set, tag claims for the tags the sync still manages.
    A device that leaves Forward's scope - or a tag that is removed from the
    include list - is never visited again, so its evidence freezes on the last
    ingestion that saw it and pins that ingestion permanently. A customer
    accumulated one undeletable ingestion per scope change.

    The rows doing the pinning are NOT stale. The devices still exist and are
    still owned; only the stamp is old. That is why every model of this problem
    that proposed pruning the evidence was wrong - it would have released
    ownership of live devices that were merely out of scope, and devices leave
    scope for entirely benign reasons, such as someone editing a tag in Forward.

    SET_NULL keeps the ownership and drops only the pointer to a run that no
    longer exists. Everything the evidence is FOR - the sync, the device, the
    source key, the `snapshot_id` it was last seen in - lives on the row and is
    untouched. The generation comparisons already treat a non-matching stamp as
    stale, and NULL reads the same way.

    `ForwardOwnershipReconciliation` is deliberately not included: it overrides
    the field to CASCADE because it is a child record of the ingestion rather
    than evidence held against it.

    Reversible: the reverse restores PROTECT and non-null. It fails if any row
    has a null stamp by then, which is the expected state once an ingestion has
    been deleted - so reversing requires deciding what those rows should point
    at, and no reverse can invent it.
    """

    dependencies = [
        ("forward_netbox", "0050_contributor_baseline_cascade"),
    ]

    operations = [
        migrations.AlterField(
            model_name=model_name,
            name="ingestion",
            field=models.ForeignKey(
                blank=True,
                db_column="generation",
                null=True,
                on_delete=django.db.models.deletion.SET_NULL,
                related_name="+",
                to="forward_netbox.forwardingestion",
            ),
        )
        for model_name in (
            "forwarddeviceidentity",
            "forwarddevicetagclaim",
            "forwardvirtualparentclaim",
        )
    ]
