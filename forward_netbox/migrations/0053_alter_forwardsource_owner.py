# Record NetBox 4.7 dropping OwnerMixin's automatic reverse relation.
#
# `owner` is inherited, not declared here: NetBox 4.7 removed the reverse
# accessor `OwnerMixin` used to create (`forwardsource_set` and friends), which
# changes the field's migration state to `related_name="+"`. No column moves and
# no data is touched - without this migration `makemigrations --check` fails on
# every run.
#
# The resulting relation is both PROTECT and hidden. Django omits
# `related_name="+"` relations from `_meta.related_objects`, which is how an
# undeletable-ingestion bug once hid; `protecting_relations()` reads them with
# `include_hidden=True` for exactly that reason. This one points from
# ForwardSource at `users.Owner`, so it protects Owner deletion rather than
# anything this sync manages - but it is the same shape, and here it is NetBox's
# choice rather than ours.
import django.db.models.deletion
from django.db import migrations
from django.db import models


class Migration(migrations.Migration):
    dependencies = [
        ("forward_netbox", "0052_device_absence_quarantine"),
        ("users", "0016_default_ordering_indexes"),
    ]

    operations = [
        migrations.AlterField(
            model_name="forwardsource",
            name="owner",
            field=models.ForeignKey(
                blank=True,
                null=True,
                on_delete=django.db.models.deletion.PROTECT,
                related_name="+",
                to="users.owner",
            ),
        ),
    ]
