from django.db import migrations
from django.db import models


class Migration(migrations.Migration):
    """Hold unsatisfiable merge rows apart from retryable failures.

    Additive with a zero default: existing ingestions recorded every exception
    as a failure, which is exactly what a zero here preserves.
    """

    dependencies = [
        ("forward_netbox", "0047_ownership_reconciliation_cascade"),
    ]

    operations = [
        migrations.AddField(
            model_name="forwardingestion",
            name="skipped_change_count",
            field=models.PositiveIntegerField(default=0),
        ),
    ]
