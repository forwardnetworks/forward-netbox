import django.core.validators
import django.db.models.deletion
import netbox.models.deletion
from django.conf import settings
from django.db import migrations
from django.db import models


class Migration(migrations.Migration):
    dependencies = [
        ("forward_netbox", "0062_change_request_fields"),
        ("users", "0016_default_ordering_indexes"),
        migrations.swappable_dependency(settings.AUTH_USER_MODEL),
    ]

    operations = [
        migrations.CreateModel(
            name="ForwardChangeApprovalRule",
            fields=[
                (
                    "id",
                    models.BigAutoField(
                        auto_created=True, primary_key=True, serialize=False
                    ),
                ),
                ("created", models.DateTimeField(auto_now_add=True, null=True)),
                ("last_updated", models.DateTimeField(auto_now=True, null=True)),
                ("name", models.CharField(max_length=100)),
                (
                    "any_reviewer",
                    models.BooleanField(
                        default=False,
                        help_text="Any reviewer counts. Leave off to require the groups and users below.",
                    ),
                ),
                (
                    "min_reviews",
                    models.PositiveSmallIntegerField(
                        default=1,
                        validators=[django.core.validators.MinValueValidator(1)],
                    ),
                ),
                (
                    "groups",
                    models.ManyToManyField(
                        blank=True, related_name="+", to="users.group"
                    ),
                ),
                (
                    "policy",
                    models.ForeignKey(
                        on_delete=django.db.models.deletion.CASCADE,
                        related_name="approval_rules",
                        to="forward_netbox.forwardchangepolicy",
                    ),
                ),
                (
                    "reviewers",
                    models.ManyToManyField(
                        blank=True,
                        related_name="+",
                        to=settings.AUTH_USER_MODEL,
                    ),
                ),
            ],
            options={
                "verbose_name": "Forward Change Approval Rule",
                "verbose_name_plural": "Forward Change Approval Rules",
                "db_table": "forward_netbox_change_approval_rule",
                "ordering": ("policy", "name"),
                "constraints": [
                    models.UniqueConstraint(
                        fields=("policy", "name"),
                        name="forward_change_approval_rule_unique_name",
                    )
                ],
            },
            bases=(netbox.models.deletion.DeleteMixin, models.Model),
        ),
    ]
