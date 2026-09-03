# Pre/post approvals, and the permission that guards them.
#
# `min_approvals` is REMOVED and two fields added rather than renamed. It is
# not a rename: one number cannot say "two sign-offs on the plan, one on the
# closure", which is the common real shape. Nothing is lost because 0054 has
# not shipped - 3.0 is the release that introduces all of this - so there are
# no policy rows anywhere holding a value to migrate.
#
# The unique constraint on (change, reviewer, phase) is the one that matters:
# without it, N approvals from the same person satisfy an N-approval policy
# alone, which makes the count meaningless.
from django.conf import settings
from django.db import migrations
from django.db import models


class Migration(migrations.Migration):
    dependencies = [
        ("forward_netbox", "0054_forwardchange_forwardchangecriterion_and_more"),
        migrations.swappable_dependency(settings.AUTH_USER_MODEL),
    ]

    operations = [
        migrations.AlterModelOptions(
            name="forwardchange",
            options={
                "ordering": ("-created",),
                "permissions": (
                    ("approve_forwardchange", "Can approve Forward change"),
                ),
                "verbose_name": "Forward Change",
                "verbose_name_plural": "Forward Changes",
            },
        ),
        migrations.RemoveField(
            model_name="forwardchangepolicy",
            name="min_approvals",
        ),
        migrations.AddField(
            model_name="forwardchangepolicy",
            name="min_pre_approvals",
            field=models.PositiveSmallIntegerField(default=1),
        ),
        migrations.AddField(
            model_name="forwardchangepolicy",
            name="min_post_approvals",
            field=models.PositiveSmallIntegerField(default=1),
        ),
        migrations.AddField(
            model_name="forwardchangereview",
            name="phase",
            field=models.CharField(
                choices=[("pre", "Pre-change"), ("post", "Post-change")],
                default="pre",
                max_length=16,
            ),
        ),
        migrations.AddField(
            model_name="forwardchangereview",
            name="after_snapshot_id",
            field=models.CharField(blank=True, default="", max_length=100),
        ),
        migrations.AddField(
            model_name="forwardchangereview",
            name="verdict",
            field=models.CharField(blank=True, default="", max_length=16),
        ),
        migrations.AddConstraint(
            model_name="forwardchangereview",
            constraint=models.UniqueConstraint(
                fields=("change", "reviewer", "phase"),
                name="forward_change_review_one_per_reviewer_phase",
            ),
        ),
    ]
