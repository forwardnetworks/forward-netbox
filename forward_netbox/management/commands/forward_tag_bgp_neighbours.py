import json

from django.contrib.auth import get_user_model
from django.core.management.base import BaseCommand
from django.core.management.base import CommandError

from forward_netbox.utilities.bgp_neighbour_ips import BGP_NEIGHBOUR_TAG_SLUG
from forward_netbox.utilities.bgp_neighbour_ips import tag_bgp_neighbour_ips


class Command(BaseCommand):
    help = (
        "Tag the IP addresses that exist only because a BGP peer needs them "
        f"with `{BGP_NEIGHBOUR_TAG_SLUG}`, so they are not mistaken for orphans "
        "in IPAM. Self-healing: the tag set is reconciled to exactly the current "
        "neighbour addresses on every run. Never deletes. Writes one change-logged "
        "edit per address the first time, so run it deliberately, not from a sync."
    )

    def add_arguments(self, parser):
        parser.add_argument(
            "--user",
            required=True,
            help="Username the tag changes are logged and attributed to.",
        )

    def handle(self, *args, **options):
        user = get_user_model().objects.filter(username=options["user"]).first()
        if user is None:
            raise CommandError(f"No user named `{options['user']}`.")
        result = tag_bgp_neighbour_ips(user=user)
        payload = {
            "tag_slug": BGP_NEIGHBOUR_TAG_SLUG,
            "netbox_routing_installed": result is not None,
            **(result or {"added": 0, "removed": 0, "total": 0}),
        }
        self.stdout.write(json.dumps(payload, indent=2))
