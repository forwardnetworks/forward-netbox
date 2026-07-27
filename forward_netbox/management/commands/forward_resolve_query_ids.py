from django.core.management.base import BaseCommand
from django.core.management.base import CommandError

from forward_netbox.models import ForwardNQEMap
from forward_netbox.models import ForwardSync
from forward_netbox.utilities.query_binding import resolve_nqe_map_query_ids


class Command(BaseCommand):
    help = (
        "Resolve enabled path-bound maps to query IDs using read-only Forward "
        "repository metadata."
    )

    def add_arguments(self, parser):
        parser.add_argument(
            "--sync-id",
            type=int,
            required=True,
            help="Local ForwardSync primary key whose enabled maps should be resolved.",
        )

    def handle(self, *args, **options):
        sync = (
            ForwardSync.objects.select_related("source")
            .filter(pk=options["sync_id"])
            .first()
        )
        if sync is None:
            raise CommandError("The selected local Forward sync does not exist.")

        map_ids = [
            query_map.pk
            for query_map in sync.get_maps()
            if sync.is_model_enabled(query_map.model_string)
        ]
        results = resolve_nqe_map_query_ids(
            client=sync.source.get_client(),
            queryset=ForwardNQEMap.objects.filter(pk__in=map_ids).select_related(
                "netbox_model"
            ),
        )
        resolved_count = len([result for result in results if result.matched])
        unchanged_count = len(results) - resolved_count
        self.stdout.write(
            f"Enabled maps={len(results)} resolved={resolved_count} "
            f"unchanged={unchanged_count}"
        )
        if unchanged_count:
            self.stdout.write(
                self.style.WARNING(
                    "Some maps remained unchanged because they were already bound, "
                    "not path-bound, ambiguous, or unresolved."
                )
            )
        if resolved_count:
            self.stdout.write(
                self.style.SUCCESS(
                    "Query IDs were persisted using read-only repository metadata; "
                    "no Forward queries were published or changed."
                )
            )
