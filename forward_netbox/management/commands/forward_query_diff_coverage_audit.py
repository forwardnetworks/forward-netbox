import json

from django.core.management.base import BaseCommand

from forward_netbox.models import ForwardNQEMap

# Execution modes that resolve to a committed Forward query_id and are therefore
# eligible for Forward nqe-diff execution on later runs. A raw inline `query`
# cannot diff and forces a full fetch of that model every sync.
DIFF_ELIGIBLE_MODES = {"query_id", "query_path"}


class Command(BaseCommand):
    help = (
        "Audit enabled Forward NQE maps for diff coverage: report which maps "
        "resolve to a query_id/query_path (nqe-diff eligible) versus raw inline "
        "query text (forces a full fetch every sync)."
    )

    def add_arguments(self, parser):
        parser.add_argument(
            "--include-disabled",
            action="store_true",
            help="Include disabled maps in the audit instead of enabled-only.",
        )
        parser.add_argument(
            "--fail-on-full",
            action="store_true",
            help="Exit non-zero when any audited map is full-fetch-only.",
        )

    def handle(self, *args, **options):
        maps = ForwardNQEMap.objects.select_related("netbox_model").order_by(
            "weight", "pk"
        )
        if not options["include_disabled"]:
            maps = maps.filter(enabled=True)

        diff_eligible = []
        full_only = []
        for query_map in maps:
            entry = {
                "name": query_map.name,
                "model": query_map.model_string,
                "execution_mode": query_map.execution_mode,
                "enabled": bool(query_map.enabled),
            }
            if query_map.execution_mode in DIFF_ELIGIBLE_MODES:
                diff_eligible.append(entry)
            else:
                full_only.append(entry)

        payload = {
            "scope": "all" if options["include_disabled"] else "enabled",
            "counts": {
                "total": len(diff_eligible) + len(full_only),
                "diff_eligible": len(diff_eligible),
                "full_fetch_only": len(full_only),
            },
            "full_fetch_only": full_only,
            "diff_eligible": diff_eligible,
            "remediation": (
                (
                    "Bind full-fetch-only maps to an Org Repository query_path (or a "
                    "committed query_id) so later runs use Forward nqe-diffs instead "
                    "of re-fetching the whole model."
                )
                if full_only
                else ""
            ),
        }

        self.stdout.write(json.dumps(payload, indent=2, default=str))

        if options["fail_on_full"] and full_only:
            self.stderr.write(
                f"{len(full_only)} map(s) are full-fetch-only (no diff coverage)."
            )
            raise SystemExit(1)
