import json
import time

from django.core.management.base import BaseCommand
from django.core.management.base import CommandError

from forward_netbox.exceptions import ForwardClientError
from forward_netbox.exceptions import ForwardConnectivityError
from forward_netbox.models import ForwardSync
from forward_netbox.utilities.branch_budget import row_shard_key
from forward_netbox.utilities.branch_budget import shard_fetch_contract
from forward_netbox.utilities.query_fetch import ForwardQueryFetcher
from forward_netbox.utilities.query_registry import get_query_specs
from forward_netbox.utilities.query_registry import resolve_query_specs_for_client


class Command(BaseCommand):
    help = (
        "Profile Forward query pushdown behavior for one NetBox model against a live "
        "sync/source configuration."
    )

    def add_arguments(self, parser):
        parser.add_argument("--sync-name", required=True)
        parser.add_argument("--model", default="")
        parser.add_argument(
            "--query-name",
            default="",
            help="Optional map query name when multiple maps are bound to the model.",
        )
        parser.add_argument(
            "--top-slow-models",
            type=int,
            default=0,
            help=(
                "If set, profile this many slowest models from recent execution-step "
                "history instead of a single --model."
            ),
        )
        parser.add_argument(
            "--sample-shard-keys",
            type=int,
            default=200,
            help="Number of shard keys to sample for pushdown profiling.",
        )
        parser.add_argument(
            "--output-json",
            default="",
            help="Optional path to write the profile report as JSON.",
        )

    def handle(self, *args, **options):
        sync = ForwardSync.objects.filter(name=options["sync_name"]).first()
        if sync is None:
            raise CommandError(f"Forward sync `{options['sync_name']}` was not found.")
        if options["sample_shard_keys"] < 1:
            raise CommandError("--sample-shard-keys must be at least 1.")
        if options["top_slow_models"] < 0:
            raise CommandError("--top-slow-models must be 0 or greater.")
        if options["top_slow_models"] == 0 and not options["model"]:
            raise CommandError("Provide --model or set --top-slow-models.")

        client = sync.source.get_client()
        fetcher = ForwardQueryFetcher(sync, client, sync.logger)
        try:
            context = fetcher.resolve_context()
        except ForwardConnectivityError as exc:
            raise CommandError(
                "Unable to run live pushdown profile because Forward API "
                f"connectivity failed for source `{sync.source.name}` "
                f"({sync.source.url}): {exc}"
            ) from exc

        if options["top_slow_models"] > 0:
            models_to_profile = self._top_slow_models(sync, options["top_slow_models"])
            if not models_to_profile:
                raise CommandError(
                    "No recent execution-step timing data was found to select slow models."
                )
        else:
            models_to_profile = [options["model"]]

        reports = []
        for model_string in models_to_profile:
            report = self._profile_model(
                sync=sync,
                client=client,
                fetcher=fetcher,
                context=context,
                model_string=model_string,
                query_name=options["query_name"],
                sample_shard_keys=options["sample_shard_keys"],
            )
            reports.append(report)

        report = {
            "sync_name": sync.name,
            "source_name": sync.source.name,
            "snapshot_id": context.snapshot_id,
            "models_profiled": len(reports),
            "reports": reports,
        }

        rendered = json.dumps(report, indent=2, sort_keys=True)
        self.stdout.write(rendered)

        output_path = (options["output_json"] or "").strip()
        if output_path:
            with open(output_path, "w", encoding="utf-8") as handle:
                handle.write(rendered + "\n")
            self.stdout.write(
                self.style.SUCCESS(f"Wrote pushdown profile report to {output_path}")
            )

    def _profile_model(
        self,
        *,
        sync,
        client,
        fetcher,
        context,
        model_string,
        query_name,
        sample_shard_keys,
    ):
        specs = get_query_specs(model_string, maps=context.maps)
        specs = resolve_query_specs_for_client(specs, client)
        if not specs:
            raise CommandError(f"No query specs are bound for model `{model_string}`.")
        spec = self._select_spec(specs, query_name)

        coalesce_fields = fetcher._coalesce_fields(model_string, [spec])
        merged_parameters = spec.merged_parameters(context.query_parameters)

        used_parameter_fallback = False
        full_start = time.perf_counter()
        try:
            full_rows = client.run_nqe_query(
                query=spec.query,
                query_id=spec.run_query_id,
                commit_id=spec.commit_id,
                network_id=context.network_id,
                snapshot_id=context.snapshot_id,
                parameters=merged_parameters,
                fetch_all=True,
            )
        except ForwardClientError as exc:
            if not self._looks_like_parameter_contract_error(exc):
                raise
            used_parameter_fallback = True
            full_rows = client.run_nqe_query(
                query=spec.query,
                query_id=spec.run_query_id,
                commit_id=spec.commit_id,
                network_id=context.network_id,
                snapshot_id=context.snapshot_id,
                parameters={},
                fetch_all=True,
            )
        full_runtime_ms = round((time.perf_counter() - full_start) * 1000.0, 3)

        shard_keys = []
        for row in full_rows:
            try:
                shard_keys.append(row_shard_key(model_string, row, coalesce_fields))
            except Exception:
                continue
        shard_keys = sorted({str(key) for key in shard_keys if key})
        if not shard_keys:
            raise CommandError(
                f"No shard keys were derived from full query rows for `{model_string}`."
            )
        sampled_keys = tuple(shard_keys[:sample_shard_keys])
        scope = shard_fetch_contract(model_string, sampled_keys)

        pushdown_parameters = dict(merged_parameters)
        pushdown_parameters.update(scope.get("query_parameters") or {})
        pushdown_filters = scope.get("fetch_column_filters") or None

        pushdown_start = time.perf_counter()
        pushdown_error = ""
        pushdown_supported = True
        try:
            pushdown_rows = client.run_nqe_query(
                query=spec.query,
                query_id=spec.run_query_id,
                commit_id=spec.commit_id,
                network_id=context.network_id,
                snapshot_id=context.snapshot_id,
                parameters=pushdown_parameters,
                column_filters=pushdown_filters,
                fetch_all=True,
            )
        except ForwardClientError as exc:
            if not self._looks_like_parameter_contract_error(exc):
                pushdown_supported = False
                pushdown_error = str(exc)
                pushdown_rows = []
            else:
                used_parameter_fallback = True
                try:
                    pushdown_rows = client.run_nqe_query(
                        query=spec.query,
                        query_id=spec.run_query_id,
                        commit_id=spec.commit_id,
                        network_id=context.network_id,
                        snapshot_id=context.snapshot_id,
                        parameters={},
                        column_filters=pushdown_filters,
                        fetch_all=True,
                    )
                except ForwardClientError as fallback_exc:
                    pushdown_supported = False
                    pushdown_error = str(fallback_exc)
                    pushdown_rows = []
        pushdown_runtime_ms = round((time.perf_counter() - pushdown_start) * 1000.0, 3)

        local_filtered_rows = self._filter_rows_to_scope(
            model_string,
            full_rows,
            coalesce_fields,
            set(sampled_keys),
        )
        parity = self._parity_result(
            local_filtered_rows=local_filtered_rows,
            pushdown_rows=pushdown_rows,
            pushdown_supported=pushdown_supported,
        )
        return {
            "model": model_string,
            "query_name": spec.query_name,
            "execution_mode": spec.execution_mode,
            "execution_value": spec.execution_value,
            "sample_shard_key_count": len(sampled_keys),
            "scope": {
                "fetch_mode": scope.get("fetch_mode"),
                "fetch_key_family": scope.get("fetch_key_family"),
                "query_parameter_keys": sorted(pushdown_parameters.keys()),
                "column_filter_count": len(pushdown_filters or []),
                "used_parameter_fallback": used_parameter_fallback,
                "pushdown_supported": pushdown_supported,
                "pushdown_error": pushdown_error,
            },
            "runtime_ms": {
                "full_fetch": full_runtime_ms,
                "pushdown_fetch": pushdown_runtime_ms,
            },
            "rows": {
                "full_fetch": len(full_rows),
                "pushdown_fetch": len(pushdown_rows),
            },
            "parity": parity,
        }

    def _parity_result(self, *, local_filtered_rows, pushdown_rows, pushdown_supported):
        if pushdown_supported:
            local_key_set = {
                json.dumps(row, sort_keys=True, separators=(",", ":"))
                for row in local_filtered_rows
            }
            pushdown_key_set = {
                json.dumps(row, sort_keys=True, separators=(",", ":"))
                for row in pushdown_rows
            }
            return {
                "local_filtered_count": len(local_filtered_rows),
                "pushdown_count": len(pushdown_rows),
                "missing_from_pushdown": max(0, len(local_key_set - pushdown_key_set)),
                "extra_in_pushdown": max(0, len(pushdown_key_set - local_key_set)),
                "exact_match": local_key_set == pushdown_key_set,
            }
        return {
            "local_filtered_count": len(local_filtered_rows),
            "pushdown_count": None,
            "missing_from_pushdown": None,
            "extra_in_pushdown": None,
            "exact_match": None,
        }

    def _top_slow_models(self, sync, limit):
        from forward_netbox.models import ForwardExecutionRun
        from forward_netbox.models import ForwardExecutionStep
        from forward_netbox.choices import ForwardExecutionRunStatusChoices
        from forward_netbox.choices import ForwardExecutionStepStatusChoices

        run_statuses = {
            ForwardExecutionRunStatusChoices.COMPLETED,
            ForwardExecutionRunStatusChoices.FAILED,
            ForwardExecutionRunStatusChoices.TIMEOUT,
        }
        step_statuses = {
            ForwardExecutionStepStatusChoices.STAGED,
            ForwardExecutionStepStatusChoices.MERGED,
            ForwardExecutionStepStatusChoices.FAILED,
            ForwardExecutionStepStatusChoices.TIMEOUT,
            ForwardExecutionStepStatusChoices.MERGE_TIMEOUT,
        }
        runs = (
            ForwardExecutionRun.objects.filter(sync=sync, status__in=run_statuses)
            .order_by("-created")
            .values_list("pk", flat=True)[:5]
        )
        if not runs:
            return []
        model_seconds = {}
        for step in ForwardExecutionStep.objects.filter(
            run_id__in=list(runs),
            status__in=step_statuses,
        ):
            if not step.model_string or not step.started or not step.completed:
                continue
            seconds = max(0.0, (step.completed - step.started).total_seconds())
            model_seconds[step.model_string] = (
                model_seconds.get(step.model_string, 0.0) + seconds
            )
        ordered = sorted(model_seconds.items(), key=lambda item: item[1], reverse=True)
        return [model_string for model_string, _ in ordered[:limit]]

    def _filter_rows_to_scope(self, model_string, rows, coalesce_fields, shard_keys):
        scoped_rows = []
        for row in rows:
            try:
                if row_shard_key(model_string, row, coalesce_fields) in shard_keys:
                    scoped_rows.append(row)
            except Exception:
                continue
        return scoped_rows

    def _select_spec(self, specs, query_name):
        if not query_name:
            return specs[0]
        for spec in specs:
            if spec.query_name == query_name:
                return spec
        raise CommandError(
            f"Query name `{query_name}` is not bound for the selected model."
        )

    def _looks_like_parameter_contract_error(self, exc):
        message = str(exc).lower()
        return "does not take parameters" in message or "nqe_runtime_error" in message
