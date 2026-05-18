import json
from types import SimpleNamespace

from django.core.management.base import BaseCommand
from django.core.management.base import CommandError

from forward_netbox.choices import FORWARD_SUPPORTED_MODELS
from forward_netbox.models import ForwardSync
from forward_netbox.utilities.apply_engine import ADAPTER_MODEL_BLOCKERS
from forward_netbox.utilities.apply_engine import ADAPTER_MODELS_WITHOUT_BLOCKER
from forward_netbox.utilities.apply_engine import ADAPTER_REQUIRED_MODELS
from forward_netbox.utilities.apply_engine import apply_engine_decision_for
from forward_netbox.utilities.apply_engine import BULK_ORM_ENABLED_MODELS
from forward_netbox.utilities.apply_engine import BULK_ORM_ENABLED_MODELS_WITHOUT_SPECS
from forward_netbox.utilities.apply_engine import UNCLASSIFIED_SUPPORTED_MODELS
from forward_netbox.utilities.branch_budget import shard_fetch_capability_for_model
from forward_netbox.utilities.execution_ledger import latest_execution_run
from forward_netbox.utilities.health import sync_health_summary


class Command(BaseCommand):
    help = (
        "Emit an architecture audit report with apply-engine model classification "
        "and optional sync/runtime health evidence."
    )

    def add_arguments(self, parser):
        parser.add_argument(
            "--sync-name",
            default="",
            help="Optional ForwardSync name to include sync health/runtime evidence.",
        )
        parser.add_argument(
            "--output-json",
            default="",
            help="Optional path to write the report JSON.",
        )
        parser.add_argument(
            "--fail-on-gap",
            action="store_true",
            help=(
                "Exit non-zero when model classification has architecture gaps "
                "(for example unclassified supported models, adapter models "
                "without blocker codes, or enabled bulk-ORM models without "
                "spec coverage)."
            ),
        )

    def handle(self, *args, **options):
        sync_name = (options.get("sync_name") or "").strip()
        sync = None
        if sync_name:
            sync = ForwardSync.objects.filter(name=sync_name).first()
            if sync is None:
                raise CommandError(f"Forward sync `{sync_name}` was not found.")

        report = {
            "apply_engine_matrix": self._apply_engine_matrix(),
            "sync_evidence": self._sync_evidence(sync) if sync is not None else None,
        }
        gaps = report["apply_engine_matrix"]["classification_gaps"]

        rendered = json.dumps(report, indent=2, sort_keys=True, default=str)
        self.stdout.write(rendered)

        output_path = (options.get("output_json") or "").strip()
        if output_path:
            with open(output_path, "w", encoding="utf-8") as handle:
                handle.write(rendered + "\n")
            self.stdout.write(
                self.style.SUCCESS(f"Wrote architecture audit report to {output_path}")
            )

        if options.get("fail_on_gap") and self._has_classification_gaps(gaps):
            raise CommandError(
                "Architecture model classification gaps detected. "
                "Inspect `apply_engine_matrix.classification_gaps`."
            )

    def _apply_engine_matrix(self):
        model_eligibility = self._model_eligibility()
        fetch_contracts = self._fetch_contracts()
        decision_fallback_models = sorted(
            model_string
            for model_string, decisions in model_eligibility.items()
            if decisions["default"]["reason_code"]
            == "adapter_default_unclassified_model"
            or decisions["bulk_enabled"]["reason_code"]
            == "adapter_default_unclassified_model"
        )
        fetch_contract_coverage_gaps = sorted(
            model_string
            for model_string, contract in fetch_contracts.items()
            if contract.get("model") != model_string
            or contract.get("fetch_mode")
            not in {"nqe_column_filter", "nqe_parameters", "model"}
            or not contract.get("schema_contract")
            or not contract.get("reason_code")
            or not contract.get("reason")
            or contract.get("local_safety_filter") is not True
            or "bucket_strategy" not in contract
        )
        return {
            "bulk_orm_safe_models": sorted(BULK_ORM_ENABLED_MODELS),
            "adapter_required_models": sorted(ADAPTER_REQUIRED_MODELS),
            "adapter_blockers": {
                model_string: ADAPTER_MODEL_BLOCKERS[model_string]["blocker_code"]
                for model_string in sorted(ADAPTER_MODEL_BLOCKERS)
            },
            "model_eligibility": model_eligibility,
            "fetch_contracts": fetch_contracts,
            "classification_gaps": {
                "unclassified_supported_models": sorted(UNCLASSIFIED_SUPPORTED_MODELS),
                "adapter_models_without_blocker": sorted(
                    ADAPTER_MODELS_WITHOUT_BLOCKER
                ),
                "bulk_orm_enabled_models_without_specs": sorted(
                    BULK_ORM_ENABLED_MODELS_WITHOUT_SPECS
                ),
                "decision_unclassified_fallback_models": decision_fallback_models,
                "fetch_contract_coverage_gaps": fetch_contract_coverage_gaps,
            },
        }

    def _has_classification_gaps(self, gaps):
        return any(bool(items) for items in gaps.values())

    def _model_eligibility(self):
        default_sync = SimpleNamespace(
            parameters={"execution_backend": "branching", "enable_bulk_orm": False}
        )
        bulk_enabled_sync = SimpleNamespace(
            parameters={"execution_backend": "branching", "enable_bulk_orm": True}
        )

        decisions = {}
        for model_string in sorted(FORWARD_SUPPORTED_MODELS):
            default_decision = apply_engine_decision_for(
                sync=default_sync,
                model_string=model_string,
                backend="branching",
            )
            bulk_enabled_decision = apply_engine_decision_for(
                sync=bulk_enabled_sync,
                model_string=model_string,
                backend="branching",
            )
            decisions[model_string] = {
                "default": {
                    "selected_engine": default_decision.selected_engine,
                    "reason_code": default_decision.reason_code,
                },
                "bulk_enabled": {
                    "selected_engine": bulk_enabled_decision.selected_engine,
                    "reason_code": bulk_enabled_decision.reason_code,
                },
            }
        return decisions

    def _fetch_contracts(self):
        contracts = {}
        for model_string in sorted(FORWARD_SUPPORTED_MODELS):
            contracts[model_string] = shard_fetch_capability_for_model(model_string)
        return contracts

    def _sync_evidence(self, sync):
        run = latest_execution_run(sync)
        run_summary = None
        if run is not None:
            run_summary = {
                "run_id": run.pk,
                "status": run.status,
                "phase": run.phase,
                "total_steps": run.total_steps,
                "next_step_index": run.next_step_index,
                "backend": run.backend,
            }
        return {
            "sync_name": sync.name,
            "sync_health_summary": sync_health_summary(sync),
            "latest_execution_run": run_summary,
        }
