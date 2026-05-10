from __future__ import annotations

import json
import os
from pathlib import Path

from dcim.models import Device
from dcim.models.device_components import ModuleBay
from django.core.management.base import BaseCommand
from django.core.management.base import CommandError
from django.utils import timezone
from django.utils.text import slugify

from forward_netbox.exceptions import ForwardClientError
from forward_netbox.models import ForwardSource
from forward_netbox.models import ForwardSync
from forward_netbox.utilities.forward_api import LATEST_PROCESSED_SNAPSHOT
from forward_netbox.utilities.module_readiness import summarize_module_readiness
from forward_netbox.utilities.module_readiness import write_module_bay_import_csv
from forward_netbox.utilities.query_registry import get_query_specs
from forward_netbox.utilities.query_registry import get_seeded_builtin_query_spec
from forward_netbox.utilities.query_registry import resolve_query_specs_for_client


class Command(BaseCommand):
    help = "Report readiness for optional dcim.module imports."

    def add_arguments(self, parser):
        parser.add_argument(
            "--sync-name",
            default=os.getenv("FORWARD_MODULE_READINESS_SYNC_NAME", ""),
            help="Existing Forward Sync to use for source, snapshot, and query map settings.",
        )
        parser.add_argument(
            "--source-name",
            default=os.getenv(
                "FORWARD_MODULE_READINESS_SOURCE_NAME",
                os.getenv("FORWARD_SMOKE_SOURCE_NAME", "smoke-source"),
            ),
            help="Existing Forward Source to use when --sync-name is not provided.",
        )
        parser.add_argument(
            "--network-id",
            default=os.getenv("FORWARD_MODULE_READINESS_NETWORK_ID", ""),
            help="Forward network override. Defaults to the selected source network.",
        )
        parser.add_argument(
            "--snapshot-id",
            default=os.getenv(
                "FORWARD_MODULE_READINESS_SNAPSHOT_ID",
                os.getenv("FORWARD_SMOKE_SNAPSHOT_ID", LATEST_PROCESSED_SNAPSHOT),
            ),
            help="Forward snapshot selector. Defaults to latestProcessed.",
        )
        parser.add_argument(
            "--output-dir",
            default=os.getenv(
                "FORWARD_MODULE_READINESS_OUTPUT_DIR",
                "/tmp/forward-netbox-module-readiness",
            ),
            help="Directory where summary and NetBox import CSV files will be written.",
        )

    def handle(self, *args, **options):
        sync, source = self._resolve_sync_and_source(options)
        client = source.get_client()
        network_id = options["network_id"] or (
            sync.get_network_id() if sync else source.network_id
        )
        snapshot_selector = sync.get_snapshot_id() if sync else options["snapshot_id"]
        if not network_id:
            raise CommandError("Set --network-id or configure a network on the source.")
        snapshot_id = self._resolve_snapshot_id(
            client,
            network_id=network_id,
            snapshot_selector=snapshot_selector,
        )
        rows = self._fetch_module_rows(
            sync,
            client,
            network_id=network_id,
            snapshot_id=snapshot_id,
        )
        report = summarize_module_readiness(
            rows,
            existing_devices=set(Device.objects.values_list("name", flat=True)),
            existing_module_bays=set(
                ModuleBay.objects.values_list("device__name", "name")
            ),
        )
        output_dir = self._output_dir(options["output_dir"], sync=sync, source=source)
        output_dir.mkdir(parents=True, exist_ok=True)
        csv_path = output_dir / "netbox-module-bays.csv"
        summary_path = output_dir / "summary.json"
        write_module_bay_import_csv(csv_path, report.module_bay_import_rows)
        summary_path.write_text(
            json.dumps(report.as_dict(), indent=2, sort_keys=True) + "\n",
            encoding="utf-8",
        )

        self.stdout.write(
            "Module readiness: "
            f"candidates={report.candidate_rows}, "
            f"existing_bay_rows={report.existing_bay_rows}, "
            f"missing_bay_rows={report.missing_bay_rows}, "
            f"missing_device_rows={report.missing_device_rows}, "
            f"unique_missing_bays={report.unique_missing_bays}"
        )
        self.stdout.write(f"Summary: {summary_path}")
        self.stdout.write(f"NetBox module-bay import CSV: {csv_path}")
        if report.ready:
            self.stdout.write(
                self.style.SUCCESS("Module import prerequisites are ready.")
            )
        else:
            self.stdout.write(
                self.style.WARNING(
                    "Missing module bays will be created in the native Branching diff "
                    "when dcim.module is enabled. Import the generated CSV first only "
                    "if you want to pre-stage bays through native NetBox import."
                )
            )

    def _resolve_sync_and_source(self, options):
        sync_name = str(options["sync_name"] or "").strip()
        if sync_name:
            try:
                sync = ForwardSync.objects.select_related("source").get(name=sync_name)
            except ForwardSync.DoesNotExist as exc:
                raise CommandError(
                    f"Forward Sync `{sync_name}` was not found."
                ) from exc
            return sync, sync.source

        source_name = str(options["source_name"] or "").strip()
        if not source_name:
            raise CommandError("Set --sync-name or --source-name.")
        try:
            source = ForwardSource.objects.get(name=source_name)
        except ForwardSource.DoesNotExist as exc:
            raise CommandError(
                f"Forward Source `{source_name}` was not found."
            ) from exc
        return None, source

    def _resolve_snapshot_id(self, client, *, network_id, snapshot_selector):
        if snapshot_selector != LATEST_PROCESSED_SNAPSHOT:
            return snapshot_selector
        return client.get_latest_processed_snapshot_id(network_id)

    def _fetch_module_rows(self, sync, client, *, network_id, snapshot_id):
        specs = get_query_specs("dcim.module", maps=sync.get_maps() if sync else None)
        if not specs:
            specs = [
                get_seeded_builtin_query_spec("dcim.module", "Forward Modules"),
            ]
        specs = resolve_query_specs_for_client(specs, client)

        rows = []
        for spec in specs:
            try:
                rows.extend(
                    client.run_nqe_query(
                        query=spec.query,
                        query_id=spec.run_query_id,
                        commit_id=spec.commit_id,
                        network_id=network_id,
                        snapshot_id=snapshot_id,
                        parameters=spec.merged_parameters(
                            sync.get_query_parameters() if sync else {}
                        ),
                        fetch_all=True,
                    )
                )
            except ForwardClientError as exc:
                raise CommandError(
                    f"Forward module readiness query `{spec.query_name}` failed: {exc}"
                ) from exc
        return rows

    def _output_dir(self, base_path, *, sync, source):
        label = sync.name if sync else source.name
        timestamp = timezone.now().strftime("%Y%m%dT%H%M%SZ")
        safe_label = slugify(label) or "forward-module-readiness"
        return Path(base_path) / f"{safe_label}-{timestamp}"
