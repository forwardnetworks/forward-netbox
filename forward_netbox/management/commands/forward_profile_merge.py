"""Profile real branch merges with anonymized production-shaped fixtures."""

import contextlib
import hashlib
import json
import logging
import os
import uuid
from pathlib import Path

from django.contrib.auth import get_user_model
from django.core.management.base import BaseCommand
from django.db import connection
from django.db import DatabaseError
from django.db import transaction
from django.test import RequestFactory
from django.urls import reverse
from netbox.context_managers import event_tracking
from netbox_branching.choices import BranchStatusChoices
from netbox_branching.models import Branch
from netbox_branching.utilities import activate_branch

from forward_netbox.models import ForwardIngestion
from forward_netbox.models import ForwardSource
from forward_netbox.models import ForwardSync
from forward_netbox.utilities.merge import merge_branch
from forward_netbox.utilities.merge_observability import begin_merge_attempt
from forward_netbox.utilities.merge_profiling import MergeProfileRecorder
from forward_netbox.utilities.merge_profiling import profile_scope


@contextlib.contextmanager
def _changediff_save_measured():
    """Attribute `ChangeDiff.save()` to its own scope for the merge.

    The per-row cost sits inside upstream's `ObjectChange.apply`, which the
    recorder already times as one opaque `objectchange_apply` bucket - so
    "is branching 1.1.3's ChangeDiff saving measurable on a real merge"
    could not be answered from a profile. Wrapping the method here rather
    than in the merge path keeps production untouched: this is a profiling
    command and the instrumentation belongs with the measurement.
    """
    from netbox_branching.models import ChangeDiff

    original = ChangeDiff.save

    def measured(self, *args, **kwargs):
        with profile_scope(
            "changediff_save",
            owner="upstream_netbox_branching",
            rows=1,
        ):
            return original(self, *args, **kwargs)

    ChangeDiff.save = measured
    try:
        yield
    finally:
        ChangeDiff.save = original


LOG = logging.getLogger("forward_netbox.merge_profile")
STAGE_CHUNK_SIZE = 500

# Normalized sample of the dominant baseline core rows. The source proportions
# cover roughly 84% of the observed baseline; optional third-party models and
# cables are deliberately excluded rather than synthesized inaccurately.
FIXTURE_WEIGHTS = (
    ("dcim.device", 0.006),
    ("dcim.interface", 0.540),
    ("dcim.macaddress", 0.280),
    ("dcim.inventoryitem", 0.084),
    ("ipam.ipaddress", 0.053),
    ("ipam.prefix", 0.036),
    ("dcim.site", 0.001),
    # A tree model, so `_is_bulk_safe` (bulk_merge.py) always refuses it and
    # the row goes through the per-object upstream fallback - the only path
    # that reaches ChangeDiff.save(). Without it this fixture is CREATE-only
    # and bulk-safe, so it measured everything EXCEPT the cost in question.
    ("dcim.region", 0.002),
)


def _allocated_counts(volume):
    counts = {model: int(volume * weight) for model, weight in FIXTURE_WEIGHTS}
    counts["dcim.device"] = max(1, counts["dcim.device"])
    counts["dcim.site"] = max(1, counts["dcim.site"])
    counts["dcim.region"] = max(1, counts["dcim.region"])
    assigned = sum(counts.values())
    counts["dcim.interface"] += volume - assigned
    return counts


def _postgres_snapshot():
    with connection.cursor() as cursor:
        cursor.execute("SELECT pg_backend_pid()")
        backend_pid = int(cursor.fetchone()[0])
        try:
            cursor.execute("SELECT wal_bytes::bigint FROM pg_stat_wal")
            wal_bytes = int(cursor.fetchone()[0])
        except DatabaseError:  # pragma: no cover - runtime permissions/version
            wal_bytes = None
        cursor.execute(
            "SELECT COALESCE(blk_read_time, 0), COALESCE(blk_write_time, 0), "
            "COALESCE(temp_bytes, 0) FROM pg_stat_database "
            "WHERE datname = current_database()"
        )
        block_read_ms, block_write_ms, temp_bytes = cursor.fetchone()
    return {
        "backend_pid": backend_pid,
        "wal_bytes": wal_bytes,
        "block_read_ms": float(block_read_ms),
        "block_write_ms": float(block_write_ms),
        "temp_bytes": int(temp_bytes),
    }


def _snapshot_delta(before, after):
    result = {"backend_pid": after["backend_pid"]}
    for key in ("wal_bytes", "block_read_ms", "block_write_ms", "temp_bytes"):
        if before.get(key) is None or after.get(key) is None:
            result[key] = None
        else:
            result[key] = after[key] - before[key]
    return result


class Command(BaseCommand):
    help = (
        "Profile merge cost at two or more volumes using anonymous real-model "
        "branch fixtures; writes one fsync'd JSON object per completed round."
    )

    def add_arguments(self, parser):
        parser.add_argument("--volumes", default="1000,5000")
        parser.add_argument("--rounds", type=int, default=3)
        parser.add_argument("--output", required=True)
        parser.add_argument(
            "--i-understand-this-creates-test-data",
            action="store_true",
            dest="confirmed",
        )

    def handle(self, *args, **options):
        if not options["confirmed"]:
            raise RuntimeError(
                "Refusing to create profiling fixtures without "
                "--i-understand-this-creates-test-data."
            )
        volumes = [
            int(item.strip())
            for item in str(options["volumes"]).split(",")
            if item.strip()
        ]
        if len(volumes) < 2 or any(volume < 10 for volume in volumes):
            raise RuntimeError("Provide at least two comma-separated volumes >= 10.")
        rounds = max(2, int(options["rounds"]))
        output_path = Path(options["output"])
        output_path.parent.mkdir(parents=True, exist_ok=True)

        user, _ = get_user_model().objects.get_or_create(
            username="forward-merge-profiler",
            defaults={"is_superuser": True},
        )
        request = RequestFactory().get(reverse("home"))
        request.user = user

        for volume in volumes:
            for round_number in range(1, rounds + 1):
                result = self._run_round(
                    user=user,
                    request=request,
                    volume=volume,
                    round_number=round_number,
                )
                encoded = json.dumps(result, sort_keys=True, default=str)
                with output_path.open("a", encoding="utf-8") as handle:
                    handle.write(encoded + "\n")
                    handle.flush()
                    os.fsync(handle.fileno())
                self.stdout.write(
                    "PROFILE_CHECKPOINT "
                    f"volume={volume} round={round_number} "
                    f"wall={result['wall_seconds']:.3f}s "
                    f"rate={result['changes_per_second']:.2f}/s "
                    f"statements_per_change={result['statements_per_change']:.4f}"
                )

    def _run_round(self, *, user, request, volume, round_number):
        from dcim.models import (
            Device,
            DeviceRole,
            DeviceType,
            Interface,
            InventoryItem,
            MACAddress,
            Manufacturer,
            Region,
            Site,
        )
        from ipam.models import IPAddress, Prefix

        token = uuid.uuid4().hex
        short = token[:12]
        counts = _allocated_counts(volume)
        manufacturer = Manufacturer.objects.create(
            name=f"Merge Profile Manufacturer {short}",
            slug=f"merge-profile-mfr-{short}",
        )
        device_type = DeviceType.objects.create(
            manufacturer=manufacturer,
            model=f"Merge Profile Model {short}",
            slug=f"merge-profile-model-{short}",
        )
        role = DeviceRole.objects.create(
            name=f"Merge Profile Role {short}",
            slug=f"merge-profile-role-{short}",
        )
        main_site = Site.objects.create(
            name=f"Merge Profile Main Site {short}",
            slug=f"merge-profile-main-site-{short}",
        )
        source = ForwardSource.objects.create(
            name=f"Merge Profile Source {short}",
            type="saas",
            url="https://profile.invalid",
            parameters={"network_id": f"anonymous-{short}"},
        )
        sync = ForwardSync.objects.create(
            name=f"Merge Profile Sync {short}",
            source=source,
            user=user,
            parameters={"snapshot_id": f"anonymous-{short}"},
        )
        branch = Branch(name=f"Merge Profile {short}")
        branch.save(provision=False)
        branch.provision(user=user)
        branch.refresh_from_db()

        devices = self._stage_rows(
            branch,
            request,
            counts["dcim.device"],
            lambda index: Device.objects.create(
                name=f"profile-device-{short}-{index}",
                device_type=device_type,
                role=role,
                site=main_site,
                status="active",
            ),
        )
        interfaces = self._stage_rows(
            branch,
            request,
            counts["dcim.interface"],
            lambda index: Interface.objects.create(
                device=devices[index % len(devices)],
                name=f"Ethernet{index}",
                type="1000base-t",
                enabled=True,
            ),
        )
        self._stage_rows(
            branch,
            request,
            counts["dcim.macaddress"],
            lambda index: MACAddress.objects.create(
                mac_address=self._mac_value(token, index),
                assigned_object=interfaces[index % len(interfaces)],
            ),
        )
        self._stage_rows(
            branch,
            request,
            counts["dcim.inventoryitem"],
            lambda index: InventoryItem.objects.create(
                device=devices[index % len(devices)],
                name=f"Profile Component {index}",
            ),
        )
        self._stage_rows(
            branch,
            request,
            counts["ipam.ipaddress"],
            lambda index: IPAddress.objects.create(
                address=f"2001:db8:{token[:4]}:{token[4:8]}::{index + 1}/128",
                status="active",
                assigned_object=interfaces[index % len(interfaces)],
            ),
        )
        self._stage_rows(
            branch,
            request,
            counts["ipam.prefix"],
            lambda index: Prefix.objects.create(
                prefix=f"2001:db8:{token[8:12]}:{token[12:16]}::{index + 1}/128",
                status="active",
            ),
        )
        self._stage_rows(
            branch,
            request,
            counts["dcim.site"],
            lambda index: Site.objects.create(
                name=f"Profile Branch Site {short} {index}",
                slug=f"profile-branch-site-{short}-{index}",
            ),
        )

        self._stage_rows(
            branch,
            request,
            counts["dcim.region"],
            lambda index: Region.objects.create(
                name=f"Profile Branch Region {short} {index}",
                slug=f"profile-branch-region-{short}-{index}",
            ),
        )

        Branch.objects.filter(pk=branch.pk).update(status=BranchStatusChoices.READY)
        branch.refresh_from_db()
        staged_changes = branch.get_unmerged_changes().count()
        if staged_changes != volume:
            raise RuntimeError(
                f"Fixture staged {staged_changes} changes; expected {volume}."
            )
        ingestion = ForwardIngestion.objects.create(
            sync=sync,
            snapshot_selector=f"anonymous-{short}",
            snapshot_id=f"anonymous-{short}",
            branch=branch,
        )
        attempt = begin_merge_attempt(ingestion)
        before_postgres = _postgres_snapshot()
        recorder = MergeProfileRecorder(
            metadata={
                "fixture": "anonymized-production-shaped-core",
                "fixture_coverage_note": (
                    "Normalized dominant core models representing about 84% of "
                    "the measured baseline rows; optional plugin models and cables "
                    "are not represented."
                ),
                "volume": volume,
                "round": round_number,
                "model_counts": counts,
                "staged_changes": staged_changes,
            }
        )
        with recorder.activate(), _changediff_save_measured():
            merge_branch(
                ingestion,
                user=user,
                merge_attempt=attempt,
            )
        after_postgres = _postgres_snapshot()
        result = recorder.result()
        result["postgres"] = _snapshot_delta(before_postgres, after_postgres)
        result["changes_per_second"] = (
            volume / result["wall_seconds"] if result["wall_seconds"] else 0.0
        )
        result["statements_per_change"] = result["statements"] / volume
        result["db_round_trips_per_change"] = result["statements_per_change"]
        result["peak_rss_mib"] = result["peak_rss_bytes"] / 1024 / 1024
        return result

    @staticmethod
    def _stage_rows(branch, request, count, factory):
        objects = []
        for start in range(0, count, STAGE_CHUNK_SIZE):
            with transaction.atomic(), activate_branch(branch), event_tracking(request):
                request.id = uuid.uuid4()
                for index in range(start, min(count, start + STAGE_CHUNK_SIZE)):
                    objects.append(factory(index))
        return objects

    @staticmethod
    def _mac_value(token, index):
        digest = hashlib.sha256(f"{token}:{index}".encode()).digest()
        return ":".join(f"{value:02x}" for value in bytes([0x02]) + digest[:5])
