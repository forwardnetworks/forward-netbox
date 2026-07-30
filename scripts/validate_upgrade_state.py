#!/usr/bin/env python3
"""Seed plugin rows under one release and read them back under the next.

`artifact-test` installs the wheel into an empty database. That proves a clean
install works; it proves nothing about an upgrade, which is how every existing
deployment actually receives a release. A migration that drops a column, a
default that does not backfill, or a field whose meaning changed all pass a
clean install and corrupt a real one.

So: `--mode seed` writes a fixed row set under the *previous* release, then
`--mode verify` re-reads it under the built wheel after `migrate` has run. The
values are constants defined here, not customer data, and the chain
(source -> sync -> ingestion -> issue) is deliberately made of plugin-owned
models with no NetBox core foreign keys, so the fixture cannot fail for reasons
unrelated to the upgrade.
"""
from __future__ import annotations

import argparse
import json
import os
import sys

sys.path.insert(0, "/opt/netbox/netbox")
os.environ.setdefault("DJANGO_SETTINGS_MODULE", "netbox.settings")

import django  # noqa: E402

django.setup()

from django.contrib.contenttypes.models import ContentType  # noqa: E402
from forward_netbox.models import (  # noqa: E402
    ForwardDriftPolicy,
    ForwardIngestion,
    ForwardIngestionIssue,
    ForwardNQEMap,
    ForwardSource,
    ForwardSync,
)

# Fixed, synthetic, and deliberately not customer-shaped.
SOURCE_NAME = "upgrade-fixture-source"
SOURCE_URL = "https://fwd.example.invalid"
SYNC_NAME = "upgrade-fixture-sync"
MAP_NAME = "upgrade-fixture-map"
# ForwardNQEMap.netbox_model is a ContentType FK, so this row also proves a
# cross-app relation survives the upgrade, not just plugin-local columns.
MAP_APP_LABEL = "dcim"
MAP_MODEL = "device"
POLICY_NAME = "upgrade-fixture-policy"
ISSUE_MESSAGE = "upgrade fixture issue"
ISSUE_EXCEPTION = "UpgradeFixtureError"


def _map_content_type():
    return ContentType.objects.get(app_label=MAP_APP_LABEL, model=MAP_MODEL)


def seed() -> dict:
    source, _ = ForwardSource.objects.get_or_create(
        name=SOURCE_NAME, defaults={"url": SOURCE_URL}
    )
    sync, _ = ForwardSync.objects.get_or_create(
        name=SYNC_NAME, defaults={"source": source}
    )
    ingestion = ForwardIngestion.objects.create(sync=sync)
    ForwardIngestionIssue.objects.create(
        ingestion=ingestion,
        message=ISSUE_MESSAGE,
        exception=ISSUE_EXCEPTION,
    )
    ForwardNQEMap.objects.get_or_create(
        name=MAP_NAME, defaults={"netbox_model": _map_content_type()}
    )
    ForwardDriftPolicy.objects.get_or_create(name=POLICY_NAME)
    return {"ingestion_id": ingestion.pk, "source_id": source.pk, "sync_id": sync.pk}


def verify() -> dict:
    failures = []

    source = ForwardSource.objects.filter(name=SOURCE_NAME).first()
    if source is None:
        failures.append("ForwardSource seeded before the upgrade is gone")
    elif source.url != SOURCE_URL:
        failures.append(f"ForwardSource.url changed to {source.url!r}")

    sync = ForwardSync.objects.filter(name=SYNC_NAME).first()
    if sync is None:
        failures.append("ForwardSync seeded before the upgrade is gone")
    elif source is not None and sync.source_id != source.pk:
        failures.append("ForwardSync.source no longer points at the seeded source")

    ingestion = (
        ForwardIngestion.objects.filter(sync=sync).order_by("pk").last()
        if sync is not None
        else None
    )
    if ingestion is None:
        failures.append("ForwardIngestion seeded before the upgrade is gone")

    issue = (
        ForwardIngestionIssue.objects.filter(ingestion=ingestion).first()
        if ingestion is not None
        else None
    )
    if issue is None:
        failures.append("ForwardIngestionIssue seeded before the upgrade is gone")
    else:
        if issue.message != ISSUE_MESSAGE:
            failures.append(
                f"ForwardIngestionIssue.message changed to {issue.message!r}"
            )
        if issue.exception != ISSUE_EXCEPTION:
            failures.append(
                f"ForwardIngestionIssue.exception changed to {issue.exception!r}"
            )

    nqe_map = ForwardNQEMap.objects.filter(name=MAP_NAME).first()
    if nqe_map is None:
        failures.append("ForwardNQEMap seeded before the upgrade is gone")
    elif nqe_map.netbox_model_id != _map_content_type().pk:
        failures.append(
            "ForwardNQEMap.netbox_model no longer resolves to "
            f"{MAP_APP_LABEL}.{MAP_MODEL}"
        )

    if not ForwardDriftPolicy.objects.filter(name=POLICY_NAME).exists():
        failures.append("ForwardDriftPolicy seeded before the upgrade is gone")

    if failures:
        raise SystemExit(
            "upgrade validation failed; rows written under the previous release "
            "did not survive the upgrade:\n  " + "\n  ".join(failures)
        )

    return {
        "ingestion_id": ingestion.pk,
        "source_id": source.pk,
        "sync_id": sync.pk,
        "verified": True,
    }


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--mode", choices=("seed", "verify"), required=True)
    arguments = parser.parse_args()
    result = seed() if arguments.mode == "seed" else verify()
    print(json.dumps({"mode": arguments.mode, **result}, sort_keys=True))


if __name__ == "__main__":
    main()
