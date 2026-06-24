from datetime import timedelta
from uuid import uuid4

from core.choices import JobStatusChoices
from core.models import Job
from django.contrib.contenttypes.models import ContentType
from django.test import TestCase
from django.utils import timezone

from forward_netbox.models import ForwardSource
from forward_netbox.models import ForwardSync
from forward_netbox.tests import scenarios
from forward_netbox.utilities.forward_api import LATEST_PROCESSED_SNAPSHOT
from forward_netbox.utilities.job_compat import ensure_core_job_compat_defaults


class SyntheticSyncScenarioHarnessTest(TestCase):
    def setUp(self):
        self.source = ForwardSource.objects.create(
            name="source-synthetic-scenarios",
            type="saas",
            url="https://fwd.app",
            parameters=scenarios.source_parameters(),
        )
        self.sync = ForwardSync.objects.create(
            name="sync-synthetic-scenarios",
            source=self.source,
            auto_merge=True,
            parameters={
                "snapshot_id": LATEST_PROCESSED_SNAPSHOT,
                "dcim.site": True,
                "enable_bulk_orm": False,
            },
        )

    def _job(self, *, instance, status=JobStatusChoices.STATUS_PENDING, completed=None):
        ensure_core_job_compat_defaults()
        started = (
            completed - timedelta(seconds=5)
            if completed is not None
            else timezone.now()
        )
        values = {
            "object_type": ContentType.objects.get_for_model(instance),
            "object_id": instance.pk,
            "name": f"synthetic {instance._meta.model_name} job",
            "status": status,
            "job_id": uuid4(),
            "created": timezone.now(),
            "started": started,
            "completed": completed,
            "data": {},
        }
        if any(field.name == "notifications" for field in Job._meta.fields):
            values["notifications"] = []
        return Job.objects.create(**values)
