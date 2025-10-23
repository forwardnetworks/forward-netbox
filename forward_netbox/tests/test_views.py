from uuid import uuid4

from core.choices import DataSourceStatusChoices
from core.choices import JobStatusChoices
from core.models import Job
from django.contrib.contenttypes.models import ContentType
from django.utils import timezone
from utilities.testing import ViewTestCases
from utilities.testing.views import ModelViewTestCase

from forward_netbox.models import ForwardNQEQuery
from forward_netbox.models import ForwardSnapshot
from forward_netbox.models import ForwardSource
from forward_netbox.choices import ForwardSnapshotStatusModelChoices


class PluginViewTestMixin(ModelViewTestCase):
    """Mixin that rewrites reverse() lookups for plugin URLs."""

    maxDiff = 1000

    def _get_base_url(self):
        return f"plugins:forward_netbox:{self.model._meta.model_name}_{{}}"


class ForwardSourceViewTestCase(
    PluginViewTestMixin,
    ViewTestCases.GetObjectViewTestCase,
    ViewTestCases.GetObjectChangelogViewTestCase,
    ViewTestCases.CreateObjectViewTestCase,
    ViewTestCases.EditObjectViewTestCase,
    ViewTestCases.DeleteObjectViewTestCase,
    ViewTestCases.ListObjectsViewTestCase,
    ViewTestCases.BulkDeleteObjectsViewTestCase,
):
    model = ForwardSource
    validation_excluded_fields = ["deployment_mode", "access_key", "secret_key"]

    @classmethod
    def setUpTestData(cls):
        parameters = {"auth": "token", "verify": False, "deployment_mode": "saas"}
        for idx in range(1, 4):
            ForwardSource.objects.create(
                name=f"Forward Source {idx}",
                url="https://fwd.app",
                network_id=f"net-{idx}",
                status=DataSourceStatusChoices.NEW,
                parameters=parameters,
            )

        cls.form_data = {
            "name": "Forward Source Created",
            "deployment_mode": "saas",
            "network_id": "net-created",
            "access_key": "user",
            "secret_key": "pass",
        }


class ForwardSnapshotViewTestCase(
    PluginViewTestMixin,
    ViewTestCases.GetObjectViewTestCase,
    ViewTestCases.GetObjectChangelogViewTestCase,
    ViewTestCases.DeleteObjectViewTestCase,
    ViewTestCases.ListObjectsViewTestCase,
    ViewTestCases.BulkDeleteObjectsViewTestCase,
):
    model = ForwardSnapshot

    @classmethod
    def setUpTestData(cls):
        source = ForwardSource.objects.create(
            name="Forward Source Snapshots",
            url="https://fwd.app",
            network_id="net-snapshots",
            status=DataSourceStatusChoices.NEW,
            parameters={"auth": "token", "verify": False, "deployment_mode": "saas"},
        )

        for idx in range(1, 4):
            ForwardSnapshot.objects.create(
                name=f"Snapshot {idx}",
                source=source,
                snapshot_id=f"$snap-{idx}",
                status=ForwardSnapshotStatusModelChoices.STATUS_LOADED,
                data={"sites": []},
                date=timezone.now(),
            )

        # Populate job history for changelog view
        instance = ForwardSnapshot.objects.first()
        Job.objects.create(
            object_id=instance.pk,
            object_type=ContentType.objects.get_for_model(ForwardSnapshot),
            name="Snapshot job",
            job_id=uuid4(),
            status=JobStatusChoices.STATUS_COMPLETED,
            completed=timezone.now(),
            created=timezone.now(),
        )


class ForwardNQEQueryViewTestCase(
    PluginViewTestMixin,
    ViewTestCases.GetObjectViewTestCase,
    ViewTestCases.GetObjectChangelogViewTestCase,
    ViewTestCases.CreateObjectViewTestCase,
    ViewTestCases.EditObjectViewTestCase,
    ViewTestCases.DeleteObjectViewTestCase,
    ViewTestCases.ListObjectsViewTestCase,
    ViewTestCases.BulkDeleteObjectsViewTestCase,
):
    model = ForwardNQEQuery
    validation_excluded_fields = ["content_type"]

    @classmethod
    def setUpTestData(cls):
        ForwardNQEQuery.objects.all().delete()
        cls.ct_manufacturer = ContentType.objects.get(app_label="dcim", model="manufacturer")
        cls.ct_interface = ContentType.objects.get(app_label="dcim", model="interface")
        cls.ct_device = ContentType.objects.get(app_label="dcim", model="device")

        ForwardNQEQuery.objects.create(
            content_type=cls.ct_manufacturer,
            query_id="FQ_vendor",
            enabled=True,
        )
        ForwardNQEQuery.objects.create(
            content_type=cls.ct_interface,
            query_id="FQ_interface",
            enabled=True,
        )

        cls.form_data = {
            "content_type": cls.ct_manufacturer.pk,
            "query_id": "FQ_updated",
            "enabled": False,
            "description": "Updated manufacturer query",
        }
        cls.create_data = {
            "content_type": cls.ct_device.pk,
            "query_id": "FQ_device",
            "enabled": True,
            "description": "Device query",
        }

    def test_create_object_with_permission(self):
        original_form_data = self.form_data
        try:
            self.form_data = self.create_data.copy()
            super().test_create_object_with_permission()
        finally:
            self.form_data = original_form_data

    def test_create_object_with_constrained_permission(self):
        original_form_data = self.form_data
        try:
            self.form_data = self.create_data.copy()
            super().test_create_object_with_constrained_permission()
        finally:
            self.form_data = original_form_data
