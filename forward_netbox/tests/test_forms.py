from django.contrib.contenttypes.models import ContentType
from django.test import TestCase

from forward_netbox.forms import ForwardNQEQueryForm
from forward_netbox.forms import ForwardSyncForm
from forward_netbox.models import ForwardSnapshot
from forward_netbox.models import ForwardSource
from forward_netbox.models import ForwardSync
from forward_netbox.models import ForwardNQEQuery
from forward_netbox.utilities.nqe_map import get_default_nqe_map


class ForwardSyncFormNQEMapTest(TestCase):
    def setUp(self) -> None:
        self.source = ForwardSource.objects.create(
            name="forward",
            url="https://forward.local",
            parameters={"auth": "token", "verify": False},
        )
        self.snapshot = ForwardSnapshot.objects.create(
            name="snapshot",
            source=self.source,
            snapshot_id="$last",
            status="loaded",
            data={"sites": ["site-a"]},
        )

        self.default_map = get_default_nqe_map()

    def _base_form_data(self) -> dict:
        data = {
            "name": "Sync",
            "source": self.source.pk,
            "snapshot_data": self.snapshot.pk,
            "sites": ["site-a"],
            "auto_merge": False,
            "update_custom_fields": False,
        }

        # Enable all supported models
        for model_key in self.default_map.keys():
            short_key = model_key.split(".", 1)[1]
            data[f"fwd_{short_key}"] = "on"

        # Populate NQE query fields
        for model_key, meta in self.default_map.items():
            field_name = f"nqe__{model_key.replace('.', '__')}"
            data[field_name] = meta.get("query_id", "")

        return data

    def test_form_saves_nqe_map_parameters(self):
        form = ForwardSyncForm(data=self._base_form_data())
        self.assertTrue(form.is_valid(), form.errors)
        sync: ForwardSync = form.save()

        nqe_map = sync.parameters.get("nqe_map")
        self.assertIsNotNone(nqe_map)
        for model_key, meta in self.default_map.items():
            self.assertIn(model_key, nqe_map)
            self.assertEqual(nqe_map[model_key]["query_id"], meta.get("query_id"))
            self.assertTrue(nqe_map[model_key]["enabled"])

    def test_get_nqe_map_merges_overrides(self):
        form_data = self._base_form_data()
        override_field = "nqe__dcim__device"
        form_data[override_field] = "FQ_override"

        form = ForwardSyncForm(data=form_data)
        self.assertTrue(form.is_valid(), form.errors)
        sync = form.save()

        effective_map = sync.get_nqe_map()
        self.assertEqual(
            effective_map["dcim.device"]["query_id"],
            "FQ_override",
        )


class ForwardNQEQueryFormTest(TestCase):
    def setUp(self) -> None:
        self.content_type = ContentType.objects.get(app_label="dcim", model="device")
        self.instance = ForwardNQEQuery.objects.create(
            content_type=self.content_type,
            query_id="FQ_original",
            enabled=True,
        )

    def test_update_query(self):
        form = ForwardNQEQueryForm(
            data={
                "content_type": self.content_type.pk,
                "query_id": "FQ_updated",
                "enabled": False,
                "description": "Updated query",
            },
            instance=self.instance,
        )
        self.assertTrue(form.is_valid(), form.errors)
        obj = form.save()
        self.assertEqual(obj.query_id, "FQ_updated")
        self.assertFalse(obj.enabled)
