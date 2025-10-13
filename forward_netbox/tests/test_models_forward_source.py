from django.test import TestCase

from forward_netbox.models import ForwardSource
from forward_netbox.choices import ForwardSourceTypeChoices


class ForwardSourceModelTest(TestCase):
    def setUp(self):
        self.base_kwargs = {
            "name": "Forward Local",
            "type": ForwardSourceTypeChoices.LOCAL,
            "url": "https://forward.example.com",
        }

    def test_create_source_with_network_id(self):
        source = ForwardSource.objects.create(network_id="net-12345", **self.base_kwargs)
        self.assertEqual(source.network_id, "net-12345")

    def test_update_source_network_id(self):
        source = ForwardSource.objects.create(**self.base_kwargs)
        source.network_id = "net-67890"
        source.save()
        source.refresh_from_db()
        self.assertEqual(source.network_id, "net-67890")

    def test_delete_source_with_network_id(self):
        source = ForwardSource.objects.create(network_id="net-abc", **self.base_kwargs)
        source_pk = source.pk
        source.delete()
        self.assertFalse(ForwardSource.objects.filter(pk=source_pk).exists())
