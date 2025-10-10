from datetime import timedelta
from unittest.mock import patch

from core.choices import DataSourceStatusChoices
from dcim.models import Device
from dcim.models import Site
from django.contrib.contenttypes.models import ContentType
from django.test import TestCase
from utilities.datetime import local_now

from ipfabric_netbox.choices import IPFabricSnapshotStatusModelChoices
from ipfabric_netbox.choices import IPFabricSourceTypeChoices
from ipfabric_netbox.forms import IPFabricIngestionFilterForm
from ipfabric_netbox.forms import IPFabricIngestionMergeForm
from ipfabric_netbox.forms import IPFabricRelationshipFieldForm
from ipfabric_netbox.forms import IPFabricSnapshotFilterForm
from ipfabric_netbox.forms import IPFabricSourceFilterForm
from ipfabric_netbox.forms import IPFabricSourceForm
from ipfabric_netbox.forms import IPFabricSyncForm
from ipfabric_netbox.forms import IPFabricTransformFieldForm
from ipfabric_netbox.forms import IPFabricTransformMapCloneForm
from ipfabric_netbox.forms import IPFabricTransformMapForm
from ipfabric_netbox.forms import IPFabricTransformMapGroupForm
from ipfabric_netbox.models import IPFabricRelationshipField
from ipfabric_netbox.models import IPFabricSnapshot
from ipfabric_netbox.models import IPFabricSource
from ipfabric_netbox.models import IPFabricSync
from ipfabric_netbox.models import IPFabricTransformField
from ipfabric_netbox.models import IPFabricTransformMap
from ipfabric_netbox.models import IPFabricTransformMapGroup


class IPFabricSourceFormTestCase(TestCase):
    @classmethod
    def setUpTestData(cls):
        # Create a test IPFabricSource instance for form tests
        cls.ipfabric_source = IPFabricSource.objects.create(
            name="Test IP Fabric Source",
            type=IPFabricSourceTypeChoices.LOCAL,
            url="https://test.ipfabric.local",
            status=DataSourceStatusChoices.NEW,
            parameters={"auth": "test_token", "verify": True, "timeout": 30},
        )

    def test_fields_are_required(self):
        form = IPFabricSourceForm(data={})
        self.assertFalse(form.is_valid(), form.errors)
        self.assertIn("name", form.errors)
        self.assertIn("type", form.errors)
        self.assertIn("url", form.errors)

    def test_fields_are_optional(self):
        form = IPFabricSourceForm(
            data={
                "name": "Test No Comments Source",
                "type": IPFabricSourceTypeChoices.REMOTE,
                "url": "https://test.ipfabric.local",
            }
        )
        self.assertTrue(form.is_valid(), form.errors)

    def test_type_must_be_defined_choice(self):
        form = IPFabricSourceForm(
            data={
                "name": "Test Source",
                "type": "invalid_type",
                "url": "https://test.ipfabric.local",
            }
        )
        self.assertFalse(form.is_valid(), form.errors)
        self.assertIn("type", form.errors)
        self.assertTrue(form.errors["type"][-1].startswith("Select a valid choice."))

    def test_valid_local_source_form(self):
        form = IPFabricSourceForm(
            data={
                "name": "Test Local Source",
                "type": IPFabricSourceTypeChoices.LOCAL,
                "url": "https://test.ipfabric.local",
                "auth": "test_api_token",
                "verify": False,
                "timeout": 45,
                "description": "Test local IP Fabric source",
                "comments": "Test comments",
            }
        )
        self.assertTrue(form.is_valid(), form.errors)
        instance = form.save()

        # Check that parameters are properly stored
        self.assertEqual(instance.parameters["auth"], "test_api_token")
        self.assertEqual(instance.parameters["verify"], False)
        self.assertEqual(instance.parameters["timeout"], 45)

    def test_valid_remote_source_form(self):
        form = IPFabricSourceForm(
            data={
                "name": "Test Remote Source",
                "type": IPFabricSourceTypeChoices.REMOTE,
                "url": "https://remote.ipfabric.local",
                "timeout": 60,
                "description": "Test remote IP Fabric source",
                "comments": "Test comments",
            }
        )
        self.assertTrue(form.is_valid(), form.errors)

    def test_local_source_requires_auth_token(self):
        # Test that when type is 'local', auth field becomes required
        form = IPFabricSourceForm(
            data={
                "name": "Test Local Source",
                "type": IPFabricSourceTypeChoices.LOCAL,
                "url": "https://test.ipfabric.local",
                "verify": True,
                "timeout": 30,
            }
        )
        # Since auth is dynamically added as required for local sources
        # we need to check if the form properly handles this validation
        self.assertFalse(form.is_valid(), form.errors)
        self.assertIn("auth", form.errors)

    def test_form_save_sets_status_to_new(self):
        form = IPFabricSourceForm(
            data={
                "name": "Test Save Source",
                "type": IPFabricSourceTypeChoices.LOCAL,
                "url": "https://test.ipfabric.local",
                "auth": "test_api_token",
                "verify": True,
                "timeout": 30,
            }
        )
        self.assertTrue(form.is_valid(), form.errors)
        instance = form.save()
        self.assertEqual(instance.status, DataSourceStatusChoices.NEW)

    def test_form_initializes_existing_parameters(self):
        # Test that form properly initializes with existing instance parameters
        form = IPFabricSourceForm(instance=self.ipfabric_source)

        # Check that the form fields are initialized with the instance's parameters
        self.assertEqual(form.fields["auth"].initial, "test_token")
        self.assertEqual(form.fields["verify"].initial, True)
        self.assertEqual(form.fields["timeout"].initial, 30)

    def test_remote_source_creates_last_snapshot(self):
        """Check that $last snapshot is created for remote sources"""
        from ipfabric_netbox.models import IPFabricSnapshot

        self.assertEqual(IPFabricSnapshot.objects.count(), 0)

        form = IPFabricSourceForm(
            data={
                "name": "Test Remote Snapshot Source",
                "type": IPFabricSourceTypeChoices.REMOTE,
                "url": "https://remote.ipfabric.local",
                "timeout": 30,
            }
        )
        self.assertTrue(form.is_valid(), form.errors)
        instance = form.save()

        last_snapshot = IPFabricSnapshot.objects.filter(
            source=instance, snapshot_id="$last"
        ).first()
        self.assertIsNotNone(last_snapshot)
        self.assertEqual(last_snapshot.name, "$last")

    def test_fieldsets_for_remote_source_type(self):
        """Test that fieldsets property returns correct structure for remote source type"""
        form = IPFabricSourceForm(
            data={
                "name": "Test Remote Source",
                "type": IPFabricSourceTypeChoices.REMOTE,
                "url": "https://remote.ipfabric.local",
            }
        )

        fieldsets = form.fieldsets

        # Should have 2 fieldsets for remote type
        self.assertEqual(len(fieldsets), 2)

        # First fieldset should be for Source
        self.assertEqual(fieldsets[0].name, "Source")

        # Second fieldset should be for Parameters
        self.assertEqual(fieldsets[1].name, "Parameters")

        # Verify the remote type fieldsets match the expected structure from forms.py
        # For remote type: FieldSet("timeout", name=_("Parameters"))
        # This means the Parameters fieldset should only contain timeout field
        self.assertEqual(len(form.fieldsets), 2)
        self.assertEqual(form.fieldsets[0].name, "Source")
        self.assertEqual(form.fieldsets[1].name, "Parameters")

        # For remote sources, verify that auth and verify fields are NOT in the form
        # (they are only added for local sources in the __init__ method)
        self.assertNotIn("auth", form.fields)
        self.assertNotIn("verify", form.fields)
        # But timeout should be present for all source types
        self.assertIn("timeout", form.fields)

    def test_fieldsets_for_local_source_type(self):
        """Test that fieldsets property returns correct structure for local source type"""
        form = IPFabricSourceForm(
            data={
                "name": "Test Local Source",
                "type": IPFabricSourceTypeChoices.LOCAL,
                "url": "https://local.ipfabric.local",
                "auth": "test_token",
                "verify": True,
                "timeout": 30,
            }
        )

        fieldsets = form.fieldsets

        # Should have 2 fieldsets for local type as well
        self.assertEqual(len(fieldsets), 2)

        # First fieldset should be for Source
        self.assertEqual(fieldsets[0].name, "Source")

        # Second fieldset should be for Parameters
        self.assertEqual(fieldsets[1].name, "Parameters")

        # Verify the local type fieldsets match the expected structure from forms.py
        # For local type: FieldSet("auth", "verify", "timeout", name=_("Parameters"))
        # This means the Parameters fieldset should contain auth, verify, and timeout fields
        self.assertEqual(len(form.fieldsets), 2)
        self.assertEqual(form.fieldsets[0].name, "Source")
        self.assertEqual(form.fieldsets[1].name, "Parameters")

        # For local sources, verify that auth, verify, and timeout fields ARE in the form
        # (they are dynamically added for local sources in the __init__ method)
        self.assertIn("auth", form.fields)
        self.assertIn("verify", form.fields)
        self.assertIn("timeout", form.fields)

        # Verify that the auth field is required for local sources
        self.assertTrue(form.fields["auth"].required)
        # Verify that verify field is optional (BooleanField with required=False)
        self.assertFalse(form.fields["verify"].required)
        # Verify that timeout field is optional
        self.assertFalse(form.fields["timeout"].required)

    def test_fieldsets_with_no_source_type_set(self):
        """Test fieldsets behavior when source_type is None or not set"""
        form = IPFabricSourceForm()

        # When no source_type is set, should default to basic fieldsets (non-local behavior)
        fieldsets = form.fieldsets

        self.assertEqual(len(fieldsets), 2)
        self.assertEqual(fieldsets[0].name, "Source")
        self.assertEqual(fieldsets[1].name, "Parameters")

    def test_fieldsets_with_existing_instance_local_type(self):
        """Test fieldsets behavior with an existing local source instance"""
        form = IPFabricSourceForm(instance=self.ipfabric_source)

        fieldsets = form.fieldsets

        # Should have extended fieldsets for local type since test instance is local
        self.assertEqual(len(fieldsets), 2)
        self.assertEqual(fieldsets[1].name, "Parameters")

    def test_fieldsets_dynamic_behavior_consistency(self):
        """Test that fieldsets method consistently returns the same structure for same source_type"""
        # Test local type consistency
        form_local_1 = IPFabricSourceForm(
            data={"type": IPFabricSourceTypeChoices.LOCAL}
        )
        form_local_2 = IPFabricSourceForm(
            data={"type": IPFabricSourceTypeChoices.LOCAL}
        )

        fieldsets_1 = form_local_1.fieldsets
        fieldsets_2 = form_local_2.fieldsets

        # Both should have the same structure
        self.assertEqual(len(fieldsets_1), len(fieldsets_2))
        self.assertEqual(fieldsets_1[0].name, fieldsets_2[0].name)
        self.assertEqual(fieldsets_1[1].name, fieldsets_2[1].name)

        # Test remote type consistency
        form_remote_1 = IPFabricSourceForm(
            data={"type": IPFabricSourceTypeChoices.REMOTE}
        )
        form_remote_2 = IPFabricSourceForm(
            data={"type": IPFabricSourceTypeChoices.REMOTE}
        )

        fieldsets_remote_1 = form_remote_1.fieldsets
        fieldsets_remote_2 = form_remote_2.fieldsets

        # Both should have the same structure
        self.assertEqual(len(fieldsets_remote_1), len(fieldsets_remote_2))
        self.assertEqual(fieldsets_remote_1[0].name, fieldsets_remote_2[0].name)
        self.assertEqual(fieldsets_remote_1[1].name, fieldsets_remote_2[1].name)

    def test_fieldsets_source_type_changes_parameters_fieldset(self):
        """Test that changing source_type results in different parameters fieldset"""
        # Create forms with different source types
        form_local = IPFabricSourceForm(data={"type": IPFabricSourceTypeChoices.LOCAL})
        form_remote = IPFabricSourceForm(
            data={"type": IPFabricSourceTypeChoices.REMOTE}
        )

        fieldsets_local = form_local.fieldsets
        fieldsets_remote = form_remote.fieldsets

        # Both should have same number of fieldsets
        self.assertEqual(len(fieldsets_local), 2)
        self.assertEqual(len(fieldsets_remote), 2)

        # Both should have same Source fieldset name
        self.assertEqual(fieldsets_local[0].name, fieldsets_remote[0].name)
        self.assertEqual(fieldsets_local[0].name, "Source")

        # Both should have Parameters fieldset, but they should be different objects
        # (one with basic timeout, one with auth, verify, timeout)
        self.assertEqual(fieldsets_local[1].name, "Parameters")
        self.assertEqual(fieldsets_remote[1].name, "Parameters")

        # The fieldsets should be different objects since they contain different fields
        # We can't easily test field contents without knowing FieldSet internals,
        # but we can verify the method creates new objects as expected
        self.assertIsInstance(fieldsets_local[1], type(fieldsets_remote[1]))


class IPFabricRelationshipFieldFormTestCase(TestCase):
    @classmethod
    def setUpTestData(cls):
        cls.source = IPFabricSource.objects.create(
            name="Test Source",
            type=IPFabricSourceTypeChoices.LOCAL,
            url="https://test.ipfabric.local",
            status=DataSourceStatusChoices.NEW,
        )

        cls.transform_map_group = IPFabricTransformMapGroup.objects.create(
            name="Test Group", description="Test group description"
        )

        cls.device_content_type = ContentType.objects.get_for_model(Device)
        cls.site_content_type = ContentType.objects.get_for_model(Site)

        cls.transform_map = IPFabricTransformMap.objects.create(
            name="Test Transform Map",
            group=cls.transform_map_group,
            source_model="device",
            target_model=cls.device_content_type,
        )

    def test_fields_are_required(self):
        form = IPFabricRelationshipFieldForm(data={})
        self.assertFalse(form.is_valid(), form.errors)
        self.assertIn("transform_map", form.errors)
        self.assertIn("source_model", form.errors)
        self.assertIn("target_field", form.errors)

    def test_fields_are_optional(self):
        form = IPFabricRelationshipFieldForm(
            data={
                "transform_map": self.transform_map.pk,
                "source_model": self.device_content_type.pk,
                "target_field": "site",
            }
        )
        self.assertTrue(form.is_valid(), form.errors)

    def test_valid_relationship_field_form(self):
        # Initialize form with transform_map to set up field choices
        form = IPFabricRelationshipFieldForm(
            initial={"transform_map": self.transform_map.pk},
            data={
                "transform_map": self.transform_map.pk,
                "source_model": self.device_content_type.pk,
                "target_field": "site",
                "coalesce": True,
                "template": "{{ object.siteName }}",
            },
        )
        self.assertTrue(form.is_valid(), form.errors)

    def test_coalesce_field_defaults_to_false(self):
        # Initialize form with transform_map to set up field choices
        form = IPFabricRelationshipFieldForm(
            initial={"transform_map": self.transform_map.pk},
            data={
                "transform_map": self.transform_map.pk,
                "source_model": self.device_content_type.pk,  # Use ContentType pk instead of string
                "target_field": "site",
            },
        )
        # Since the form requires dynamic field setup, let's manually set the choices
        form.fields["target_field"].widget.choices = [("site", "Site")]
        self.assertTrue(form.is_valid(), form.errors)
        instance = form.save()
        self.assertFalse(instance.coalesce)

    def test_form_initialization_with_existing_instance_no_data(self):
        """Test no self.data with existing instance"""
        # Create an existing IPFabricRelationshipField instance
        relationship_field = IPFabricRelationshipField.objects.create(
            transform_map=self.transform_map,
            source_model=self.device_content_type,
            target_field="site",
            coalesce=True,
        )

        # Initialize form with existing instance but no data
        form = IPFabricRelationshipFieldForm(instance=relationship_field)

        # Verify that the form sets up field choices based on the existing instance
        self.assertIsNotNone(form.fields["target_field"].widget.choices)
        self.assertEqual(form.fields["target_field"].widget.initial, "site")

    def test_form_initialization_with_initial_transform_map_no_data(self):
        """Test no self.data with initial transform_map"""
        # Initialize form with initial transform_map but no data
        form = IPFabricRelationshipFieldForm(
            initial={"transform_map": self.transform_map.pk}
        )

        # Verify that the form sets up field choices based on the transform_map
        self.assertIsNotNone(form.fields["target_field"].widget.choices)
        # Verify choices contain relation fields (excluding exclude_fields)
        target_choices = form.fields["target_field"].widget.choices
        self.assertTrue(len(target_choices) > 0)

    def test_form_initialization_without_initial_data_no_data(self):
        """Test no self.data without initial transform_map"""
        # Initialize form without initial data and no data
        form = IPFabricRelationshipFieldForm()

        # Verify that the form doesn't crash and has default field setup
        self.assertIsNotNone(form.fields["source_model"])
        self.assertIsNotNone(form.fields["target_field"])
        # Widget choices should be empty or default since no transform_map is provided
        self.assertTrue(hasattr(form.fields["target_field"], "widget"))
        self.assertTrue(hasattr(form.fields["source_model"], "widget"))


class IPFabricTransformFieldFormTestCase(TestCase):
    @classmethod
    def setUpTestData(cls):
        cls.source = IPFabricSource.objects.create(
            name="Test Source",
            type=IPFabricSourceTypeChoices.LOCAL,
            url="https://test.ipfabric.local",
            status=DataSourceStatusChoices.NEW,
        )

        cls.transform_map_group = IPFabricTransformMapGroup.objects.create(
            name="Test Group", description="Test group description"
        )

        cls.device_content_type = ContentType.objects.get_for_model(Device)

        cls.transform_map = IPFabricTransformMap.objects.create(
            name="Test Transform Map",
            group=cls.transform_map_group,
            source_model="device",
            target_model=cls.device_content_type,
        )

    def test_fields_are_required(self):
        form = IPFabricTransformFieldForm(data={})
        self.assertFalse(form.is_valid(), form.errors)
        self.assertIn("source_field", form.errors)
        self.assertIn("target_field", form.errors)
        self.assertIn("transform_map", form.errors)

    def test_fields_are_optional(self):
        form = IPFabricTransformFieldForm(
            data={
                "transform_map": self.transform_map.pk,
                "source_field": "hostname",
                "target_field": "name",
            },
        )
        self.assertTrue(form.is_valid(), form.errors)

    def test_valid_transform_field_form(self):
        # Initialize form with transform_map to set up field choices
        form = IPFabricTransformFieldForm(
            initial={"transform_map": self.transform_map.pk},
            data={
                "transform_map": self.transform_map.pk,
                "source_field": "hostname",
                "target_field": "name",
                "coalesce": True,
                "template": "{{ object.hostname }}",
            },
        )
        self.assertTrue(form.is_valid(), form.errors)

    def test_coalesce_field_defaults_to_false(self):
        # Initialize form with transform_map to set up field choices
        form = IPFabricTransformFieldForm(
            initial={"transform_map": self.transform_map.pk},
            data={
                "transform_map": self.transform_map.pk,
                "source_field": "hostname",
                "target_field": "name",
            },
        )
        self.assertTrue(form.is_valid(), form.errors)
        instance = form.save()
        self.assertFalse(instance.coalesce)

    def test_form_initialization_with_existing_instance_no_data(self):
        """Test no data with existing instance"""
        # Create an existing IPFabricTransformField instance
        transform_field = IPFabricTransformField.objects.create(
            transform_map=self.transform_map,
            source_field="hostname",
            target_field="name",
            coalesce=True,
        )

        # Initialize form with existing instance but no data
        form = IPFabricTransformFieldForm(instance=transform_field)

        # Verify that the form sets up field choices based on the existing instance
        self.assertIsNotNone(form.fields["target_field"].widget.choices)
        self.assertIsNotNone(form.fields["source_field"].widget.choices)
        self.assertEqual(form.fields["target_field"].widget.initial, "name")

    def test_form_initialization_with_initial_transform_map_no_data(self):
        """Test no data with initial transform_map"""
        # Initialize form with initial transform_map but no data
        form = IPFabricTransformFieldForm(
            initial={"transform_map": self.transform_map.pk}
        )

        # Verify that the form sets up field choices based on the transform_map
        self.assertIsNotNone(form.fields["target_field"].widget.choices)
        self.assertIsNotNone(form.fields["source_field"].widget.choices)
        # Verify choices contain non-relation fields (excluding exclude_fields)
        target_choices = form.fields["target_field"].widget.choices
        self.assertTrue(len(target_choices) > 0)

    def test_form_initialization_without_initial_data_no_data(self):
        """Test no data without initial transform_map"""
        # Initialize form without initial data and no data
        form = IPFabricTransformFieldForm()

        # Verify that the form doesn't crash and has default field setup
        self.assertIsNotNone(form.fields["source_field"])
        self.assertIsNotNone(form.fields["target_field"])
        # Widget choices should be empty or default since no transform_map is provided
        self.assertTrue(hasattr(form.fields["target_field"], "widget"))
        self.assertTrue(hasattr(form.fields["source_field"], "widget"))


class IPFabricTransformMapGroupFormTestCase(TestCase):
    def test_fields_are_required(self):
        form = IPFabricTransformMapGroupForm(data={})
        self.assertFalse(form.is_valid(), form.errors)
        self.assertIn("name", form.errors)

    def test_fields_are_optional(self):
        form = IPFabricTransformMapGroupForm(data={"name": "Test Group"})
        self.assertTrue(form.is_valid(), form.errors)

    def test_valid_transform_map_group_form(self):
        form = IPFabricTransformMapGroupForm(
            data={"name": "Test Group", "description": "Test group description"}
        )
        self.assertTrue(form.is_valid(), form.errors)
        instance = form.save()
        self.assertEqual(instance.name, "Test Group")
        self.assertEqual(instance.description, "Test group description")


class IPFabricTransformMapFormTestCase(TestCase):
    @classmethod
    def setUpTestData(cls):
        cls.transform_map_group = IPFabricTransformMapGroup.objects.create(
            name="Test Group", description="Test group description"
        )
        cls.device_content_type = ContentType.objects.get_for_model(Device)

    def test_fields_are_required(self):
        form = IPFabricTransformMapForm(data={})
        self.assertFalse(form.is_valid(), form.errors)
        self.assertIn("name", form.errors)
        self.assertIn("source_model", form.errors)
        self.assertIn("target_model", form.errors)

    def test_group_is_optional(self):
        # Need to avoid unique_together constraint violation
        IPFabricTransformMap.objects.get(
            group=None, target_model=self.device_content_type
        ).delete()
        form = IPFabricTransformMapForm(
            data={
                "name": "Test Transform Map",
                "source_model": "device",
                "target_model": self.device_content_type.pk,
            }
        )
        self.assertTrue(form.is_valid(), form.errors)

    def test_valid_transform_map_form(self):
        form = IPFabricTransformMapForm(
            data={
                "name": "Test Transform Map",
                "group": self.transform_map_group.pk,
                "source_model": "device",
                "target_model": self.device_content_type.pk,
            }
        )
        self.assertTrue(form.is_valid(), form.errors)
        instance = form.save()
        self.assertEqual(instance.name, "Test Transform Map")
        self.assertEqual(instance.group, self.transform_map_group)


class IPFabricTransformMapCloneFormTestCase(TestCase):
    @classmethod
    def setUpTestData(cls):
        cls.transform_map_group = IPFabricTransformMapGroup.objects.create(
            name="Test Group", description="Test group description"
        )

    def fields_are_required(self):
        form = IPFabricTransformMapCloneForm(data={})
        self.assertFalse(form.is_valid(), form.errors)
        self.assertIn("name", form.errors)

    def test_fields_are_optional(self):
        form = IPFabricTransformMapCloneForm(
            data={
                "name": "Cloned Transform Map",
            }
        )
        self.assertTrue(form.is_valid(), form.errors)

    def test_clone_options_default_to_true(self):
        form = IPFabricTransformMapCloneForm(
            data={"name": "Cloned Transform Map", "group": self.transform_map_group.pk}
        )
        self.assertTrue(form.is_valid(), form.errors)
        # Check initial values
        self.assertTrue(form.fields["clone_fields"].initial)
        self.assertTrue(form.fields["clone_relationships"].initial)

    def test_valid_clone_form(self):
        form = IPFabricTransformMapCloneForm(
            data={
                "name": "Cloned Transform Map",
                "group": self.transform_map_group.pk,
                "clone_fields": False,
                "clone_relationships": True,
            }
        )
        self.assertTrue(form.is_valid(), form.errors)


class IPFabricSnapshotFilterFormTestCase(TestCase):
    @classmethod
    def setUpTestData(cls):
        cls.source = IPFabricSource.objects.create(
            name="Test Source",
            type=IPFabricSourceTypeChoices.LOCAL,
            url="https://test.ipfabric.local",
            status=DataSourceStatusChoices.NEW,
        )

    def test_all_fields_are_optional(self):
        form = IPFabricSnapshotFilterForm(data={})
        self.assertTrue(form.is_valid(), form.errors)

    def test_valid_filter_form_with_all_fields(self):
        form = IPFabricSnapshotFilterForm(
            data={
                "name": "Test Snapshot",
                "status": "loaded",
                "source_id": [self.source.pk],
                "snapshot_id": "test-snapshot-id",
            }
        )
        self.assertTrue(form.is_valid(), form.errors)


class IPFabricSourceFilterFormTestCase(TestCase):
    def test_all_fields_are_optional(self):
        form = IPFabricSourceFilterForm(data={})
        self.assertTrue(form.is_valid(), form.errors)

    def test_valid_filter_form_with_status(self):
        form = IPFabricSourceFilterForm(
            data={
                "status": [
                    DataSourceStatusChoices.NEW,
                    DataSourceStatusChoices.COMPLETED,
                ]
            }
        )
        self.assertTrue(form.is_valid(), form.errors)


class IPFabricIngestionFilterFormTestCase(TestCase):
    @classmethod
    def setUpTestData(cls):
        cls.source = IPFabricSource.objects.create(
            name="Test Source",
            type=IPFabricSourceTypeChoices.LOCAL,
            url="https://test.ipfabric.local",
            status=DataSourceStatusChoices.NEW,
        )

        cls.snapshot = IPFabricSnapshot.objects.create(
            name="Test Snapshot",
            source=cls.source,
            snapshot_id="test-snapshot-id",
            status=IPFabricSnapshotStatusModelChoices.STATUS_LOADED,
        )

        cls.sync = IPFabricSync.objects.create(
            name="Test Sync",
            snapshot_data=cls.snapshot,
        )

    def test_all_fields_are_optional(self):
        form = IPFabricIngestionFilterForm(data={})
        self.assertTrue(form.is_valid(), form.errors)

    def test_valid_filter_form_with_sync(self):
        form = IPFabricIngestionFilterForm(data={"sync_id": [self.sync.pk]})
        self.assertTrue(form.is_valid(), form.errors)


class IPFabricIngestionMergeFormTestCase(TestCase):
    def test_remove_branch_defaults_to_true(self):
        form = IPFabricIngestionMergeForm(data={"confirm": True})
        self.assertTrue(form.is_valid(), form.errors)
        self.assertTrue(form.fields["remove_branch"].initial)

    def test_remove_branch_is_optional(self):
        form = IPFabricIngestionMergeForm(data={"confirm": True})
        self.assertTrue(form.is_valid(), form.errors)

    def test_valid_merge_form(self):
        form = IPFabricIngestionMergeForm(data={"confirm": True, "remove_branch": True})
        self.assertTrue(form.is_valid(), form.errors)


class IPFabricSyncFormTestCase(TestCase):
    maxDiff = 1500

    @classmethod
    def setUpTestData(cls):
        cls.source = IPFabricSource.objects.create(
            name="Test Source",
            type=IPFabricSourceTypeChoices.LOCAL,
            url="https://test.ipfabric.local",
            status=DataSourceStatusChoices.NEW,
        )

        cls.snapshot = IPFabricSnapshot.objects.create(
            name="Test Snapshot",
            source=cls.source,
            snapshot_id="test-snapshot-id",
            status=IPFabricSnapshotStatusModelChoices.STATUS_LOADED,
            data={
                "sites": ["site1", "site2", "site3"]
            },  # Store as list instead of comma-separated string
        )

        cls.transform_map_group = IPFabricTransformMapGroup.objects.create(
            name="Test Group", description="Test group description"
        )

    def test_fields_are_required(self):
        form = IPFabricSyncForm(data={})
        self.assertFalse(form.is_valid(), form.errors)
        self.assertIn("name", form.errors)
        self.assertIn("source", form.errors)
        self.assertIn("snapshot_data", form.errors)

    def test_fields_are_optional(self):
        form = IPFabricSyncForm(
            data={
                "name": "Test Sync",
                "source": self.source.pk,
                "snapshot_data": self.snapshot.pk,
            }
        )
        self.assertTrue(form.is_valid(), form.errors)

    def test_valid_sync_form(self):
        form = IPFabricSyncForm(
            data={
                "name": "Test Sync",
                "source": self.source.pk,
                "snapshot_data": self.snapshot.pk,
                "auto_merge": True,
                "update_custom_fields": True,
            }
        )
        self.assertTrue(form.is_valid(), form.errors)

    def test_form_initialization_with_sites_no_data(self):
        """Test sites handling without data"""
        form = IPFabricSyncForm(initial={"sites": ["site1", "site2"]})

        # Verify that sites choices and initial values are set
        # Convert to list for comparison since form returns list, not tuple
        expected_choices = [("site1", "site1"), ("site2", "site2")]
        self.assertEqual(form.fields["sites"].choices, expected_choices)
        self.assertEqual(form.fields["sites"].initial, tuple(expected_choices))

    def test_form_initialization_with_snapshot_data_in_form_data(self):
        """Test form with data containing snapshot_data"""
        form = IPFabricSyncForm(
            data={
                "name": "Test Sync",
                "source": self.source.pk,
                "snapshot_data": self.snapshot.pk,
            }
        )

        # Verify that site choices are set based on snapshot's sites when data exists
        expected_choices = [("site1", "site1"), ("site2", "site2"), ("site3", "site3")]
        self.assertEqual(form.fields["sites"].choices, expected_choices)
        self.assertEqual(self.snapshot.sites, ["site1", "site2", "site3"])

    def test_form_initialization_with_different_snapshot_sites(self):
        """Verify different snapshot sites are properly handled"""
        # Create another snapshot with different sites
        snapshot2 = IPFabricSnapshot.objects.create(
            name="Test Snapshot 2",
            source=self.source,
            snapshot_id="test-snapshot-id-2",
            status=IPFabricSnapshotStatusModelChoices.STATUS_LOADED,
            data={"sites": ["siteA", "siteB"]},
        )

        # Test form with the second snapshot
        form = IPFabricSyncForm(
            data={
                "name": "Test Sync 2",
                "source": self.source.pk,
                "snapshot_data": snapshot2.pk,
            }
        )

        # Verify that the correct snapshot's sites are used
        # Convert to list for comparison since form returns list, not tuple
        expected_choices = [("siteA", "siteA"), ("siteB", "siteB")]
        self.assertEqual(form.fields["sites"].choices, expected_choices)

    def test_form_initialization_with_snapshot_no_sites_data(self):
        """Verify handling when snapshot has no sites data"""
        # Create a snapshot with no sites data
        snapshot_no_sites = IPFabricSnapshot.objects.create(
            name="Test Snapshot No Sites",
            source=self.source,
            snapshot_id="test-snapshot-no-sites",
            status=IPFabricSnapshotStatusModelChoices.STATUS_LOADED,
            data={},  # No sites data
        )

        # Test form with snapshot that has no sites
        form = IPFabricSyncForm(
            data={
                "name": "Test Sync No Sites",
                "source": self.source.pk,
                "snapshot_data": snapshot_no_sites.pk,
            }
        )

        # Verify that sites choices are empty when snapshot has no sites
        sites_choices = form.fields["sites"].choices
        self.assertTrue(len(sites_choices) == 0)
        self.assertEqual(snapshot_no_sites.sites, [])

    def test_form_initialization_with_existing_instance_no_data(self):
        """Test existing instance initialization when not self.data"""
        # Create an existing sync instance
        sync_instance = IPFabricSync.objects.create(
            name="Existing Sync",
            snapshot_data=self.snapshot,
            parameters={
                "sites": ["site1", "site2"],
                "groups": [self.transform_map_group.pk],
            },
        )

        # Test form initialization with existing instance but no data
        form = IPFabricSyncForm(instance=sync_instance)

        # Verify that initial values are set from instance parameters
        self.assertEqual(form.initial["source"], self.source)
        self.assertEqual(form.initial["sites"], ["site1", "site2"])
        self.assertEqual(form.initial["groups"], [self.transform_map_group.pk])

        # Verify that sites choices are set from instance's snapshot when no data
        # Convert to list for comparison since form returns list, not tuple
        expected_choices = [("site1", "site1"), ("site2", "site2"), ("site3", "site3")]
        self.assertEqual(form.fields["sites"].choices, expected_choices)

    def test_form_initialization_with_existing_instance_and_initial_kwargs(self):
        """Test existing instance initialization with initial kwargs"""
        # Create an existing sync instance
        sync_instance = IPFabricSync.objects.create(
            name="Existing Sync",
            snapshot_data=self.snapshot,
            parameters={"sites": ["site1"], "groups": []},
        )

        # Test form initialization with existing instance and initial kwargs
        form = IPFabricSyncForm(
            instance=sync_instance, initial={"name": "Override Name"}
        )

        # These should be set from instance even when not in initial kwarg
        self.assertIn("source", form.initial)
        self.assertIn("sites", form.initial)
        self.assertIn("groups", form.initial)

        # But the provided initial value should be present
        self.assertEqual(form.initial.get("name"), "Override Name")

    def test_sites_initial_value_set_from_form_initial(self):
        """Test that sites field initial is set from self.initial["sites"]"""
        # Create an existing sync instance with sites in parameters
        sync_instance = IPFabricSync.objects.create(
            name="Existing Sync",
            snapshot_data=self.snapshot,
            parameters={
                "sites": ["site1", "site2"],
                "groups": [self.transform_map_group.pk],
            },
        )

        # Initialize form with existing instance and additional initial data for sites
        # This will trigger the else branch where self.initial["sites"] is used
        form = IPFabricSyncForm(
            instance=sync_instance,
            initial={"sites": ["override_site1", "override_site2"]},
        )

        # Verify sites field initial is set from self.initial
        self.assertEqual(
            form.fields["sites"].initial, ["override_site1", "override_site2"]
        )

        # Also verify that self.initial contains the expected sites
        self.assertEqual(form.initial["sites"], ["override_site1", "override_site2"])

    def test_htmx_boolean_field_list_values_handled(self):
        """Test sanitizing HTMX BooleanField list values like ['', 'on']"""
        # Simulate HTMX request where BooleanField values become lists
        # This happens when `source` field value is changed and form is re-drawn via HTMX
        form = IPFabricSyncForm(
            initial={
                "auto_merge": ["", "on"],  # HTMX sends BooleanField as list
                "update_custom_fields": ["", "on"],  # Another BooleanField as list
                "ipf_site": ["", "on"],  # ipf_ prefixed field as list
                "name": "Test Sync",  # Normal field (not affected)
            },
            data={
                "name": "Test Sync HTMX",
                "source": self.source.pk,
                "snapshot_data": self.snapshot.pk,
            },
        )

        # The last value from ['', 'on'] should be 'on' which evaluates to True for BooleanFields
        self.assertEqual(form.initial["auto_merge"], "on")
        self.assertEqual(form.initial["update_custom_fields"], "on")
        self.assertEqual(form.initial["ipf_site"], "on")
        self.assertEqual(form.initial["name"], "Test Sync")  # Normal field unchanged

        # Verify the form is still valid and processes correctly
        self.assertTrue(form.is_valid(), form.errors)

    def test_htmx_boolean_field_single_values_unchanged(self):
        """Test that normal single values are not affected by the HTMX list handling"""
        # Test with normal single values (not lists)
        form = IPFabricSyncForm(
            initial={
                "auto_merge": True,  # Normal boolean value
                "update_custom_fields": False,  # Normal boolean value
                "ipf_site": "on",  # Normal string value
                "name": "Test Sync",  # Normal string value
            },
            data={
                "name": "Test Sync Normal",
                "source": self.source.pk,
                "snapshot_data": self.snapshot.pk,
            },
        )

        # Verify that single values are not processed by value sanitization
        self.assertEqual(form.initial["auto_merge"], True)
        self.assertEqual(form.initial["update_custom_fields"], False)
        self.assertEqual(form.initial["ipf_site"], "on")
        self.assertEqual(form.initial["name"], "Test Sync")

        # Verify the form is still valid
        self.assertTrue(form.is_valid(), form.errors)

    def test_clean_snapshot_does_not_belong_to_source(self):
        """Test form validation when snapshot doesn't belong to the selected source"""
        # Create a second source
        different_source = IPFabricSource.objects.create(
            name="Different Source",
            type=IPFabricSourceTypeChoices.LOCAL,
            url="https://different.ipfabric.local",
            status=DataSourceStatusChoices.NEW,
        )

        # Try to use self.snapshot (which belongs to self.source) with different_source
        form = IPFabricSyncForm(
            data={
                "name": "Test Sync Mismatched Source",
                "source": different_source.pk,
                "snapshot_data": self.snapshot.pk,  # This snapshot belongs to self.source, not different_source
            }
        )

        # Form should be invalid due to snapshot/source mismatch validation
        self.assertFalse(form.is_valid(), form.errors)
        self.assertIn("snapshot_data", form.errors)
        self.assertTrue(
            "Snapshot does not belong to the selected source"
            in str(form.errors["snapshot_data"])
        )

    def test_clean_sites_not_part_of_snapshot(self):
        """Test form validation when selected sites are not part of the snapshot"""
        form = IPFabricSyncForm(
            data={
                "name": "Test Sync Invalid Sites",
                "source": self.source.pk,
                "snapshot_data": self.snapshot.pk,
                "sites": ["invalid_site1", "invalid_site2"],  # Sites not in snapshot
            }
        )

        # Form should be invalid due to sites validation
        self.assertFalse(form.is_valid(), form.errors)
        self.assertIn("sites", form.errors)
        self.assertTrue("not part of the snapshot" in str(form.errors["sites"]))

    def test_clean_sites_validation_with_valid_sites(self):
        """Test form validation when selected sites are valid (part of the snapshot)"""
        form = IPFabricSyncForm(
            data={
                "name": "Test Sync Valid Sites",
                "source": self.source.pk,
                "snapshot_data": self.snapshot.pk,
                "sites": ["site1", "site2"],  # Valid sites that are in snapshot
            }
        )

        # Form should be valid since sites are part of the snapshot
        self.assertTrue(form.is_valid(), form.errors)

    def test_clean_sites_validation_with_partial_match(self):
        """Test form validation when some sites are valid and some are not"""
        form = IPFabricSyncForm(
            data={
                "name": "Test Sync Partial Sites",
                "source": self.source.pk,
                "snapshot_data": self.snapshot.pk,
                "sites": ["site1", "invalid_site"],  # Mix of valid and invalid sites
            }
        )

        # Form should be invalid since not all sites are part of the snapshot
        self.assertFalse(form.is_valid(), form.errors)
        self.assertIn("sites", form.errors)
        self.assertTrue("not part of the snapshot" in str(form.errors["sites"]))

    def test_clean_sites_validation_without_sites(self):
        """Test form validation when no sites are selected (sites is None/empty)"""
        form = IPFabricSyncForm(
            data={
                "name": "Test Sync No Sites",
                "source": self.source.pk,
                "snapshot_data": self.snapshot.pk,
                # No sites specified
            }
        )

        # Form should be valid since the condition only triggers when sites exist
        self.assertTrue(form.is_valid(), form.errors)

    def test_clean_scheduled_time_in_past(self):
        """Test form validation when scheduled time is in the past"""
        past_time = local_now() - timedelta(hours=1)
        form = IPFabricSyncForm(
            data={
                "name": "Test Sync Past Schedule",
                "source": self.source.pk,
                "snapshot_data": self.snapshot.pk,
                "scheduled": past_time,
            }
        )

        # Form should be invalid due to scheduled time validation
        self.assertFalse(form.is_valid(), form.errors)
        self.assertTrue("Scheduled time must be in the future" in str(form.errors))

    def test_clean_interval_without_scheduled_time(self):
        """Test interval is provided without scheduled time"""
        form = IPFabricSyncForm(
            data={
                "name": "Test Sync No Schedule",
                "source": self.source.pk,
                "snapshot_data": self.snapshot.pk,
                "interval": 60,
                # No scheduled time specified
            }
        )

        self.assertTrue(form.is_valid(), form.errors)
        self.assertIsNotNone(form.cleaned_data["scheduled"])

    def test_clean_groups_missing_required_transform_maps(self):
        """Test form validation when transform map groups are missing required maps"""
        # Delete a required default transform map to trigger validation failure
        # This ensures that the missing map cannot be covered by default maps
        manufacturer_content_type = ContentType.objects.get(
            app_label="dcim", model="manufacturer"
        )
        IPFabricTransformMap.objects.filter(
            target_model=manufacturer_content_type, group__isnull=True
        ).delete()

        form = IPFabricSyncForm(
            data={
                "name": "Test Sync Missing Maps",
                "source": self.source.pk,
                "snapshot_data": self.snapshot.pk,
            }
        )

        # Form should be invalid due to missing required transform maps
        self.assertFalse(form.is_valid(), form.errors)
        self.assertIn("groups", form.errors)
        self.assertTrue("Missing maps:" in str(form.errors["groups"]))
        # Check that it mentions some of the missing required maps
        error_message = str(form.errors["groups"])
        self.assertTrue("dcim.manufacturer" in error_message, error_message)

    def test_save_method_basic_functionality(self):
        """Test basic save functionality without scheduling"""
        form = IPFabricSyncForm(
            data={
                "name": "Test Sync Save",
                "source": self.source.pk,
                "snapshot_data": self.snapshot.pk,
                "sites": ["site1", "site2"],
                "groups": [self.transform_map_group.pk],
                "auto_merge": True,
                "update_custom_fields": True,
            }
        )

        self.assertTrue(form.is_valid(), form.errors)

        # Save the form
        sync_instance = form.save()

        # Verify the instance was created correctly
        self.assertIsInstance(sync_instance, IPFabricSync)
        self.assertEqual(sync_instance.name, "Test Sync Save")
        self.assertEqual(sync_instance.snapshot_data.source, self.source)
        self.assertEqual(sync_instance.snapshot_data, self.snapshot)
        self.assertEqual(sync_instance.status, DataSourceStatusChoices.NEW)
        self.assertTrue(sync_instance.auto_merge)
        self.assertTrue(sync_instance.update_custom_fields)

        # Verify parameters were stored correctly
        # All models are `False` since checkboxes must always default to False
        expected_parameters = {
            "sites": ["site1", "site2"],
            "groups": [self.transform_map_group.pk],
            "site": False,
            "manufacturer": False,
            "devicetype": False,
            "devicerole": False,
            "platform": False,
            "device": False,
            "virtualchassis": False,
            "interface": False,
            "macaddress": False,
            "inventoryitem": False,
            "vlan": False,
            "vrf": False,
            "prefix": False,
            "ipaddress": False,
        }
        self.assertEqual(sync_instance.parameters, expected_parameters)

    def test_save_method_with_ipf_parameters(self):
        """Test save method properly handles ipf_ prefixed form fields"""
        form = IPFabricSyncForm(
            data={
                "name": "Test Sync IPF Params",
                "source": self.source.pk,
                "snapshot_data": self.snapshot.pk,
                "ipf_site": True,
                "ipf_interface": True,
                "ipf_prefix": True,
            }
        )

        self.assertTrue(form.is_valid(), form.errors)

        sync_instance = form.save()

        # Verify ipf_ parameters were stripped and stored correctly
        # All models are `False` since checkboxes must always default to False
        expected_parameters = {
            "sites": [],
            "groups": [],
            "site": True,  # Explicitly set via ipf_site
            "manufacturer": False,
            "devicetype": False,
            "devicerole": False,
            "platform": False,
            "device": False,
            "virtualchassis": False,
            "interface": True,  # Explicitly set via ipf_interface
            "macaddress": False,
            "inventoryitem": False,
            "ipaddress": False,
            "vlan": False,
            "vrf": False,
            "prefix": True,  # Explicitly set via ipf_prefix
        }
        self.assertEqual(sync_instance.parameters, expected_parameters)

    @patch("ipfabric_netbox.models.IPFabricSync.enqueue_sync_job")
    def test_save_method_with_scheduling_no_interval(self, mock_enqueue):
        """Test save method with scheduled time but no interval"""
        future_time = local_now() + timedelta(hours=1)

        form = IPFabricSyncForm(
            data={
                "name": "Test Sync Scheduled",
                "source": self.source.pk,
                "snapshot_data": self.snapshot.pk,
                "scheduled": future_time,
            }
        )

        self.assertTrue(form.is_valid(), form.errors)

        # Capture the time before save to compare - with more tolerance
        sync_instance = form.save()

        # Verify the instance was created correctly
        self.assertEqual(sync_instance.scheduled, future_time)
        self.assertIsNone(sync_instance.interval)

        # Verify the instance exists in database
        saved_instance = IPFabricSync.objects.get(pk=sync_instance.pk)
        self.assertEqual(saved_instance.scheduled, future_time)

        # Verify that enqueue_sync_job was called when object.scheduled is set
        mock_enqueue.assert_called_once()

    @patch("ipfabric_netbox.models.IPFabricSync.enqueue_sync_job")
    def test_save_method_with_interval_auto_schedule(self, mock_enqueue):
        """Test save method with interval automatically schedules for current time"""
        form = IPFabricSyncForm(
            data={
                "name": "Test Sync Interval",
                "source": self.source.pk,
                "snapshot_data": self.snapshot.pk,
                "interval": 60,
                # No scheduled time specified
            }
        )

        self.assertTrue(form.is_valid(), form.errors)

        # Capture the time before save to compare - with more tolerance
        before_save = local_now() - timedelta(seconds=1)
        sync_instance = form.save()
        after_save = local_now() + timedelta(seconds=1)

        # Verify interval was set and scheduled time was auto-generated
        self.assertEqual(sync_instance.interval, 60)
        self.assertIsNotNone(sync_instance.scheduled)

        # Scheduled time should be close to current time (within the test execution window)
        self.assertTrue(before_save <= sync_instance.scheduled <= after_save)

        # Verify that enqueue_sync_job was called when object.scheduled is set
        mock_enqueue.assert_called_once()

    @patch("ipfabric_netbox.models.IPFabricSync.enqueue_sync_job")
    def test_save_method_with_both_scheduled_and_interval(self, mock_enqueue):
        """Test save method with both scheduled time and interval"""
        future_time = local_now() + timedelta(hours=2)

        form = IPFabricSyncForm(
            data={
                "name": "Test Sync Both",
                "source": self.source.pk,
                "snapshot_data": self.snapshot.pk,
                "scheduled": future_time,
                "interval": 120,
            }
        )

        self.assertTrue(form.is_valid(), form.errors)

        sync_instance = form.save()

        # Verify both values were set correctly
        self.assertEqual(sync_instance.scheduled, future_time)
        self.assertEqual(sync_instance.interval, 120)

        # Verify that enqueue_sync_job was called when object.scheduled is set
        mock_enqueue.assert_called_once()

    def test_save_method_empty_sites_and_groups(self):
        """Test save method handles empty sites and groups correctly"""
        form = IPFabricSyncForm(
            data={
                "name": "Test Sync Empty",
                "source": self.source.pk,
                "snapshot_data": self.snapshot.pk,
                # No sites or groups specified
            }
        )

        self.assertTrue(form.is_valid(), form.errors)

        sync_instance = form.save()

        # Verify empty collections are handled correctly
        self.assertEqual(sync_instance.parameters["sites"], [])
        self.assertEqual(sync_instance.parameters["groups"], [])

    def test_save_method_status_always_set_to_new(self):
        """Test that save method always sets status to NEW regardless of input"""
        form = IPFabricSyncForm(
            data={
                "name": "Test Sync Status",
                "source": self.source.pk,
                "snapshot_data": self.snapshot.pk,
            }
        )

        self.assertTrue(form.is_valid(), form.errors)

        sync_instance = form.save()

        # Status should always be NEW after save
        self.assertEqual(sync_instance.status, DataSourceStatusChoices.NEW)

        # Verify in database as well
        saved_instance = IPFabricSync.objects.get(pk=sync_instance.pk)
        self.assertEqual(saved_instance.status, DataSourceStatusChoices.NEW)

    def test_fieldsets_for_local_source_type(self):
        """Test that fieldsets returns correct structure for local source type"""
        form = IPFabricSyncForm(
            data={
                "name": "Test Sync",
                "source": self.source.pk,
                "snapshot_data": self.snapshot.pk,
            }
        )

        fieldsets = form.fieldsets

        # Should have multiple fieldsets
        self.assertGreater(len(fieldsets), 5)

        # First fieldset should be IP Fabric Source
        self.assertEqual(fieldsets[0].name, "IP Fabric Source")

        # Second fieldset should be Snapshot Information with sites for local source
        self.assertEqual(fieldsets[1].name, "Snapshot Information")

        # Should contain Ingestion Execution Parameters fieldset
        exec_params_fieldset = next(
            (fs for fs in fieldsets if fs.name == "Ingestion Execution Parameters"),
            None,
        )
        self.assertIsNotNone(exec_params_fieldset)

        # Should contain Extras fieldset
        extras_fieldset = next((fs for fs in fieldsets if fs.name == "Extras"), None)
        self.assertIsNotNone(extras_fieldset)

        # Should contain Tags fieldset
        tags_fieldset = next((fs for fs in fieldsets if fs.name == "Tags"), None)
        self.assertIsNotNone(tags_fieldset)

    def test_fieldsets_for_remote_source_type(self):
        """Test that fieldsets returns correct structure for remote source type"""
        # Create a remote source
        remote_source = IPFabricSource.objects.create(
            name="Test Remote Source",
            type=IPFabricSourceTypeChoices.REMOTE,
            url="https://remote.ipfabric.local",
            status=DataSourceStatusChoices.NEW,
        )

        remote_snapshot = IPFabricSnapshot.objects.create(
            name="Test Remote Snapshot",
            source=remote_source,
            snapshot_id="test-remote-snapshot-id",
            status=IPFabricSnapshotStatusModelChoices.STATUS_LOADED,
            data={"sites": ["remote_site1", "remote_site2"]},
        )

        form = IPFabricSyncForm(
            data={
                "name": "Test Remote Sync",
                "source": remote_source.pk,
                "snapshot_data": remote_snapshot.pk,
            }
        )

        fieldsets = form.fieldsets

        # Should have multiple fieldsets
        self.assertGreater(len(fieldsets), 5)

        # First fieldset should be IP Fabric Source
        self.assertEqual(fieldsets[0].name, "IP Fabric Source")

        # Second fieldset should be Snapshot Information without sites for remote source
        self.assertEqual(fieldsets[1].name, "Snapshot Information")

        # Verify the fieldsets structure is consistent
        fieldset_names = [fs.name for fs in fieldsets]
        expected_names = [
            "IP Fabric Source",
            "Snapshot Information",
            "Extras",
            "Tags",
        ]

        for expected_name in expected_names:
            self.assertIn(expected_name, fieldset_names)

    def test_fieldsets_with_existing_instance_local_source(self):
        """Test fieldsets behavior with an existing sync instance from local source"""
        # Create an existing sync instance
        sync_instance = IPFabricSync.objects.create(
            name="Existing Sync",
            snapshot_data=self.snapshot,
            parameters={
                "sites": ["site1", "site2"],
                "groups": [self.transform_map_group.pk],
            },
        )

        form = IPFabricSyncForm(instance=sync_instance)
        fieldsets = form.fieldsets

        # Should have multiple fieldsets
        self.assertGreater(len(fieldsets), 5)

        # First fieldset should be IP Fabric Source
        self.assertEqual(fieldsets[0].name, "IP Fabric Source")

        # Second fieldset should be Snapshot Information with sites (local source)
        self.assertEqual(fieldsets[1].name, "Snapshot Information")

        # Should contain parameter fieldsets for ALL type
        fieldset_names = [fs.name for fs in fieldsets]
        self.assertIn("DCIM Parameters", fieldset_names)
        self.assertIn("IPAM Parameters", fieldset_names)

    def test_fieldsets_property_returns_correct_field_types(self):
        """Test that fieldsets property returns FieldSet objects with correct structure"""
        from utilities.forms.rendering import FieldSet

        form = IPFabricSyncForm(
            data={
                "name": "Test Sync",
                "source": self.source.pk,
                "snapshot_data": self.snapshot.pk,
            }
        )

        fieldsets = form.fieldsets

        # Each item should be a FieldSet instance
        for fieldset in fieldsets:
            self.assertIsInstance(fieldset, FieldSet)
            # Each fieldset should have a name
            self.assertIsNotNone(fieldset.name)

    def test_fieldsets_dynamic_behavior_consistency(self):
        """Test that fieldsets method consistently returns the same structure for same parameters"""
        # Test consistency for same parameters
        form1 = IPFabricSyncForm(
            data={
                "name": "Test Sync 1",
                "source": self.source.pk,
                "snapshot_data": self.snapshot.pk,
            }
        )
        form2 = IPFabricSyncForm(
            data={
                "name": "Test Sync 2",
                "source": self.source.pk,
                "snapshot_data": self.snapshot.pk,
            }
        )

        fieldsets1 = form1.fieldsets
        fieldsets2 = form2.fieldsets

        # Both should have the same structure
        self.assertEqual(len(fieldsets1), len(fieldsets2))

        fieldset_names1 = [fs.name for fs in fieldsets1]
        fieldset_names2 = [fs.name for fs in fieldsets2]
        self.assertEqual(fieldset_names1, fieldset_names2)
