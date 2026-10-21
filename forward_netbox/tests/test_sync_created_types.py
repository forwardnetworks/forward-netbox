from dcim.models import Device
from dcim.models import DeviceBayTemplate
from dcim.models import DeviceType
from dcim.models import InterfaceTemplate
from dcim.models import Manufacturer
from dcim.models import Module
from dcim.models import ModuleBay
from dcim.models import ModuleBayTemplate
from dcim.models import PowerPortTemplate
from dcim.models.modules import ModuleType

from forward_netbox.models import ForwardDeviceIdentity
from forward_netbox.tests.test_subinterface_apply import SubinterfaceFixture
from forward_netbox.utilities.sync_created_types import sync_created_types
from forward_netbox.utilities.sync_created_types import templated_type_ids


class TypesFixture(SubinterfaceFixture):
    def setUp(self):
        super().setUp()
        self.mfr = Manufacturer.objects.create(name="Opengear", slug="opengear")
        # The fixture's own device type ("dt-s") has no templates and the device is
        # not bound to the sync; start from a clean slate for these tests.
        ForwardDeviceIdentity.objects.filter(sync=self.sync).delete()

    def library_type(self, model="CM7148-2-DAC", slug="og-cm7148"):
        library = DeviceType.objects.create(
            manufacturer=self.mfr, model=model, slug=slug
        )
        InterfaceTemplate.objects.create(
            device_type=library, name="eth0", type="1000base-t"
        )
        return library

    def bare_type(self, model="Opengear CM7148-2-DAC", slug="og-og-cm7148"):
        return DeviceType.objects.create(manufacturer=self.mfr, model=model, slug=slug)

    def device_of(self, device_type, name, in_sync=True):
        device = Device.objects.create(
            name=name,
            device_type=device_type,
            role=self.device.role,
            site=self.device.site,
        )
        if in_sync:
            ForwardDeviceIdentity.objects.create(
                sync=self.sync, source_device_key=name, device=device
            )
        return device


class DeviceTypesTest(TypesFixture):
    def test_a_type_with_templates_is_never_listed(self):
        library = self.library_type()
        self.device_of(library, "d1")

        result = sync_created_types("device", self.sync)

        self.assertIn(library.pk, templated_type_ids("device"))
        self.assertEqual(result["equivalent"], [])
        self.assertEqual(result["unmatched"], [])
        self.assertNotIn(library.pk, result["unused"])

    def test_a_bare_type_in_use_with_a_library_equivalent_is_paired_with_it(self):
        library = self.library_type()
        bare = self.bare_type()
        self.device_of(bare, "d1")
        self.device_of(bare, "d2")

        result = sync_created_types("device", self.sync)

        self.assertEqual(result["equivalent"], [(bare.pk, library.pk, 2)])
        self.assertEqual(result["unmatched"], [])

    def test_a_bare_type_with_no_equivalent_is_unmatched(self):
        bare = self.bare_type(model="Opengear Brand New 9000", slug="og-new")
        self.device_of(bare, "d1")

        result = sync_created_types("device", self.sync)

        self.assertEqual(result["unmatched"], [(bare.pk, 1)])
        self.assertEqual(result["equivalent"], [])

    def test_an_equivalent_must_itself_have_templates(self):
        # A look-alike with no templates is another sync-made type, not a library one.
        self.bare_type(model="CM7148-2-DAC", slug="og-cm7148")
        bare = self.bare_type()
        self.device_of(bare, "d1")

        result = sync_created_types("device", self.sync)

        self.assertEqual(result["equivalent"], [])
        self.assertEqual(result["unmatched"], [(bare.pk, 1)])

    def test_two_possible_equivalents_are_ambiguous_so_none_is_suggested(self):
        self.library_type()
        self.library_type(model="CM 7148-2-DAC", slug="og-cm-7148")
        bare = self.bare_type()
        self.device_of(bare, "d1")

        result = sync_created_types("device", self.sync)

        self.assertEqual(result["equivalent"], [])
        self.assertEqual(result["unmatched"], [(bare.pk, 1)])

    def test_a_type_only_another_sources_devices_use_is_not_this_syncs(self):
        bare = self.bare_type()
        self.device_of(bare, "elsewhere", in_sync=False)

        result = sync_created_types("device", self.sync)

        self.assertEqual(result["equivalent"] + result["unmatched"], [])
        # Used by a device, so it is not unused either: it is in no group.
        self.assertNotIn(bare.pk, result["unused"])

    def test_a_bare_type_nothing_uses_is_unused_across_all_of_netbox(self):
        orphan = self.bare_type(model="Left Behind", slug="left-behind")
        used = self.bare_type()
        self.device_of(used, "d1", in_sync=False)

        result = sync_created_types("device", self.sync)

        self.assertIn(orphan.pk, result["unused"])
        self.assertNotIn(used.pk, result["unused"])

    def test_every_template_less_type_lands_in_exactly_one_group(self):
        self.library_type()
        a = self.bare_type()
        b = self.bare_type(model="Opengear New", slug="og-new")
        c = self.bare_type(model="Unused", slug="og-unused")
        self.device_of(a, "d1")
        self.device_of(b, "d2")

        result = sync_created_types("device", self.sync)

        grouped = (
            [pk for pk, _e, _n in result["equivalent"]]
            + [pk for pk, _n in result["unmatched"]]
            + result["unused"]
        )
        self.assertEqual(len(grouped), len(set(grouped)))
        for pk in (a.pk, b.pk, c.pk):
            self.assertIn(pk, grouped)


class EveryKindOfTemplateCountTest(TypesFixture):
    def test_any_kind_of_component_template_marks_a_device_type_as_having_templates(
        self,
    ):
        for index, make in enumerate(
            (
                lambda t: PowerPortTemplate.objects.create(device_type=t, name="psu"),
                lambda t: DeviceBayTemplate.objects.create(device_type=t, name="bay"),
                lambda t: ModuleBayTemplate.objects.create(device_type=t, name="mb"),
            )
        ):
            device_type = DeviceType.objects.create(
                manufacturer=self.mfr, model=f"Kind {index}", slug=f"kind-{index}"
            )
            make(device_type)

            self.assertIn(device_type.pk, templated_type_ids("device"), index)

    def test_a_module_types_templates_never_mark_a_device_type(self):
        module_type = ModuleType.objects.create(manufacturer=self.mfr, model="Card")
        InterfaceTemplate.objects.create(
            module_type=module_type, name="e1", type="1000base-t"
        )
        bare_device_type = self.bare_type()

        self.assertIn(module_type.pk, templated_type_ids("module"))
        self.assertNotIn(bare_device_type.pk, templated_type_ids("device"))


class SuggestionHonestyTest(TypesFixture):
    def test_a_template_less_lookalike_makes_the_suggestion_ambiguous(self):
        # After the old type is deleted the sync would see two candidates, so it
        # would create a duplicate again; no equivalent is offered.
        self.library_type()
        self.bare_type(model="CM7148 2 DAC", slug="og-lookalike")
        used = self.bare_type()
        self.device_of(used, "d1")

        result = sync_created_types("device", self.sync)

        self.assertEqual(result["equivalent"], [])
        self.assertIn((used.pk, 1), result["unmatched"])


class ModuleTypesTest(TypesFixture):
    def module_type(self, model, *, templated=False):
        module_type = ModuleType.objects.create(manufacturer=self.mfr, model=model)
        if templated:
            InterfaceTemplate.objects.create(
                module_type=module_type, name="e1", type="1000base-t"
            )
        return module_type

    def module_of(self, module_type, device):
        bay = ModuleBay.objects.create(
            device=device, name=f"bay-{module_type.pk}-{Module.objects.count()}"
        )
        return Module.objects.create(
            device=device, module_bay=bay, module_type=module_type
        )

    def test_a_bare_module_type_in_use_is_paired_with_its_library_equivalent(self):
        library = self.module_type("N9K-C93180YC-FX3", templated=True)
        bare = self.module_type("N9K C93180YC FX3")
        device = self.device_of(self.bare_type(), "d1")
        self.module_of(bare, device)

        result = sync_created_types("module", self.sync)

        self.assertEqual(result["equivalent"], [(bare.pk, library.pk, 1)])

    def test_a_module_type_with_templates_or_no_use_is_classified_correctly(self):
        templated = self.module_type("Has templates", templated=True)
        unused = self.module_type("Nothing uses me")

        result = sync_created_types("module", self.sync)

        self.assertNotIn(templated.pk, result["unused"])
        self.assertIn(unused.pk, result["unused"])


class ExplainedRowsTest(TypesFixture):
    def rows(self):
        from forward_netbox.utilities.explained_findings import explained_findings

        return [
            r
            for r in explained_findings(self.sync)
            if "sync-created" in r["finding"] or "nothing uses" in r["finding"]
        ]

    def test_the_groups_become_rows_with_a_reason_a_label_and_a_link(self):
        library = self.library_type()
        paired = self.bare_type()
        alone = self.bare_type(model="Opengear New", slug="og-new")
        self.bare_type(model="Left Behind", slug="og-left")
        self.device_of(paired, "d1")
        self.device_of(paired, "d2")
        self.device_of(alone, "d3")

        rows = {r["finding"]: r for r in self.rows()}

        equivalent = rows[
            "Device types that look sync-created, with an equivalent in NetBox (this sync)"
        ]
        self.assertEqual(equivalent["count"], 1)
        self.assertIn("2 devices use them", equivalent["reason"])
        self.assertEqual(equivalent["fixable"], "needs a person")
        self.assertIn("bulk edit", equivalent["action"])
        # Deleting the old type is part of the move, or the next sync undoes it.
        self.assertIn("delete the old type straight away", equivalent["action"])
        self.assertIn("moves the devices back", equivalent["action"])
        self.assertEqual(
            rows["Device types that look sync-created, with no equivalent (this sync)"][
                "fixable"
            ],
            "no, by design",
        )
        self.assertEqual(rows["Device types nothing uses (all of NetBox)"]["count"], 1)
        for row in rows.values():
            self.assertIn("kind=device_", row["url"])
            self.assertTrue(row["action"].strip() and row["reason"].strip())
        del library

    def test_one_device_is_not_called_devices(self):
        self.library_type()
        self.device_of(self.bare_type(), "only-one")

        (row,) = [r for r in self.rows() if "equivalent in NetBox" in r["finding"]]

        self.assertIn("1 device uses them", row["reason"])
        self.assertNotIn("1 devices", row["reason"])

    def test_the_unused_wording_names_the_right_kind_of_user(self):
        from forward_netbox.utilities.explained_findings import type_group_text

        self.assertIn("No device uses", type_group_text("device", "unused")[1])
        self.assertIn("No module uses", type_group_text("module", "unused")[1])

    def test_no_type_name_appears_in_the_rows(self):
        self.library_type()
        self.device_of(self.bare_type(model="zq-secret-model", slug="zq"), "d1")

        self.assertNotIn("zq-secret-model", str(self.rows()))

    def test_a_group_with_no_types_has_no_row(self):
        self.assertEqual(self.rows(), [])


class TypesPageTest(TypesFixture):
    def setUp(self):
        super().setUp()
        from django.contrib.auth import get_user_model
        from django.urls import reverse

        user = get_user_model().objects.create_superuser(
            "root-t", "t@example.com", "pw"
        )
        self.client.force_login(user)
        self.url = reverse(
            "plugins:forward_netbox:forwardsync_types", args=[self.sync.pk]
        )

    def test_it_lists_the_types_with_their_devices_and_the_equivalent(self):
        library = self.library_type()
        bare = self.bare_type()
        self.device_of(bare, "d1")

        response = self.client.get(self.url, {"kind": "device_equivalent"})

        self.assertContains(response, "Opengear CM7148-2-DAC")
        self.assertContains(response, "CM7148-2-DAC")
        self.assertContains(response, f"device_type_id={bare.pk}")
        self.assertContains(response, library.get_absolute_url())
        self.assertContains(response, "How a type is recognised")

    def test_only_the_chosen_group_is_listed(self):
        self.library_type()
        paired = self.bare_type()
        alone = self.bare_type(model="Opengear New", slug="og-new")
        self.device_of(paired, "d1")
        self.device_of(alone, "d2")

        response = self.client.get(self.url, {"kind": "device_unmatched"})

        self.assertContains(response, "Opengear New")
        self.assertNotContains(response, "Opengear CM7148-2-DAC")

    def test_an_unknown_kind_lists_nothing(self):
        response = self.client.get(self.url, {"kind": "nonsense_x"})

        self.assertEqual(response.status_code, 200)
        self.assertContains(response, "not one this page shows")

    def test_unused_types_have_no_devices_link(self):
        self.bare_type(model="Left Behind", slug="og-left")

        response = self.client.get(self.url, {"kind": "device_unused"})

        self.assertContains(response, "Left Behind")
        self.assertNotContains(response, "device_type_id=")

    def test_someone_who_may_open_the_sync_but_not_the_types_sees_none(self):
        from django.contrib.auth import get_user_model

        from forward_netbox.models import ForwardSync
        from forward_netbox.tests.test_change_control_comments import grant

        self.bare_type(model="Left Behind", slug="og-left")
        user = get_user_model().objects.create(username="sync-viewer-types")
        grant(user, ForwardSync, "view")
        self.client.force_login(user)

        response = self.client.get(self.url, {"kind": "device_unused"})

        self.assertEqual(response.status_code, 200)
        self.assertNotContains(response, "Left Behind")
        self.assertEqual(response.context["total"], 0)

    def test_a_module_kind_links_to_the_module_list(self):
        module_type = ModuleType.objects.create(
            manufacturer=self.mfr, model="Bare card"
        )
        device = self.device_of(self.bare_type(model="Host", slug="host"), "h1")
        bay = ModuleBay.objects.create(device=device, name="bay1")
        Module.objects.create(device=device, module_bay=bay, module_type=module_type)

        response = self.client.get(self.url, {"kind": "module_unmatched"})

        self.assertContains(response, "Bare card")
        self.assertContains(response, f"module_type_id={module_type.pk}")

    def test_every_explained_link_opens(self):
        from forward_netbox.utilities.explained_findings import explained_findings

        self.library_type()
        paired = self.bare_type()
        self.device_of(paired, "d1")
        self.bare_type(model="Left Behind", slug="og-left")

        links = [r["url"] for r in explained_findings(self.sync) if "kind=" in r["url"]]

        self.assertTrue(links)
        for link in links:
            self.assertEqual(self.client.get(link).status_code, 200, link)


class HealthReRaisesATimeoutTest(TypesFixture):
    def test_a_job_timeout_is_not_swallowed_building_the_rows(self):
        from unittest import mock

        from rq.timeouts import JobTimeoutException

        from forward_netbox.utilities.health import _explained_findings

        with mock.patch(
            "forward_netbox.utilities.explained_findings.explained_findings",
            side_effect=JobTimeoutException("slow"),
        ):
            with self.assertRaises(JobTimeoutException):
                _explained_findings(self.sync)
