from unittest import mock

from dcim.models import DeviceType
from dcim.models import Manufacturer
from dcim.models.modules import ModuleType
from django.test import TestCase

from forward_netbox.utilities.netbox_type_matching import match_existing_types
from forward_netbox.utilities.netbox_type_matching import normalize
from forward_netbox.utilities.netbox_type_matching import TypeIndex


def devicetype_row(model, slug, manufacturer_slug="opengear", **extra):
    return {
        "manufacturer": manufacturer_slug.title(),
        "manufacturer_slug": manufacturer_slug,
        "model": model,
        "slug": slug,
        **extra,
    }


class TypeMatchingFixture(TestCase):
    def setUp(self):
        self.opengear = Manufacturer.objects.create(name="Opengear", slug="opengear")
        self.cisco = Manufacturer.objects.create(name="Cisco", slug="cisco")
        self.library = DeviceType.objects.create(
            manufacturer=self.opengear,
            model="CM7148-2-DAC",
            slug="opengear-cm7148-2-dac",
            part_number="CM7148-2-DAC",
        )

    def match(self, model_string, rows):
        return match_existing_types(model_string, rows)


class NormalizeTest(TestCase):
    def test_only_letters_and_digits_survive_in_lowercase(self):
        self.assertEqual(normalize("DCS-7280QR-C72-M-F"), "dcs7280qrc72mf")
        self.assertEqual(normalize(" ACS 8000 "), "acs8000")
        self.assertEqual(normalize(None), "")


class DeviceTypeMatchingTest(TypeMatchingFixture):
    def test_a_type_that_already_matches_exactly_is_left_alone(self):
        rows = [devicetype_row("CM7148-2-DAC", "opengear-cm7148-2-dac")]

        self.assertIs(self.match("dcim.devicetype", rows), rows)

    def test_a_matched_device_type_row_is_dropped_so_the_type_is_never_written(self):
        rows = [
            devicetype_row(
                "Opengear CM7148-2-DAC",
                "opengear-opengear-cm7148-2-dac",
                part_number="Opengear CM7148-2-DAC",
            ),
            devicetype_row("Opengear Brand New 9000", "new-9000"),
        ]

        kept = self.match("dcim.devicetype", rows)

        # The type exists, so its row is not applied; an unmatched model still is.
        self.assertEqual([r["model"] for r in kept], ["Opengear Brand New 9000"])

    def test_a_difference_of_one_hyphen_is_matched(self):
        DeviceType.objects.create(
            manufacturer=self.cisco, model="DCS-7280QR-C72M-F", slug="dcs-7280qr-c72m-f"
        )
        rows = [
            {
                "name": "d",
                **devicetype_row(
                    "DCS-7280QR-C72-M-F",
                    "dcs-7280qr-c72-m-f",
                    manufacturer_slug="cisco",
                ),
                "device_type": "DCS-7280QR-C72-M-F",
                "device_type_slug": "dcs-7280qr-c72-m-f",
            }
        ]

        (row,) = self.match("dcim.device", rows)

        self.assertEqual(row["device_type"], "DCS-7280QR-C72M-F")
        self.assertEqual(row["device_type_slug"], "dcs-7280qr-c72m-f")

    def test_a_part_number_names_the_library_model(self):
        DeviceType.objects.create(
            manufacturer=self.cisco,
            model="ISR4331",
            slug="cisco-isr4331",
            part_number="ISR4331/K9",
        )
        rows = [
            {
                **devicetype_row("ISR4331/K9", "isr4331-k9", manufacturer_slug="cisco"),
                "device_type": "ISR4331/K9",
                "device_type_slug": "isr4331-k9",
            }
        ]

        (row,) = self.match("dcim.device", rows)

        self.assertEqual(row["device_type"], "ISR4331")
        self.assertEqual(row["device_type_slug"], "cisco-isr4331")

    def test_two_candidates_are_ambiguous_and_nothing_is_rewritten(self):
        DeviceType.objects.create(
            manufacturer=self.opengear, model="CM 7148-2-DAC", slug="cm-7148-2-dac"
        )
        rows = [devicetype_row("Opengear CM7148-2-DAC", "x")]

        self.assertEqual(self.match("dcim.devicetype", rows), rows)

    def test_a_different_manufacturer_is_never_matched_across(self):
        rows = [devicetype_row("CM7148-2-DAC", "x", manufacturer_slug="cisco")]

        self.assertIs(self.match("dcim.devicetype", rows), rows)

    def test_an_unknown_manufacturer_is_left_alone(self):
        rows = [devicetype_row("CM7148-2-DAC", "x", manufacturer_slug="nobody")]

        self.assertIs(self.match("dcim.devicetype", rows), rows)

    def test_a_type_with_no_counterpart_is_left_for_the_sync_to_create(self):
        rows = [devicetype_row("Opengear Brand New 9000", "new-9000")]

        self.assertIs(self.match("dcim.devicetype", rows), rows)

    def test_a_name_too_short_to_say_anything_is_never_matched(self):
        DeviceType.objects.create(
            manufacturer=self.opengear, model="A1", slug="opengear-a1"
        )
        rows = [devicetype_row("a-1", "a-1")]

        self.assertIs(self.match("dcim.devicetype", rows), rows)

    def test_matching_is_idempotent_and_never_touches_its_input(self):
        original = {
            "manufacturer_slug": "opengear",
            "device_type": "Opengear CM7148-2-DAC",
            "device_type_slug": "x",
        }
        rows = [original]

        once = self.match("dcim.device", rows)
        twice = self.match("dcim.device", once)

        self.assertEqual(original["device_type"], "Opengear CM7148-2-DAC")
        self.assertEqual(once, twice)
        self.assertIs(twice, once)

    def test_a_model_that_names_no_type_is_returned_as_it_came(self):
        rows = [{"name": "x"}]

        self.assertIs(self.match("dcim.site", rows), rows)


class OtherRowKindsTest(TypeMatchingFixture):
    def test_a_device_row_points_at_the_existing_type(self):
        rows = [
            {
                "name": "dev-1",
                **devicetype_row("Opengear CM7148-2-DAC", "x"),
                "device_type": "Opengear CM7148-2-DAC",
                "device_type_slug": "opengear-opengear-cm7148-2-dac",
            }
        ]

        (row,) = self.match("dcim.device", rows)

        self.assertEqual(row["device_type"], "CM7148-2-DAC")
        self.assertEqual(row["device_type_slug"], "opengear-cm7148-2-dac")
        self.assertEqual(row["name"], "dev-1")

    def test_a_hardware_notice_carries_no_manufacturer_so_it_matches_only_if_unique(
        self,
    ):
        rows = [
            {
                "device_type": "Opengear CM7148-2-DAC",
                "device_type_slug": "opengear-opengear-cm7148-2-dac",
            }
        ]

        (row,) = self.match("netbox_dlm.hardwarenotice", rows)
        self.assertEqual(row["device_type_slug"], "opengear-cm7148-2-dac")

        DeviceType.objects.create(
            manufacturer=self.cisco,
            model="Opengear-CM7148-2-DAC",
            slug="cisco-og-cm7148",
        )
        self.assertIs(self.match("netbox_dlm.hardwarenotice", rows), rows)

    def test_a_module_row_points_at_the_existing_module_type(self):
        ModuleType.objects.create(
            manufacturer=self.cisco, model="N9K-C93180YC-FX3", part_number="PN-1"
        )
        rows = [
            {
                "device": "d",
                "module_bay": "1",
                "manufacturer": "Cisco",
                "manufacturer_slug": "cisco",
                "model": "N9K C93180YC FX3",
                "part_number": "N9K C93180YC FX3",
                "status": "active",
            }
        ]

        (row,) = self.match("dcim.module", rows)

        self.assertEqual(row["model"], "N9K-C93180YC-FX3")
        self.assertEqual(row["part_number"], "PN-1")
        self.assertNotIn("slug", row)

    def test_a_matched_module_type_keeps_its_description_and_comments(self):
        ModuleType.objects.create(
            manufacturer=self.cisco,
            model="C9500-48Y4C",
            part_number="PN-2",
            description="Library description",
            comments="Library comments",
        )
        rows = [
            {
                "manufacturer_slug": "cisco",
                "model": "C9500 48Y4C",
                "part_number": "C9500 48Y4C",
                "description": "Forward's component text",
                "comments": "",
            }
        ]

        (row,) = self.match("dcim.module", rows)

        self.assertEqual(row["description"], "Library description")
        self.assertEqual(row["comments"], "Library comments")

    def test_a_vendor_written_in_front_matches_only_a_type_of_that_vendor(self):
        # "Dell 3048" must not land on another vendor's unique "3048".
        DeviceType.objects.create(
            manufacturer=self.cisco, model="3048", slug="cisco-3048"
        )
        rows = [{"device_type": "Dell 3048", "device_type_slug": "dell-3048"}]

        self.assertIs(self.match("netbox_dlm.hardwarenotice", rows), rows)

        dell = Manufacturer.objects.create(name="Dell", slug="dell")
        DeviceType.objects.create(manufacturer=dell, model="3048", slug="dell-3048-x")
        (row,) = self.match("netbox_dlm.hardwarenotice", rows)
        self.assertEqual(row["device_type_slug"], "dell-3048-x")


class ApplyUsesTheExistingTypeTest(TypeMatchingFixture):
    def test_a_matched_type_is_never_applied_and_a_device_row_finds_it(self):
        from forward_netbox.tests.test_subinterface_apply import SubinterfaceFixture
        from forward_netbox.utilities.sync import ForwardSyncRunner

        fixture = SubinterfaceFixture()
        fixture.setUp()
        runner = ForwardSyncRunner(
            sync=fixture.sync, ingestion=None, client=None, logger_=mock.Mock()
        )
        type_row = {
            **devicetype_row("Opengear CM7148-2-DAC", "x"),
            "part_number": "Opengear CM7148-2-DAC",
        }
        device_row = {
            **devicetype_row("Opengear CM7148-2-DAC", "x"),
            "name": "dev-1",
            "device_type": "Opengear CM7148-2-DAC",
            "device_type_slug": "x",
        }
        before = DeviceType.objects.count()

        # The type's own row is not applied, so the library type cannot be written.
        self.assertEqual(self.match("dcim.devicetype", [type_row]), [])
        (resolved,) = self.match("dcim.device", [device_row])
        found = runner._get_unique_or_raise(
            DeviceType,
            {"manufacturer": self.opengear, "model": resolved["device_type"]},
        )

        self.assertEqual(found.pk, self.library.pk)
        self.assertEqual(DeviceType.objects.count(), before)
        self.library.refresh_from_db()
        self.assertEqual(self.library.model, "CM7148-2-DAC")
        self.assertEqual(self.library.slug, "opengear-cm7148-2-dac")
        self.assertEqual(self.library.part_number, "CM7148-2-DAC")


class TypeIndexTest(TestCase):
    def test_a_missing_manufacturer_slug_searches_every_manufacturer(self):
        index = TypeIndex(
            [
                {
                    "pk": 1,
                    "manufacturer_slug": "a",
                    "manufacturer_name": "A",
                    "model": "Widget 9000",
                    "slug": "a-widget",
                    "part_number": "",
                }
            ]
        )

        hit = index.resolve(manufacturer_slug="", model="widget-9000")

        self.assertEqual(hit["pk"], 1)


class PartNumberRuleTest(TypeMatchingFixture):
    def test_a_truncated_description_is_not_taken_for_a_part_number(self):
        # The module query truncates the part to 50 characters; a value that long is
        # a description, and must not pull a new module onto an old type.
        long_part = "Cisco Nexus 9300 series 48-port 10G SFP+ line card X"[:50]
        ModuleType.objects.create(
            manufacturer=self.cisco, model="Old card", part_number=long_part
        )
        rows = [
            {
                "manufacturer_slug": "cisco",
                "model": "Cisco Nexus 9300 series 48-port 10G SFP+ line card Y",
                "part_number": long_part,
            }
        ]

        self.assertIs(self.match("dcim.module", rows), rows)

    def test_a_placeholder_part_number_matches_nothing(self):
        for placeholder in ("N/A", "unknown", "None", "tbd"):
            ModuleType.objects.create(
                manufacturer=self.cisco,
                model=f"Card {placeholder}",
                part_number=placeholder,
            )
        for placeholder in ("N/A", "unknown", "None", "tbd"):
            rows = [
                {
                    "manufacturer_slug": "cisco",
                    "model": "Something else entirely",
                    "part_number": placeholder,
                }
            ]

            self.assertIs(self.match("dcim.module", rows), rows, placeholder)


class ModuleApplyKeepsTheLibraryTypeTest(TypeMatchingFixture):
    def test_applying_a_module_row_shaped_like_the_query_leaves_the_type_untouched(
        self,
    ):
        from forward_netbox.tests.test_subinterface_apply import SubinterfaceFixture
        from forward_netbox.utilities.sync import ForwardSyncRunner

        library = ModuleType.objects.create(
            manufacturer=self.cisco,
            model="C9500-48Y4C",
            part_number="PN-2",
            description="Library description",
            comments="Library comments",
        )
        fixture = SubinterfaceFixture()
        fixture.setUp()
        runner = ForwardSyncRunner(
            sync=fixture.sync, ingestion=None, client=None, logger_=mock.Mock()
        )
        # forward_modules.nqe emits no `comments` column.
        raw = {
            "device": "dev-s",
            "module_bay": "1",
            "manufacturer": "Cisco",
            "manufacturer_slug": "cisco",
            "model": "C9500 48Y4C",
            "part_number": "C9500 48Y4C",
            "description": "Forward's component text",
            "status": "active",
        }
        (row,) = self.match("dcim.module", [raw])
        before = ModuleType.objects.count()

        runner._ensure_module_type(row)

        self.assertEqual(ModuleType.objects.count(), before)
        library.refresh_from_db()
        self.assertEqual(library.model, "C9500-48Y4C")
        self.assertEqual(library.part_number, "PN-2")
        self.assertEqual(library.description, "Library description")
        self.assertEqual(library.comments, "Library comments")


class FetchWiringTest(TypeMatchingFixture):
    """The matcher is applied where rows are fetched, and only to fetched rows."""

    def setUp(self):
        super().setUp()
        from forward_netbox.tests.test_subinterface_apply import SubinterfaceFixture

        fixture = SubinterfaceFixture()
        fixture.setUp()
        self.sync = fixture.sync
        self.client = mock.Mock()

    def fetch(self, model_string, rows, **context_extra):
        from forward_netbox.utilities.forward_api import LATEST_PROCESSED_SNAPSHOT
        from forward_netbox.utilities.query_fetch import ForwardQueryContext
        from forward_netbox.utilities.query_fetch import ForwardQueryFetcher
        from forward_netbox.utilities.query_registry import QuerySpec

        fetcher = ForwardQueryFetcher(
            sync=self.sync, client=self.client, logger_=mock.Mock()
        )
        context = ForwardQueryContext(
            network_id="test-network",
            snapshot_selector=LATEST_PROCESSED_SNAPSHOT,
            snapshot_id="snapshot-1",
            **context_extra,
        )
        spec = QuerySpec(
            model_string=model_string,
            query_name="types",
            query="""
@query
f(forward_netbox_shard_keys: List<String>) =
foreach x select {name: x.name}
""",
            parameters={"forward_netbox_shard_keys": []},
        )
        with mock.patch(
            "forward_netbox.utilities.query_fetch_execution.run_nqe_query",
            return_value=rows,
        ), mock.patch.object(
            fetcher, "_full_run_removals", return_value=[]
        ) as removals:
            result = fetcher._fetch_spec_rows(
                model_string,
                spec,
                baseline=None,
                context=context,
                coalesce_fields=[["manufacturer_slug", "slug"]],
                return_fetch_meta=True,
            )
        return result, removals

    def test_a_matched_type_row_is_skipped_counted_and_never_part_of_a_removal(self):
        matched = devicetype_row("Opengear CM7148-2-DAC", "x")
        unmatched = devicetype_row("Opengear Brand New 9000", "new-9000")

        (rows, delete_rows, mode, meta), removals = self.fetch(
            "dcim.devicetype", [matched, unmatched]
        )

        self.assertEqual([r["model"] for r in rows], ["Opengear Brand New 9000"])
        self.assertEqual(delete_rows, [])
        self.assertEqual(meta["matched_type_rows_skipped"], 1)
        # The removal computation never saw the skipped row.
        current = removals.call_args.kwargs["current_rows"]
        self.assertEqual([r["model"] for r in current], ["Opengear Brand New 9000"])

    def test_with_a_scope_that_keeps_nothing_the_skipped_rows_are_not_counted(self):
        matched = devicetype_row("Opengear CM7148-2-DAC", "x")

        (rows, _delete, _mode, meta), _removals = self.fetch(
            "dcim.devicetype",
            [matched],
            device_tag_include_tags=["no-such-tag"],
            scoped_device_names=set(),
        )

        # Reported as the empty result it is, so the zero-row guard still sees it.
        self.assertEqual(rows, [])
        self.assertEqual(meta["matched_type_rows_skipped"], 0)

    def test_a_device_row_is_pointed_at_the_existing_type_and_nothing_is_skipped(self):
        device = {
            "name": "dev-1",
            **devicetype_row("Opengear CM7148-2-DAC", "x"),
            "device_type": "Opengear CM7148-2-DAC",
            "device_type_slug": "x",
        }

        (rows, _delete, _mode, meta), _removals = self.fetch("dcim.device", [device])

        self.assertEqual(rows[0]["device_type"], "CM7148-2-DAC")
        self.assertEqual(meta["matched_type_rows_skipped"], 0)

    def test_a_model_that_names_no_type_is_fetched_untouched(self):
        site = {"name": "s", "slug": "s"}

        (rows, _delete, _mode, meta), _removals = self.fetch("dcim.site", [site])

        self.assertEqual(rows, [site])
        self.assertEqual(meta["matched_type_rows_skipped"], 0)


class PlaceholderNameTest(TypeMatchingFixture):
    def test_a_placeholder_model_never_matches_a_type_of_that_name(self):
        DeviceType.objects.create(
            manufacturer=self.opengear, model="UNKNOWN", slug="opengear-unknown"
        )
        rows = [
            {
                "manufacturer_slug": "opengear",
                "device_type": "Unknown",
                "device_type_slug": "unknown",
            }
        ]

        self.assertIs(self.match("dcim.device", rows), rows)


class DeterminismTest(TestCase):
    def test_an_ambiguous_vendor_split_is_never_resolved_by_set_order(self):
        # "HPE 1920" reads as HP + "E1920" or as HPE + "1920". Both are types NetBox
        # holds, so it is ambiguous and the row is left alone, every time, whatever
        # order string hashing puts the vendors in.
        hp = Manufacturer.objects.create(name="HP", slug="hp")
        hpe = Manufacturer.objects.create(name="HPE", slug="hpe")
        DeviceType.objects.create(manufacturer=hpe, model="1920", slug="hpe-1920")
        DeviceType.objects.create(manufacturer=hp, model="E1920", slug="hp-e1920")
        rows = [{"device_type": "HPE 1920", "device_type_slug": "hpe-1920-x"}]

        for _ in range(5):
            self.assertIs(match_existing_types("netbox_dlm.hardwarenotice", rows), rows)

    def test_with_only_one_reading_available_it_is_used_every_time(self):
        hpe = Manufacturer.objects.create(name="HPE", slug="hpe")
        DeviceType.objects.create(manufacturer=hpe, model="1920", slug="hpe-1920")
        rows = [{"device_type": "HPE 1920", "device_type_slug": "hpe-1920-x"}]

        results = {
            tuple(
                (r["device_type"], r["device_type_slug"])
                for r in match_existing_types("netbox_dlm.hardwarenotice", rows)
            )
            for _ in range(5)
        }

        self.assertEqual(results, {(("1920", "hpe-1920"),)})


class NoticeAuditSeesWhatTheSyncSeesTest(TypeMatchingFixture):
    def test_the_audit_reads_matched_rows_not_forwards_raw_ones(self):
        from django.contrib.contenttypes.models import ContentType

        from forward_netbox.utilities.dlm_notice_audit import (
            fetch_emitted_hardware_notice_rows,
        )

        raw = [
            {
                "device_type": "Opengear CM7148-2-DAC",
                "device_type_slug": "opengear-opengear-cm7148-2-dac",
            }
        ]
        nqe_map = mock.Mock(query_id="q", commit_id="", parameters={})
        nqe_map.name = "notices"
        sync = mock.Mock()
        with mock.patch.object(ContentType.objects, "get"), mock.patch(
            "forward_netbox.models.ForwardNQEMap.objects.filter"
        ) as maps, mock.patch(
            "forward_netbox.utilities.forward_api.run_nqe_query", return_value=raw
        ):
            maps.return_value.order_by.return_value = [nqe_map]

            rows, error = fetch_emitted_hardware_notice_rows(sync, client=mock.Mock())

        self.assertEqual(error, "")
        # The audit sees the slug the sync writes the notice under, so it does not
        # read that notice as stale and prune it.
        self.assertEqual(rows[0]["device_type_slug"], "opengear-cm7148-2-dac")
