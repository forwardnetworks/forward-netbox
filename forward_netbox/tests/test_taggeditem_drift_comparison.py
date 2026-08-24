# The first adapter-only model to get a drift comparison.
#
# The bulk-ORM models classify a whole batch and return counts; the adapter
# models have no bulk path at all, so each row is applied on its own and the
# loop belongs to the caller. `extras.taggeditem` is the smallest of the eight
# and goes first for that reason - it proves the shape the rest will use.
#
# It also carries the trap that makes these paths different from the bulk ones.
# `apply_extras_taggeditem` writes twice: the `Tag` row through
# `runner._upsert_values_from_defaults`, which the preview runner overrides, and
# `device.tags.add(tag)` - an M2M write reached through a module-level helper
# rather than a `runner.` call, so the preview runner's firewall does NOT stop
# it. A grep for `bulk_create`/`.save(`/`.delete(` sees neither.
from dcim.models import Device
from dcim.models import DeviceRole
from dcim.models import DeviceType
from dcim.models import Manufacturer
from dcim.models import Site
from django.test import TestCase
from extras.models import Tag
from extras.models import TaggedItem

from forward_netbox.utilities.drift_comparison import compare_model_rows


class TaggedItemPreviewTest(TestCase):
    def setUp(self):
        site = Site.objects.create(name="T Site", slug="t-site")
        mfr = Manufacturer.objects.create(name="T Mfr", slug="t-mfr")
        dtype = DeviceType.objects.create(manufacturer=mfr, model="T DT", slug="t-dt")
        role = DeviceRole.objects.create(name="T Role", slug="t-role")
        self.device = Device.objects.create(
            name="tagged-dev",
            site=site,
            device_type=dtype,
            role=role,
            status="active",
        )

    def _row(self, **extra):
        row = {
            "device": "tagged-dev",
            "tag": "Prot BGP",
            "tag_slug": "prot-bgp",
            "tag_color": "9e9e9e",
        }
        row.update(extra)
        return row

    # --- the negative space -------------------------------------------------
    #
    # This is the assertion the feature is built around. Every other test here
    # could pass against a preview that quietly tagged the operator's devices.

    def test_a_preview_creates_no_tag_and_no_assignment(self):
        tags_before = Tag.objects.count()
        assignments_before = TaggedItem.objects.count()

        result = compare_model_rows(None, "extras.taggeditem", [self._row()])

        self.assertEqual(Tag.objects.count(), tags_before)
        self.assertEqual(TaggedItem.objects.count(), assignments_before)
        self.assertFalse(Tag.objects.filter(slug="prot-bgp").exists())
        # And it still answered, rather than declining.
        self.assertEqual(result["creates"], 1)

    def test_a_preview_does_not_assign_a_tag_that_already_exists(self):
        # The tag is present but unassigned, so only the M2M write is left to
        # suppress - the case where the `_upsert_values_from_defaults` override
        # alone would not have been enough.
        Tag.objects.create(name="Prot BGP", slug="prot-bgp", color="9e9e9e")
        self.assertEqual(self.device.tags.count(), 0)

        result = compare_model_rows(None, "extras.taggeditem", [self._row()])

        self.device.refresh_from_db()
        self.assertEqual(self.device.tags.count(), 0)
        self.assertEqual(result["creates"], 1)

    def test_a_preview_does_not_rewrite_a_drifted_tag(self):
        Tag.objects.create(name="Prot BGP", slug="prot-bgp", color="ff0000")
        self.device.tags.add(Tag.objects.get(slug="prot-bgp"))

        compare_model_rows(None, "extras.taggeditem", [self._row()])

        self.assertEqual(Tag.objects.get(slug="prot-bgp").color, "ff0000")

    # --- classification -----------------------------------------------------

    def test_an_absent_tag_is_a_create(self):
        result = compare_model_rows(None, "extras.taggeditem", [self._row()])

        self.assertEqual(
            result, {"creates": 1, "updates": 0, "unchanged": 0, "rejected": 0}
        )

    def test_an_existing_assignment_is_unchanged(self):
        tag = Tag.objects.create(name="Prot BGP", slug="prot-bgp", color="9e9e9e")
        self.device.tags.add(tag)

        result = compare_model_rows(None, "extras.taggeditem", [self._row()])

        self.assertEqual(
            result, {"creates": 0, "updates": 0, "unchanged": 1, "rejected": 0}
        )

    def test_an_assigned_tag_whose_colour_drifted_is_an_update(self):
        # The assignment exists, so this is not a create - but the apply would
        # still rewrite the Tag row, and a preview that called that "unchanged"
        # would under-report drift.
        tag = Tag.objects.create(name="Prot BGP", slug="prot-bgp", color="ff0000")
        self.device.tags.add(tag)

        result = compare_model_rows(None, "extras.taggeditem", [self._row()])

        self.assertEqual(
            result, {"creates": 0, "updates": 1, "unchanged": 0, "rejected": 0}
        )

    def test_a_tag_matched_by_name_when_the_slug_differs_is_not_a_create(self):
        # The apply coalesces on slug first, then name, so a hand-created tag
        # with the same name and a different slug is REUSED rather than created.
        # The comparison has to agree, or it reports a create for a row the
        # apply would not create.
        tag = Tag.objects.create(
            name="Prot BGP", slug="prot-bgp-legacy", color="9e9e9e"
        )
        self.device.tags.add(tag)

        result = compare_model_rows(None, "extras.taggeditem", [self._row()])

        self.assertEqual(result["creates"], 0)

    def test_a_row_naming_an_unknown_device_is_rejected_not_zero_drift(self):
        result = compare_model_rows(
            None, "extras.taggeditem", [self._row(device="no-such-device")]
        )

        self.assertEqual(result["rejected"], 1)
        self.assertEqual(result["creates"], 0)
        self.assertEqual(result["unchanged"], 0)

    def test_a_row_missing_its_device_key_is_rejected(self):
        result = compare_model_rows(
            None, "extras.taggeditem", [{"tag": "X", "tag_slug": "x"}]
        )

        self.assertEqual(result["rejected"], 1)
        self.assertEqual(result["unchanged"], 0)

    def test_a_mixed_batch_is_not_all_or_nothing(self):
        tag = Tag.objects.create(name="Prot BGP", slug="prot-bgp", color="9e9e9e")
        self.device.tags.add(tag)

        result = compare_model_rows(
            None,
            "extras.taggeditem",
            [
                self._row(),
                self._row(tag="Prot OSPF", tag_slug="prot-ospf"),
                self._row(device="no-such-device"),
            ],
        )

        self.assertEqual(
            result, {"creates": 1, "updates": 0, "unchanged": 1, "rejected": 1}
        )

    # --- parity with the apply ----------------------------------------------

    def test_the_preview_count_matches_what_an_apply_actually_writes(self):
        """The property the feature sells: preview and apply agree.

        A comparison that is merely self-consistent is worth nothing; what makes
        the number usable is that applying afterwards changes exactly as many
        rows as the preview said it would.
        """
        rows = [
            self._row(),
            self._row(tag="Prot OSPF", tag_slug="prot-ospf"),
        ]

        predicted = compare_model_rows(None, "extras.taggeditem", rows)

        assignments_before = self.device.tags.count()
        self._apply_for_real(rows)
        self.device.refresh_from_db()
        actually_written = self.device.tags.count() - assignments_before

        self.assertEqual(predicted["creates"], actually_written)

    def test_a_second_preview_after_the_apply_reports_no_drift(self):
        rows = [self._row()]
        self._apply_for_real(rows)

        result = compare_model_rows(None, "extras.taggeditem", rows)

        self.assertEqual(result["unchanged"], 1)
        self.assertEqual(result["creates"], 0)

    def _apply_for_real(self, rows):
        from forward_netbox.utilities.sync_interface import apply_extras_taggeditem

        runner = _WritingRunner()
        for row in rows:
            apply_extras_taggeditem(runner, row)


class _WritingRunner:
    """The real primitives, no preview - so the parity tests apply for real."""

    def __init__(self):
        self._content_types = {}
        self._device_by_name_cache = {}
        self._missing_device_by_name_cache = set()
        self._tag_by_name_cache = {}
        self._tag_by_slug_cache = {}
        self._unique_lookup_cache = {}
        self._primed_missing_unique_lookup_keys = set()
        self._model_coalesce_fields = {}
        self._device_tag_ids_cache = {}
        self.logger = _NullLogger()

    def _get_device_by_name(self, name):
        from forward_netbox.utilities.sync_primitives import get_device_by_name

        return get_device_by_name(self, name)

    def _upsert_values_from_defaults(self, *args, **kwargs):
        from forward_netbox.utilities.sync_primitives import (
            upsert_values_from_defaults,
        )

        return upsert_values_from_defaults(self, *args, **kwargs)

    def _dependency_failed(self, model_string, key):
        return False

    def _record_issue(self, *args, **kwargs):
        return None

    def _conflict_policy(self, model_string):
        # What the real runner defaults to for a model with no configured
        # policy. `coalesce_upsert` asks for it on every write.
        return "strict"


class _NullLogger:
    def increment_statistics(self, *args, **kwargs):
        return None

    def log_warning(self, *args, **kwargs):
        return None
