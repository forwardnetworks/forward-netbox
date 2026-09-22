"""Exports carry identifiers, never the values behind them.

Two tiers exist. The GUI and the REST API are in-deployment reads and may show
the device name that collided. A downloaded file is exported to be sent to us,
so it carries primary keys, constraint names, counts and shapes.

These tests pin the boundary at both ends: the single key that may carry values
is dropped on the way out, and there is exactly one way out to drop it at.
"""

import json
import pathlib
import zipfile

from django.test import SimpleTestCase

from forward_netbox.utilities.diagnostics import assert_export_safe_diagnosis
from forward_netbox.utilities.export_redaction import export_safe_payload
from forward_netbox.utilities.export_redaction import OPERATOR_DETAIL_KEY
from forward_netbox.utilities.support_bundle_archive import (
    support_bundle_zip_response,
)


class ExportSafePayloadTest(SimpleTestCase):
    def test_operator_detail_is_dropped_at_any_depth(self):
        payload = {
            "issues": [
                {
                    "message": "Bulk create hit a constraint.",
                    "diagnosis": {
                        "constraint_name": "dcim_device_unique_name_site",
                        "conflicting_pks": [12345],
                        OPERATOR_DETAIL_KEY: {"name": "core-sw-01", "site": "DC11"},
                    },
                }
            ]
        }

        cleaned = export_safe_payload(payload)

        diagnosis = cleaned["issues"][0]["diagnosis"]
        self.assertEqual(diagnosis["constraint_name"], "dcim_device_unique_name_site")
        # The pk survives: it is what makes the row findable without naming it.
        self.assertEqual(diagnosis["conflicting_pks_count"], 1)
        self.assertNotIn(OPERATOR_DETAIL_KEY, diagnosis)
        self.assertNotIn("core-sw-01", json.dumps(cleaned))

    def test_name_bearing_keys_are_dropped_and_id_lists_become_counts(self):
        cleaned = export_safe_payload(
            {
                "uncovered_sample": ["core-sw-01"],
                "device_names": ["core-sw-02"],
                "absent_by_name": {"core-sw-03": 1},
                "quarantined_device_ids": [1, 2, 3],
                "absent_count": 7,
            }
        )

        self.assertEqual(
            cleaned, {"quarantined_device_ids_count": 3, "absent_count": 7}
        )

    def test_a_payload_without_values_is_unchanged(self):
        payload = {"status": "failed", "counts": {"devices": 3}, "flags": [True, False]}

        self.assertEqual(export_safe_payload(payload), payload)


class ExportPathsAreNarrowTest(SimpleTestCase):
    """One way out, so there is one place to redact.

    Six JSON downloads exist, and before this they went through one helper that
    redacted nothing and one that redacted partially. If a future download
    builds its own `JsonResponse`, this fails - which is the point.
    """

    def test_views_builds_json_responses_in_exactly_one_place(self):
        views = (
            pathlib.Path(__file__).resolve().parent.parent / "views.py"
        ).read_text()

        self.assertEqual(views.count("JsonResponse("), 1)

    def test_the_zip_bundle_drops_operator_detail_too(self):
        response = support_bundle_zip_response(
            {"issues": [{OPERATOR_DETAIL_KEY: {"name": "core-sw-01"}, "pk": 12345}]},
            filename="bundle.zip",
            json_filename="bundle.json",
        )

        with zipfile.ZipFile(__import__("io").BytesIO(response.content)) as archive:
            written = archive.read("bundle.json").decode("utf-8")

        self.assertIn("12345", written)
        self.assertNotIn("core-sw-01", written)
        self.assertNotIn(OPERATOR_DETAIL_KEY, written)


class WriteTimeInvariantTest(SimpleTestCase):
    """The guarantee is at the write, not the export.

    `export_safe_payload` is a net. This is what stops a value being recorded
    into an exported field in the first place - and it is honest about its
    limit: it rejects shapes that carry sentences, not hostnames that happen to
    look like slugs. That is what `operator_detail` is for.
    """

    def test_identifiers_and_paths_and_pks_are_accepted(self):
        assert_export_safe_diagnosis(
            {
                "exception_type": "IntegrityError",
                "constraint_name": "dcim_device_unique_name_site",
                "raise_site": ["forward_netbox/utilities/apply_engine_bulk.py:2477:f"],
                "conflicting_pks": [12345, 12346],
                "conflict_count": 2,
                "constraint_fields_resolved": True,
                "netbox_pk": None,
            }
        )

    def test_a_sentence_is_refused(self):
        with self.assertRaises(ValueError) as caught:
            assert_export_safe_diagnosis(
                {"detail": "device core-sw-01 already exists at DC11"}
            )

        self.assertIn(OPERATOR_DETAIL_KEY, str(caught.exception))

    def test_a_nested_mapping_is_refused(self):
        with self.assertRaises(ValueError):
            assert_export_safe_diagnosis({"conflict": {"name": "core-sw-01"}})

    def test_operator_detail_itself_is_exempt(self):
        assert_export_safe_diagnosis(
            {
                "constraint_name": "dcim_device_unique_name_site",
                OPERATOR_DETAIL_KEY: {"name": "core-sw-01", "site": "DC11"},
            }
        )

    def test_a_key_a_stricter_redactor_produced_is_left_alone(self):
        # `unrecognized_validation_rules` carries wording, not identifiers,
        # because `redacted_message_shape` already masked every token that
        # could be a value. Re-checking it here would reject safe output.
        assert_export_safe_diagnosis(
            {"unrecognized_validation_rules": ["on device is already taken"]}
        )


class IssueTierPropertiesTest(SimpleTestCase):
    """The issue page shows both halves; a bundle gets one of them."""

    def _issue(self):
        from forward_netbox.models import ForwardIngestionIssue

        return ForwardIngestionIssue(
            raw_data={
                "constraint_name": "dcim_device_unique_name_site",
                "conflicting_pks": [12345],
                OPERATOR_DETAIL_KEY: {"conflicting_rows": [{"name": "core-sw-01"}]},
            }
        )

    def test_exported_diagnosis_excludes_the_operator_tier(self):
        exported = self._issue().exported_diagnosis

        self.assertEqual(exported["constraint_name"], "dcim_device_unique_name_site")
        self.assertNotIn(OPERATOR_DETAIL_KEY, exported)
        self.assertNotIn("core-sw-01", json.dumps(exported))

    def test_operator_detail_is_reachable_for_the_page(self):
        detail = self._issue().operator_detail

        self.assertEqual(detail["conflicting_rows"][0]["name"], "core-sw-01")


class ParameterValuesAreNamesOnlyTest(SimpleTestCase):
    """Which parameters were sent is diagnostic; their values are the estate.

    The dependency preview persists `model_results`, and the bundle carried
    them raw - so `device_tag_include_tags` shipped the operator's own tag
    names in every export. Which parameters a query was sent is what a stale
    published signature rejects, so that is what survives.
    """

    def test_parameter_values_become_names(self):
        cleaned = export_safe_payload(
            {
                "model_results": [
                    {
                        "model": "dcim.interface",
                        "query_parameters": {
                            "device_tag_include_tags": ["Some.Person"],
                            "sync_endpoints": True,
                        },
                    }
                ]
            }
        )

        result = cleaned["model_results"][0]
        self.assertEqual(
            result["query_parameters_names"],
            ["device_tag_include_tags", "sync_endpoints"],
        )
        self.assertNotIn("query_parameters", result)
        self.assertNotIn("Some.Person", json.dumps(cleaned))
