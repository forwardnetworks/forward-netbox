"""An empty row list must not be reported as a successful comparison.

A deployment's drift report showed 2 of 32 models measured. One of the two was
`netbox_dlm.softwareversion`, carrying a green `In sync: Yes` against 45
Forward rows. That model is adapter-only: it appears nowhere in the comparison
dispatcher, so it has no comparison and cannot be measured. The badge came from
a shortcut that returned a confident zero whenever the row list was empty,
without ever asking whether the model was one this code can compare.

The two situations are indistinguishable at that point - a model with genuinely
nothing incoming, and a model whose rows never arrived - and only one of them
justifies a zero. So the dispatcher answers for both, and these tests pin the
part that has a consequence: an uncomparable model says `None` no matter how
many rows it is handed.
"""

from django.test import TestCase

from forward_netbox.utilities.drift_comparison import compare_model_rows


class AnUncomparableModelNeverReportsZeroTest(TestCase):
    """The failure that reached a customer: zero rows read as zero drift."""

    def test_adapter_only_model_with_no_rows_is_not_measured(self):
        # `netbox_dlm.softwareversion` is applied by `sync_dlm`, not by the
        # bulk dispatcher, so there is nothing here that could compare it.
        self.assertIsNone(compare_model_rows(None, "netbox_dlm.softwareversion", []))

    def test_adapter_only_model_with_rows_is_not_measured_either(self):
        # Same answer with rows present. The emptiness was never what made it
        # uncomparable, which is why keying the shortcut off emptiness was wrong.
        self.assertIsNone(
            compare_model_rows(None, "netbox_dlm.softwareversion", [{"version": "1.0"}])
        )

    def test_a_model_that_declines_on_purpose_still_declines_when_empty(self):
        # virtualchassis returns None deliberately; an empty list must not
        # promote it to measured.
        self.assertIsNone(compare_model_rows(None, "dcim.virtualchassis", []))

    def test_an_unknown_model_is_not_measured(self):
        self.assertIsNone(compare_model_rows(None, "nonexistent.model", []))


class AComparableModelStillAnswersZeroTest(TestCase):
    """Removing the shortcut must not cost a real model its measurement."""

    def test_a_spec_model_with_no_rows_is_measured_as_zero(self):
        result = compare_model_rows(None, "dcim.site", [])
        self.assertIsNotNone(
            result,
            "dcim.site has a comparison; an empty row list is a genuine zero "
            "for it, and reporting no comparison would lose coverage the "
            "drift report already had",
        )
        self.assertEqual(result["creates"], 0)
        self.assertEqual(result["updates"], 0)

    def test_a_bespoke_model_with_no_rows_is_measured_as_zero(self):
        result = compare_model_rows(None, "dcim.macaddress", [])
        self.assertIsNotNone(result)
        self.assertEqual(result["creates"], 0)
        self.assertEqual(result["updates"], 0)
