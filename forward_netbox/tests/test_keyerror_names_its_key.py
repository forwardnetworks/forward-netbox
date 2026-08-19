"""A KeyError must name its key when the key is this schema's own vocabulary.

A customer's sync failed after 36 minutes of staging with one ingestion issue:
phase `sync`, model empty, exception `KeyError`, message `Forward ingestion
failed (KeyError).` The job error was `<redacted diagnostic>`, per-model failure
evidence was `[]`, and 117 log rows said only that no classifier was recorded.
Nothing in the support bundle could narrow it further, and the failure is
deterministic, so every retry costs another 36 minutes and produces the same
nothing.

A KeyError is the one exception whose entire diagnostic value IS the key. It is
also the token most likely to be customer data, because a device name is a
plausible dict key. Redacting it wholesale is what made the run undiagnosable.

The distinction the redaction needs is not "is this a string" but "did this
repository choose this name". `MODEL_SYNC_CONTRACTS` is exactly that set. A key
drawn from it is vocabulary this code wrote; anything else stays redacted, and
these tests pin the second half at least as hard as the first.
"""

from django.test import SimpleTestCase

from forward_netbox.utilities.diagnostics import failure_classifier
from forward_netbox.utilities.diagnostics import missing_key_reason
from forward_netbox.utilities.diagnostics import recovered_classifiers
from forward_netbox.utilities.diagnostics import safe_operation_failure


class ASchemaKeyIsNamedTest(SimpleTestCase):
    def test_a_required_contract_field_is_named(self):
        self.assertEqual(missing_key_reason(KeyError("status")), "missing-key-status")
        self.assertEqual(missing_key_reason(KeyError("address")), "missing-key-address")

    def test_underscores_normalise_so_the_reason_reads_back(self):
        # `recovered_classifiers` only recovers lowercase hyphenated slugs. A
        # reason that cannot be read back is discarded by the log renderer and
        # the support bundle, which is how this class of fix was undone before.
        self.assertEqual(
            missing_key_reason(KeyError("mac_address")), "missing-key-mac-address"
        )

    def test_the_rendered_message_survives_the_readback(self):
        message = safe_operation_failure("Forward ingestion", KeyError("status"))
        self.assertEqual(
            message, "Forward ingestion failed (KeyError: missing-key-status)."
        )
        self.assertEqual(
            recovered_classifiers(message), ["KeyError: missing-key-status"]
        )

    def test_the_classifier_carries_it(self):
        self.assertEqual(
            failure_classifier(KeyError("status")), "KeyError: missing-key-status"
        )


class AnythingElseStaysRedactedTest(SimpleTestCase):
    """The half that matters more. A device name must never reach a log row."""

    def test_a_device_name_key_is_not_named(self):
        # RFC-reserved and obviously invented. An earlier draft of this test
        # used a device name copied from a customer's screenshot and a tag name
        # from that estate - in the test that exists to prove such values are
        # never emitted. `.invalid` (RFC 2606) and TEST-NET-1 (RFC 5737) cannot
        # collide with any real fabric.
        for key in (
            "switch-alpha.invalid",
            "router-beta-0042",
            "192.0.2.10",
            "SynthTag_Example",
        ):
            with self.subTest(key=key):
                self.assertEqual(missing_key_reason(KeyError(key)), "")
                self.assertEqual(
                    safe_operation_failure("Forward ingestion", KeyError(key)),
                    "Forward ingestion failed (KeyError).",
                )

    def test_a_key_that_merely_looks_like_a_field_is_not_named(self):
        # Not in any contract, so not this repository's vocabulary.
        self.assertEqual(missing_key_reason(KeyError("hostname")), "")
        self.assertEqual(missing_key_reason(KeyError("serial_number")), "")

    def test_a_non_string_key_is_not_named(self):
        self.assertEqual(missing_key_reason(KeyError(42)), "")
        self.assertEqual(missing_key_reason(KeyError(("device", "eth0"))), "")

    def test_a_multi_argument_keyerror_is_not_named(self):
        # `KeyError(a, b)` is not a missing-key report; do not guess at it.
        self.assertEqual(missing_key_reason(KeyError("device", "extra")), "")

    def test_an_empty_key_is_not_named(self):
        self.assertEqual(missing_key_reason(KeyError("")), "")


class TheSafeSetComesFromTheContractsTest(SimpleTestCase):
    """Not a hand-copied list, which would drift from the contracts."""

    def test_every_named_key_is_declared_by_some_contract(self):
        from forward_netbox.utilities.sync_contracts import MODEL_SYNC_CONTRACTS

        declared = set()
        for contract in MODEL_SYNC_CONTRACTS.values():
            declared.update(contract.required_fields or ())
            declared.update(contract.allowed_coalesce_fields or ())

        # A field the contracts declare is nameable; the reverse is what the
        # redaction depends on and is covered above.
        for field in ("status", "address", "device", "interface"):
            with self.subTest(field=field):
                self.assertIn(field, declared)
                self.assertTrue(missing_key_reason(KeyError(field)))


class AModelLabelIsAlsoOurVocabularyTest(SimpleTestCase):
    """A dict keyed by `model_string` is as common here as one keyed by a field.

    `ipam.ipaddress` is no more customer data than `status` is, and the run this
    was written for died somewhere that iterates models - thirty caught failures
    landed one per model - so a redacted model-label key would have wasted the
    whole fix.
    """

    def test_a_contract_model_label_is_named(self):
        self.assertEqual(
            missing_key_reason(KeyError("ipam.ipaddress")),
            "missing-key-ipam-ipaddress",
        )
        self.assertEqual(
            missing_key_reason(KeyError("dcim.interface")),
            "missing-key-dcim-interface",
        )

    def test_a_model_label_reason_reads_back(self):
        message = safe_operation_failure(
            "Forward ingestion", KeyError("ipam.ipaddress")
        )
        self.assertEqual(
            recovered_classifiers(message), ["KeyError: missing-key-ipam-ipaddress"]
        )

    def test_an_unknown_dotted_key_is_still_redacted(self):
        # Dotted does not mean safe; a hostname is dotted too.
        self.assertEqual(missing_key_reason(KeyError("plugins.notreal")), "")
        self.assertEqual(missing_key_reason(KeyError("sw01.example.net")), "")
