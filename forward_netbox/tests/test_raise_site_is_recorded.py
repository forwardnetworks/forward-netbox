"""A failure records where in this package it was raised.

A deployment's sync failed at the same point on two consecutive releases and the
only thing recorded either time was the word `KeyError`: no model, no row, no
key, and a redacted job error. The second failure came AFTER a release whose
stated purpose was making that failure name itself - the fix made the key
nameable, and the key turned out not to be one this repository chose, so it was
correctly withheld and the run was as opaque as before.

The artifact that would have located it in seconds is the frame: file, line,
function. Those are identifiers this repository wrote. Unlike an exception
message they cannot quote a device name or an address, and unlike a rendered
traceback they carry no locals.

They were being dropped only because they arrive attached to a traceback, which
is redacted wholesale and rightly so. These tests pin the separation: frame
locations inside this package are kept, everything else is not.
"""

from django.test import SimpleTestCase

from forward_netbox.utilities.diagnostics import plugin_raise_site
from forward_netbox.utilities.diagnostics import structured_failure_diagnosis


def _raise_key_error_with_a_customer_shaped_key():
    """Raises from inside this package, as the real failure does."""
    cache = {}
    return cache[("switch-alpha.invalid", "eth0")]


def _raise_through_two_plugin_frames():
    return _raise_key_error_with_a_customer_shaped_key()


class TheRaiseSiteIsRecordedTest(SimpleTestCase):
    def test_a_plugin_frame_is_named(self):
        try:
            _raise_key_error_with_a_customer_shaped_key()
        except KeyError as exc:
            site = plugin_raise_site(exc)
        self.assertTrue(site, "a KeyError raised inside the package must name a frame")
        innermost = site[0]
        self.assertIn("forward_netbox/tests/test_raise_site_is_recorded.py", innermost)
        self.assertIn("_raise_key_error_with_a_customer_shaped_key", innermost)

    def test_the_innermost_frame_comes_first(self):
        try:
            _raise_through_two_plugin_frames()
        except KeyError as exc:
            site = plugin_raise_site(exc)
        self.assertGreaterEqual(len(site), 2)
        self.assertIn("_raise_key_error_with_a_customer_shaped_key", site[0])
        self.assertIn("_raise_through_two_plugin_frames", site[1])

    def test_it_reaches_the_persisted_diagnosis(self):
        try:
            _raise_key_error_with_a_customer_shaped_key()
        except KeyError as exc:
            diagnosis = structured_failure_diagnosis(exc)
        self.assertIn("raise_site", diagnosis)
        self.assertIn("test_raise_site_is_recorded.py", diagnosis["raise_site"][0])


class TheKeyValueStillNeverAppearsTest(SimpleTestCase):
    """Naming the line must not become a way to name the value."""

    def test_the_key_is_not_in_the_frame_record(self):
        try:
            _raise_key_error_with_a_customer_shaped_key()
        except KeyError as exc:
            site = plugin_raise_site(exc)
            diagnosis = structured_failure_diagnosis(exc)
        rendered = " ".join(site) + " " + repr(diagnosis)
        for value in ("switch-alpha.invalid", "eth0"):
            self.assertNotIn(value, rendered)

    def test_no_frame_outside_this_package_is_recorded(self):
        """The safety property, stated correctly.

        An earlier version of this test raised `{}["absent"]` and asserted the
        result was empty - but that statement executes inside this very file,
        which IS a package frame, so it was asserting the opposite of what it
        described. What matters is not that some exception yields nothing; it is
        that a traceback passing through third-party code never contributes a
        path from outside this package, because such a path can embed a home
        directory.
        """
        import json

        try:
            json.loads("{not json")
        except ValueError as exc:
            site = plugin_raise_site(exc)

        # The stdlib frames are real and must be absent; this file may appear,
        # because it is part of the package.
        self.assertTrue(
            all(entry.startswith("forward_netbox/") for entry in site),
            f"a frame from outside the package was recorded: {site}",
        )
        for foreign in ("site-packages", "/usr/lib", "json/decoder.py", "/home/"):
            self.assertNotIn(foreign, " ".join(site))

    def test_the_frame_list_is_bounded(self):
        def recurse(depth):
            if depth == 0:
                raise KeyError("deep")
            return recurse(depth - 1)

        try:
            recurse(40)
        except KeyError as exc:
            self.assertLessEqual(len(plugin_raise_site(exc)), 8)


class TheRaiseSiteSurvivesSanitizationTest(SimpleTestCase):
    """Written is not the same as recorded.

    `safe_log_message` REBUILDS a failure line from what it can recover and
    discards everything else, so a detail that is not recoverable is written and
    then thrown away on the path to the log export and the support bundle. A
    previous attempt at richer failure messages was undone exactly that way.
    """

    def _sanitized(self, message, level="failure"):
        from forward_netbox.utilities.diagnostics import sanitize_job_diagnostics

        data = {"logs": [["2026-08-19T20:45:28Z", level, "sync", "/url/", message]]}
        return sanitize_job_diagnostics(data)["logs"][0][4]

    def test_the_frame_survives_the_log_sanitizer(self):
        from forward_netbox.utilities.diagnostics import with_raise_site

        try:
            _raise_key_error_with_a_customer_shaped_key()
        except KeyError as exc:
            diagnosis = structured_failure_diagnosis(exc)
        message = with_raise_site("Forward ingestion failed (KeyError).", diagnosis)
        self.assertIn("test_raise_site_is_recorded.py", message)

        rendered = self._sanitized(message)
        self.assertIn("KeyError", rendered)
        self.assertIn(
            "test_raise_site_is_recorded.py",
            rendered,
            "the frame must survive the rebuild, or it never reaches the bundle",
        )

    def test_the_sanitizer_still_discards_the_message_body(self):
        # The frame is preserved; arbitrary text around it is not.
        rendered = self._sanitized(
            "Forward ingestion failed (KeyError) for switch-alpha.invalid "
            "at forward_netbox/utilities/apply_engine_bulk.py:2575:apply."
        )
        self.assertNotIn("switch-alpha.invalid", rendered)
        self.assertIn("apply_engine_bulk.py:2575", rendered)

    def test_a_forged_frame_outside_the_package_is_not_preserved(self):
        rendered = self._sanitized(
            "Forward ingestion failed (KeyError) at /home/someone/secret.py:1:go."
        )
        self.assertNotIn("/home/someone", rendered)

    def test_a_message_with_no_frame_is_unchanged_in_shape(self):
        from forward_netbox.utilities.diagnostics import with_raise_site

        self.assertEqual(
            with_raise_site("Forward ingestion failed (KeyError).", {}),
            "Forward ingestion failed (KeyError).",
        )


class TheServerLogHoldsTheTracebackTest(SimpleTestCase):
    """The one place a full traceback belongs is the deployment's own log.

    The plugin passed `exc_info` nowhere, so every logger call recorded the
    exception class and nothing more - exactly what the issue row already
    showed. The traceback for a customer's repeated failure survived only
    because RQ re-raises and logs it itself, which meant reading RQ's
    failed-job registry out of Redis to answer "where did it fail".

    The server log never leaves the deployment. The job record, the ingestion
    issue and the support bundle are the exported surfaces, and they stay
    redacted - which the neighbouring tests assert.
    """

    def test_the_sync_failure_recorder_passes_exc_info(self):
        import inspect

        from forward_netbox.utilities import sync_orchestration

        source = inspect.getsource(sync_orchestration._record_forward_sync_failure)
        self.assertIn("exc_info=True", source)

    def test_only_the_python_logger_gets_it(self):
        """The exported surfaces must not have grown a traceback."""
        import inspect

        from forward_netbox.utilities import sync_orchestration

        source = inspect.getsource(sync_orchestration._record_forward_sync_failure)
        # The issue row is still built from the redacted message and the
        # schema-level diagnosis, never from the exception itself.
        self.assertIn("message=message", source)
        self.assertIn("raw_data=diagnosis", source)
        self.assertNotIn("raw_data=exc", source)
        self.assertNotIn("message=str(exc)", source)
