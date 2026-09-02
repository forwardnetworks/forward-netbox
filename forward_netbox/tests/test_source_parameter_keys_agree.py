# Every parameter the source form writes must be a key the model accepts.
#
# They are two hand-maintained lists of one fact, and they disagreed silently:
# `config_backup_include_unmanaged` was added to the form and not to
# `clean_forward_source`'s allowlist, so EVERY source save failed with
# "Unsupported Forward source keys" - not the new feature, every save. Nothing
# compared the two, so it reached main and was caught only by the full suite.
#
# This compares them directly. It reads the form's own source rather than
# submitting one, because the failure is structural: a key present in one list
# and absent from the other, whatever values a particular submission carries.
import ast
import inspect
from pathlib import Path

from django.test import SimpleTestCase

from forward_netbox.utilities import model_validation


def _allowlisted_keys(function):
    """The literal set this validator subtracts the submitted keys from."""
    tree = ast.parse(inspect.getsource(function))
    for node in ast.walk(tree):
        if isinstance(node, ast.BinOp) and isinstance(node.op, ast.Sub):
            right = node.right
            if isinstance(right, ast.Set):
                return {
                    element.value
                    for element in right.elts
                    if isinstance(element, ast.Constant)
                }
    raise AssertionError(f"could not find the key allowlist in {function.__name__}")


def _allowlisted_source_keys():
    return _allowlisted_keys(model_validation.clean_forward_source)


def _allowlisted_sync_keys():
    return _allowlisted_keys(model_validation.clean_forward_sync)


def _form_written_keys():
    """Every string key assigned into a parameters dict in forms.py.

    The forms build several - source parameters and sync parameters, from both
    the source form and the sync form - and the keys are literals in all of
    them, which is what makes this readable statically and what makes the
    drift possible at all. Which validator owns a given key is not decidable
    here, so the assertion below is the one that always holds: a key the forms
    write must be accepted by SOME validator. A key in neither is the defect.
    """
    source = (Path(model_validation.__file__).parent.parent / "forms.py").read_text()
    tree = ast.parse(source)
    keys = set()
    for node in ast.walk(tree):
        if not isinstance(node, ast.Assign):
            continue
        target = node.targets[0]
        name = (
            target.attr
            if isinstance(target, ast.Attribute)
            else getattr(target, "id", "")
        )
        if name not in ("parameters", "candidate_parameters"):
            continue
        if not isinstance(node.value, ast.Dict):
            continue
        keys |= {key.value for key in node.value.keys if isinstance(key, ast.Constant)}
    return keys


class SourceParameterKeysAgreeTest(SimpleTestCase):
    def test_every_key_the_forms_write_is_accepted_by_a_validator(self):
        written = _form_written_keys()
        allowed = _allowlisted_source_keys() | _allowlisted_sync_keys()
        self.assertTrue(written, "found no parameter keys in forms.py")
        unsupported = sorted(written - allowed)
        self.assertEqual(
            unsupported,
            [],
            "the forms write parameters no validator accepts, so saving fails "
            f"for every source or sync - not only the new feature: {unsupported}",
        )

    def test_both_allowlists_are_found_and_not_empty(self):
        # A parser that silently found nothing would make the test above pass
        # for the wrong reason.
        source_keys = _allowlisted_source_keys()
        self.assertIn("username", source_keys)
        self.assertIn("config_backup_data_source", source_keys)
        self.assertIn("config_backup_include_unmanaged", source_keys)
        self.assertIn("auto_merge", _allowlisted_sync_keys())

    def test_the_form_key_reader_sees_the_source_parameters(self):
        written = _form_written_keys()
        self.assertIn("config_backup_include_unmanaged", written)
        self.assertIn("device_tag_include_tags", written)
