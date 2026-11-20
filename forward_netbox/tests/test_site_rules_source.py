# An operator sets the site mapping rules on the Forward source. The rules have to be
# validated wherever a source is saved (the form and the REST API both go through
# the model), kept when the source is saved for another reason (the form rebuilds
# every parameter from scratch, so a key it forgets is silently lost), and the form
# has to tell the operator what the default is: the Forward location is the NetBox
# site unless a mapping applies.
from unittest.mock import patch

from django.core.exceptions import ValidationError
from django.test import TestCase

from forward_netbox.forms import ForwardSourceForm
from forward_netbox.models import ForwardSource

RULES = [
    {"kind": "location", "pattern": "Hall A*", "site": "Campus East"},
    {"kind": "tag", "pattern": "Prod", "site": "core"},
]
RULES_TEXT = "location Hall A* -> Campus East\ntag Prod -> core"


def _source(**parameters):
    return ForwardSource(
        name="rules-source",
        type="saas",
        url="https://fwd.app",
        parameters={
            "username": "user@example.com",
            "password": "secret",
            "verify": True,
            "network_id": "test-network",
            **parameters,
        },
    )


class ModelValidationTest(TestCase):
    def test_a_source_with_no_rules_is_valid(self):
        _source().clean()

    def test_valid_rules_are_accepted(self):
        _source(site_rules=RULES).clean()

    def test_rules_that_are_not_valid_say_which_rule_and_why(self):
        source = _source(
            site_rules=[{"kind": "location", "pattern": "*", "site": "campus"}]
        )

        with self.assertRaisesRegex(ValidationError, "Rule 1.*every device"):
            source.clean()

    def test_rules_must_be_a_list(self):
        with self.assertRaisesRegex(ValidationError, "list of rules"):
            _source(site_rules="location Hall* -> campus").clean()


class FormTest(TestCase):
    def _data(self, **overrides):
        data = {
            "name": "rules-source",
            "type": "saas",
            "username": "user@example.com",
            "password": "secret",
            "network_id": "test-network",
            "device_tag_filter_mode": "local",
            "device_tag_include_match": "any",
        }
        data.update(overrides)
        return data

    def _save(self, instance=None, **overrides):
        form = ForwardSourceForm(data=self._data(**overrides), instance=instance)
        with patch.object(ForwardSource, "validate_connection"):
            self.assertTrue(form.is_valid(), form.errors.as_json())
            return form.save()

    def test_the_field_is_on_the_form_and_in_a_fieldset(self):
        form = ForwardSourceForm()
        self.assertIn("site_rules", form.fields)
        listed = []
        for fieldset in form.fieldsets:
            listed.extend(
                str(name)
                for name in (
                    getattr(fieldset, "items", None)
                    or getattr(fieldset, "fields", None)
                    or []
                )
            )
        self.assertIn("site_rules", listed)

    def test_the_help_text_says_the_forward_location_is_the_default(self):
        help_text = ForwardSourceForm().fields["site_rules"].help_text

        self.assertIn("Forward location", help_text)
        self.assertIn("unless a mapping applies", help_text)
        # The precedence an operator needs to read the list correctly.
        self.assertIn("tag", help_text.lower())

    def test_saved_rules_show_as_one_line_each(self):
        source = _source(site_rules=RULES)
        source.save()

        form = ForwardSourceForm(instance=source)

        self.assertEqual(form.fields["site_rules"].initial, RULES_TEXT)

    def test_a_source_with_no_rules_shows_an_empty_field(self):
        source = _source()
        source.save()

        self.assertFalse(
            ForwardSourceForm(instance=source).fields["site_rules"].initial
        )

    def test_rules_typed_into_the_field_are_validated_and_saved(self):
        saved = self._save(site_rules=RULES_TEXT)

        self.assertEqual(saved.parameters["site_rules"], RULES)

    def test_a_line_that_is_not_a_rule_is_an_error_on_the_field(self):
        form = ForwardSourceForm(data=self._data(site_rules="Hall* -> campus"))

        with patch.object(ForwardSource, "validate_connection"):
            self.assertFalse(form.is_valid())

        self.assertIn("Line 1", str(form.errors["site_rules"]))

    def test_a_rule_that_is_not_valid_is_an_error_on_the_field(self):
        form = ForwardSourceForm(data=self._data(site_rules="location * -> campus"))

        with patch.object(ForwardSource, "validate_connection"):
            self.assertFalse(form.is_valid())

        self.assertIn("Rule 1", str(form.errors["site_rules"]))

    def test_saving_the_source_for_another_reason_keeps_the_rules(self):
        # Rotating the password posts the field as it was shown. The form rebuilds
        # every parameter from scratch, so a key it forgets is lost.
        saved = self._save(site_rules=RULES_TEXT)

        resaved = self._save(instance=saved, site_rules=RULES_TEXT, password="new")

        self.assertEqual(resaved.parameters["site_rules"], RULES)

    def test_a_save_that_does_not_post_the_field_keeps_the_rules(self):
        saved = self._save(site_rules=RULES_TEXT)

        resaved = self._save(instance=saved, password="new")

        self.assertEqual(resaved.parameters["site_rules"], RULES)

    def test_clearing_the_field_removes_the_rules(self):
        saved = self._save(site_rules=RULES_TEXT)

        resaved = self._save(instance=saved, site_rules="")

        self.assertNotIn("site_rules", resaved.parameters)

    def test_a_source_that_never_had_rules_gets_no_key(self):
        self.assertNotIn("site_rules", self._save().parameters)

    def test_saving_the_form_keeps_enable_predict(self):
        # The save path rebuilt the parameters without this key, so every save of
        # the source form turned the option off.
        saved = self._save(enable_predict="on")
        self.assertTrue(saved.parameters["enable_predict"])

        resaved = self._save(instance=saved, enable_predict="on", password="new")

        self.assertTrue(resaved.parameters["enable_predict"])


class WidgetTest(TestCase):
    def _html(self, source_id=None, value=RULES_TEXT):
        from forward_netbox.widgets import SiteRulesWidget

        widget = SiteRulesWidget()
        widget.source_id = source_id
        return widget.render("site_rules", value, attrs={"id": "id_site_rules"})

    def test_the_textarea_is_the_field_and_holds_the_rules(self):
        html = self._html()

        self.assertIn("<textarea", html)
        self.assertIn('name="site_rules"', html)
        self.assertIn("location Hall A* -&gt; Campus East", html)

    def test_a_saved_source_loads_tags_from_forward_for_the_dropdown(self):
        html = self._html(source_id=7)

        self.assertIn('data-source-id="7"', html)
        self.assertIn("/api/plugins/forward/source/available-tags/", html)

    def test_an_unsaved_source_says_to_save_it_to_pick_tags(self):
        html = self._html(source_id=None)

        self.assertIn('data-source-id=""', html)
        self.assertIn("Save the source to pick tags from Forward", html)

    def test_rule_text_cannot_inject_markup(self):
        html = self._html(value="location <script>alert(1)</script>* -> x")

        self.assertNotIn("<script>alert(1)</script>", html)

    def test_the_builder_offers_both_kinds_of_rule(self):
        html = self._html()

        self.assertIn('value="location"', html)
        self.assertIn('value="tag"', html)


class LocationSuggestionsTest(TestCase):
    def _html(self, source_id=None):
        from forward_netbox.widgets import SiteRulesWidget

        widget = SiteRulesWidget()
        widget.source_id = source_id
        return widget.render("site_rules", "", attrs={"id": "id_site_rules"})

    def test_the_location_field_offers_a_list_of_forward_locations(self):
        html = self._html(source_id=7)

        self.assertIn(
            'data-locations-url="/api/plugins/forward/source/available-locations/"',
            html,
        )
        self.assertIn('list="id_site_rules_locations"', html)
        self.assertIn('<datalist id="id_site_rules_locations"', html)

    def test_the_field_still_takes_any_pattern_typed_by_hand(self):
        # A datalist only suggests; the input stays a free text box, because a rule
        # is a pattern (DC01*), not one location.
        html = self._html(source_id=7)

        self.assertIn("data-location", html)
        self.assertNotIn('<select class="form-select" data-location', html)

    def test_an_unsaved_source_has_the_list_but_nothing_to_load_it_with(self):
        html = self._html(source_id=None)

        self.assertIn('data-source-id=""', html)
        self.assertIn('<datalist id="id_site_rules_locations"', html)

    def test_the_script_loads_the_locations_for_a_saved_source(self):
        html = self._html(source_id=7)

        self.assertIn("data-locations-url", html)
        self.assertIn("loadLocations", html)

    def test_the_list_loads_when_the_field_is_first_used_not_on_every_form_view(self):
        # Every edit of a saved source would otherwise ask Forward twice (a snapshot
        # and a query) before anyone had touched a location rule.
        html = self._html(source_id=7)

        self.assertIn("addEventListener('focus', loadLocations", html)
        self.assertNotIn("\n  loadLocations();", html)


class RulesTableTest(TestCase):
    """The rules as a table of rows with pickers; the textarea stays the field."""

    def _html(self, source_id=7):
        from forward_netbox.widgets import SiteRulesWidget

        widget = SiteRulesWidget()
        widget.source_id = source_id
        return widget.render("site_rules", RULES_TEXT, attrs={"id": "id_site_rules"})

    def test_the_table_and_its_site_list_are_rendered(self):
        html = self._html()

        self.assertIn('data-forward-site-rule-table="id_site_rules"', html)
        self.assertIn('<datalist id="id_site_rules_sites">', html)
        self.assertIn("data-rule-kind", html)
        self.assertIn("data-rule-site", html)

    def test_existing_netbox_sites_are_offered_as_suggestions(self):
        from dcim.models import Site

        Site.objects.create(name="Campus East", slug="campus-east")

        self.assertIn('<option value="Campus East">', self._html())

    def test_a_site_name_cannot_inject_markup(self):
        from dcim.models import Site

        Site.objects.create(name='x"><script>alert(1)</script>', slug="evil")

        self.assertNotIn("<script>alert(1)</script>", self._html())

    def test_the_textarea_is_still_the_posted_field(self):
        html = self._html()

        self.assertIn('<textarea name="site_rules"', html)
        self.assertIn("function writeBack", html)
        # Lines the table cannot show leave the text editor in charge.
        self.assertIn("if (!rules) { table.hidden = true; area.hidden = false;", html)

    def test_the_table_has_an_add_row_button_and_a_way_back_from_text(self):
        html = self._html()

        self.assertIn("data-add-row", html)
        self.assertIn("data-show-table", html)

    def test_a_half_filled_row_blocks_the_save_instead_of_being_dropped(self):
        html = self._html()

        self.assertIn("addEventListener('submit'", html)
        self.assertIn("event.preventDefault()", html)

    def test_tags_are_asked_for_once_and_only_when_a_tag_control_is_used(self):
        html = self._html()

        # No call at load: the only callers are inside handlers.
        self.assertNotIn("\n    tagsOnce();", html)
        self.assertNotIn("\n  tagsOnce();", html)
        self.assertEqual(html.count("fetch(builder.getAttribute('data-tags-url')"), 1)
        self.assertIn("addEventListener('focus', fill, {once: true})", html)

    def test_loading_tags_never_rewrites_the_text_the_operator_is_editing(self):
        html = self._html()
        fill = html[
            html.index("var fill = function") : html.index(
                "control.addEventListener('focus', fill"
            )
        ]

        self.assertNotIn("writeBack()", fill)

    def test_the_table_reads_a_line_the_way_the_server_does(self):
        # The kind is the first word and the first ' -> ' splits the rest, so a
        # pattern with spaces or tabs round-trips as the server stores it.
        html = self._html()

        self.assertIn("line.indexOf(' ')", html)
        self.assertIn("rest.indexOf(' -> ')", html)
        self.assertNotIn("\\s+(.+?)\\s+->", html)

    def test_each_tag_control_fills_its_list_once(self):
        # A click fires mousedown and focus; both ran the fill and doubled the tags.
        self.assertIn("if (filled) { return; }", self._html())

    def test_edit_as_text_refuses_while_a_row_is_half_filled(self):
        self.assertIn("Finish or remove the half-filled row", self._html())

    def test_the_box_is_not_restored_by_the_browser(self):
        self.assertRegex(self._html(), r'<textarea[^>]*autocomplete="off"')
