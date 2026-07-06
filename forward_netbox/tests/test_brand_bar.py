from django.template.loader import render_to_string
from django.test import TestCase


class BrandBarTemplateTest(TestCase):
    """The Forward Integration for NetBox brand bar must render the logo + label, and
    must NOT leak its template comment as visible text (a multi-line {# #} comment
    is not a comment in Django and rendered literally on the page)."""

    def test_brand_bar_renders_without_leaking_comment(self):
        html = render_to_string("forward_netbox/inc/brand_bar.html")
        self.assertIn("fwd-brand-bar", html)
        self.assertIn(">Forward</span>", html)
        self.assertIn("fn-logo.svg", html)
        # The {% comment %} block must be stripped — none of its words appear.
        self.assertNotIn("Self-contained", html)
        self.assertNotIn("theme-aware", html)
        self.assertNotIn("currentColor", html)
