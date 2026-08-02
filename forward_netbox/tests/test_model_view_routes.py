# Registering a detail view without its list view is a 500, not a missing page:
# NetBox reverses `<app>:<model>_list` from the object view, so opening the
# object raises NoReverseMatch. A customer hit exactly that on
# `forwardingestionissue` - the one page that exists to explain a failed merge -
# and got a Server Error.
#
# This has now happened twice. In 2.6.3 the Ingestions list raised
# NoReverseMatch for `forwardingestion_delete`, and it was resolved by removing
# the action that pointed at the missing view, which left ingestions undeletable
# for three releases.
#
# The installed-route probe does not cover this: it renders the plugin's *menu*
# lists, and a model reachable only from its parent has no menu entry. So the
# guard belongs here, over every registered model rather than every menu item.

from django.apps import apps
from django.test import TestCase
from django.urls import NoReverseMatch
from django.urls import reverse


class EveryModelWithADetailViewCanReverseItsListTest(TestCase):
    """The reverse the object view performs, for every model we register."""

    def test_list_route_resolves_for_each_plugin_model(self):
        missing = []
        for model in apps.get_app_config("forward_netbox").get_models():
            name = model._meta.model_name
            # Only models that actually expose a detail route can trigger the
            # breadcrumb reverse; a model with neither is not user-reachable.
            try:
                reverse(f"plugins:forward_netbox:{name}", kwargs={"pk": 1})
            except NoReverseMatch:
                continue
            try:
                reverse(f"plugins:forward_netbox:{name}_list")
            except NoReverseMatch:
                missing.append(name)
        self.assertEqual(
            missing,
            [],
            "these models expose a detail view whose breadcrumb reverse has no "
            f"list route, which renders as a 500: {missing}",
        )

    def test_the_ingestion_issue_list_route_exists(self):
        # Pinned by name as well as by the sweep above, because this is the one
        # a customer reported and the sweep would also pass if the model stopped
        # exposing a detail view entirely.
        self.assertTrue(reverse("plugins:forward_netbox:forwardingestionissue_list"))
