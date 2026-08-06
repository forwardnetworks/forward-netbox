# A dotted scope tag name (`A.Person`) normalizes to `aperson`, because
# `slugify` drops the dot rather than replacing it. A tag created through the
# NetBox UI for the same name gets `a-person`. When both that tag and any
# unrelated tag holding `aperson` existed, scope tag resolution refused
# outright, which left the ownership domain Incomplete, blocked convergence,
# and reported every drift figure as "Not measured".
from django.test import TestCase
from extras.models import Tag

from forward_netbox.models import ForwardManagedDeviceTag
from forward_netbox.utilities.ownership import OwnershipConflictError
from forward_netbox.utilities.ownership import _locked_scope_tag
from forward_netbox.utilities.tag_contracts import candidate_managed_tag_slugs
from forward_netbox.utilities.tag_contracts import normalized_managed_tag_slug


class CandidateSlugTest(TestCase):
    def test_a_dotted_name_offers_both_conventions(self):
        self.assertEqual(
            candidate_managed_tag_slugs("A.Person"),
            {"aperson", "a-person"},
        )

    def test_the_creation_slug_is_still_exactly_one(self):
        self.assertEqual(normalized_managed_tag_slug("A.Person"), "aperson")

    def test_a_plain_name_offers_one(self):
        self.assertEqual(candidate_managed_tag_slugs("Prod Core"), {"prod-core"})


class LockedScopeTagTest(TestCase):
    """`_locked_scope_tag` runs inside the ownership transaction."""

    def test_a_tag_stored_under_the_dashed_slug_is_found_by_name(self):
        tag = Tag.objects.create(name="A.Person", slug="a-person")
        self.assertEqual(
            _locked_scope_tag("A.Person", normalized_managed_tag_slug("A.Person")),
            tag,
        )

    def test_an_unmanaged_slug_collision_resolves_to_the_named_tag(self):
        named = Tag.objects.create(name="A.Person", slug="a-person")
        Tag.objects.create(name="Unrelated", slug="aperson")
        # The operator configured the tag by name, so the name is the stronger
        # evidence. The colliding row belongs to nobody and must not veto it.
        self.assertEqual(
            _locked_scope_tag("A.Person", normalized_managed_tag_slug("A.Person")),
            named,
        )

    def test_a_named_tag_carrying_a_candidate_slug_is_never_ambiguous(self):
        # The named tag satisfies both lookups by itself, so other rows holding
        # the sibling slug cannot make it ambiguous.
        named = Tag.objects.create(name="A.Person", slug="a-person")
        Tag.objects.create(name="Unrelated", slug="aperson")
        self.assertEqual(
            _locked_scope_tag("A.Person", normalized_managed_tag_slug("A.Person")),
            named,
        )

    def test_a_managed_slug_collision_still_refuses(self):
        # A genuine collision needs the named tag to carry NEITHER candidate
        # slug - NetBox lets an operator set any slug they like.
        named = Tag.objects.create(name="A.Person", slug="person-a-custom")
        colliding = Tag.objects.create(name="Unrelated", slug="aperson")
        ForwardManagedDeviceTag.objects.create(tag=colliding, claim_type="scope")
        with self.assertRaises(OwnershipConflictError) as caught:
            _locked_scope_tag("A.Person", normalized_managed_tag_slug("A.Person"))
        message = str(caught.exception)
        # Switching away from a managed row would strand its claims, so this
        # one is genuinely ambiguous and must stay a refusal.
        self.assertIn(str(colliding.pk), message)
        self.assertIn(str(named.pk), message)
        # Tag names are customer data here - they are people.
        self.assertNotIn("A.Person", message)

    def test_an_unmanaged_collision_on_a_custom_slug_resolves_to_the_name(self):
        named = Tag.objects.create(name="A.Person", slug="person-a-custom")
        Tag.objects.create(name="Unrelated", slug="aperson")
        self.assertEqual(
            _locked_scope_tag("A.Person", normalized_managed_tag_slug("A.Person")),
            named,
        )

    def test_no_match_returns_none_so_the_caller_creates_it(self):
        self.assertIsNone(
            _locked_scope_tag("A.Person", normalized_managed_tag_slug("A.Person"))
        )
