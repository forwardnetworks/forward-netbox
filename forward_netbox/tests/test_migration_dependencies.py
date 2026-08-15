# A plugin migration that depends on a specific NetBox core migration pins the
# plugin to the NetBox version that introduced it - and does so silently, since
# nothing fails until someone installs on an older supported release.
#
# `0052_device_absence_quarantine` shipped in 2.8.0 depending on
# `dcim.0241_nullify_empty_cable_end`, a NetBox 4.6.6 migration, while the
# plugin declares `min_version = "4.6.5"`. On 4.6.5 the migration graph could not
# be built at all:
#
#     NodeNotFoundError: Migration forward_netbox.0052_device_absence_quarantine
#     dependencies reference nonexistent parent node
#     ('dcim', '0241_nullify_empty_cable_end')
#
# The release gate did not catch it in 2.8.0 because its upgrade leg seeded the
# previous version - which had no 0052 - on 4.6.5, and only ever applied 0052 on
# 4.6.6. The 2.8.1 leg was the first to install a plugin carrying 0052 onto the
# older supported NetBox, and it failed immediately.
#
# `makemigrations` writes whichever core migration is newest on the machine that
# ran it, so this recurs by default rather than by mistake. This test is the
# thing that makes it fail loudly at author time instead of at a customer's
# upgrade.
import re
from pathlib import Path

from django.test import SimpleTestCase

MIGRATIONS_DIR = Path(__file__).resolve().parent.parent / "migrations"

# dcim's initial migration exists in every NetBox this plugin supports and is
# ordered before everything else, so it is the default anchor. Depending on
# `dcim.Device` existing is all most of these migrations actually need.
#
# The distinction that matters is WHY a migration names a later one:
#
#   legitimate - it references a model that migration creates, so the dependency
#                is real and removing it would be wrong
#   accidental - `makemigrations` wrote whichever core migration happened to be
#                newest on the author's machine
#
# Only the first belongs here, and each entry says which model it needs.
ALLOWED_DCIM_DEPENDENCIES = {
    "0001_initial",
    # 0034 creates ForeignKeys to `dcim.virtualdevicecontext`, so it needs the
    # migration that creates that model. Predates every supported NetBox.
    "0166_virtualdevicecontext",
}

_DCIM_DEPENDENCY = re.compile(r"""\(\s*["']dcim["']\s*,\s*["']([^"']+)["']\s*\)""")


class MigrationsDoNotPinANetBoxVersionTest(SimpleTestCase):
    def test_no_migration_depends_on_a_version_specific_dcim_migration(self):
        offenders = []
        for path in sorted(MIGRATIONS_DIR.glob("0*.py")):
            for name in _DCIM_DEPENDENCY.findall(path.read_text(encoding="utf-8")):
                if name not in ALLOWED_DCIM_DEPENDENCIES:
                    offenders.append(f"{path.name} -> dcim.{name}")

        self.assertEqual(
            offenders,
            [],
            "These migrations depend on a specific dcim migration, which pins "
            "the plugin to whichever NetBox version introduced it and breaks "
            "installs on older supported releases. Depend on dcim.0001_initial "
            "instead - the model only has to exist.\n  " + "\n  ".join(offenders),
        )

    def test_the_guard_would_catch_the_regression_it_was_written_for(self):
        # Pinning the assertion's own mechanism: a test that silently matched
        # nothing would pass forever and protect nothing.
        sample = 'dependencies = [("dcim", "0241_nullify_empty_cable_end")]'

        found = _DCIM_DEPENDENCY.findall(sample)

        self.assertEqual(found, ["0241_nullify_empty_cable_end"])
        self.assertNotIn(found[0], ALLOWED_DCIM_DEPENDENCIES)
