# One declaration for the tested NetBox runtime, and a check that enforces it

## Goal

Make it impossible to move the tested-on NetBox version and leave a pin behind.

## Why

The 4.6.8 uplift enumerated thirteen pins by hand and still missed one:

    "exact-runtime-artifact": (re.compile(r"\bNetBox\s+4\.6\.6\b", ...)

`scripts/check_release_authorization.py` writes its copy as an escaped regex, so
a search for `4.6.6` does not find it. The sweep reported itself complete while
leaving behind a check that would have refused correct evidence on the next
release - after the tag existed, when a tag cannot be moved.

That is the specific defect worth engineering against, and it is the same shape
as the product bugs this repository keeps finding: a search that reports
completeness over a surface it cannot see.

## Constraints

- Historical mentions must survive untouched. `min_version` is the declared
  minimum, the FROM side of the upgrade leg must stay there to test the jump,
  `UPGRADE_FROM_NETBOX_OVERRIDES` records what a past release required, and
  migration comments describe what specific versions did. Rewriting any of them
  into the current version makes them false.
- No import rewiring of the release scripts. Several run inside the container
  against an installed artifact and are loaded by tests through explicit file
  locations; changing how they resolve imports risks the release machinery for
  a cosmetic gain, days after two releases.
- A YAML default cannot import a Python constant, so at least one copy is
  unavoidable.

## Touched Surfaces

- `scripts/tested_runtime.py` (new) - the declared value
- `scripts/check_harness.py` - `TESTED_RUNTIME_PIN_FILES`,
  `_check_tested_runtime_pins_agree`

## Approach

The duplication is not removed; the possibility of a copy diverging is. One file
declares `TESTED_NETBOX_VERSION`, and the harness asserts every known pin agrees
with it, reading BOTH the plain literal and the regex-escaped form.

An uplift becomes: change the constant, run the harness, fix exactly what it
names. The enumeration nobody can be trusted to do by hand is done by the tool.

Historical versions are allowed per file, by value, with the reason recorded
beside them - allowed by name rather than invisible to the rule.

## Validation

Proven by staling pins rather than by the check being silent, which is not
evidence:

- Declaring `4.6.9` named all five pin files.
- Staling ONLY the escaped regex to `4.6.7`, leaving every plain literal at
  `4.6.8`, was caught and named - while `grep -c "4\.6\.7"` over that same file
  returned **0**. That is the 4.6.8 miss, reproduced and closed.

## Rollback

Revert. Moving the tested runtime returns to a hand enumeration that has already
been shown to miss a pin invisible to search.

## Decision Log

- **Check the copies rather than eliminate them.** The failure was a pin left
  behind, not the existence of pins. Rewiring imports across the release
  scripts carries real risk - they run in-container and are loaded by file
  location - and would not make the YAML copy go away.
- **Read the escaped form.** It is the entire reason the last sweep failed, and
  a rule that only reads plain literals would have passed that release too.
- **Allow historical versions by value, per file.** They cannot be told from a
  stale pin by pattern alone, so they are declared.

## Open

- A stale pin written as an allowed historical version in the same file would
  still pass - `4.6.6` in `tasks.py`, for instance. Narrow, and much smaller
  than the gap it replaces; distinguishing them needs line context rather than
  value.
- Consolidating the Python copies into real imports remains possible and is now
  optional rather than load-bearing.
