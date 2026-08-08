# Name the model every dependency skip is waiting on

## Goal

Stop a skipped row recording only the exception class.

## Contract

- Only schema identifiers are persisted. The dependency is an `app.model`
  string the plugin defines, never a device or interface name.
- A wrong slug is worse than none: it names a model that is not the one holding
  the row up.

## Constraints

- `failure_reason` returns "" for `ForwardDependencySkipError`, so without an
  explicit dependency the persisted `detail` is just the class name.
- `diagnostic_shape` keeps dict KEYS and drops VALUES, so the dependency has to
  travel as an exception attribute rather than inside `context`.
- A new test file that leads with a MODULE DOCSTRING must be added to the
  `reorder-python-imports` exclude in `.pre-commit-config.yaml`. That hook
  strips the blank line after the docstring and black restores it, so the two
  never converge and `invoke lint` fails forever.

## Touched Surfaces

- `forward_netbox/utilities/sync_{cable,device,interface,inventory_module,ipam,routing_impl}.py`
- `forward_netbox/tests/test_skip_raisers_name_dependency.py`
- `.pre-commit-config.yaml`

## Approach

Every unnamed raiser sits under `if runner._dependency_failed("app.model", key)`,
so the slug was already present as a literal one line above. Derive it from that
guard rather than retyping 16 literals, then assert the two agree.

## Validation

- `invoke test-isolated` - full plugin suite, 2010 tests, OK (4 skipped)
- `invoke lint` - rc=0 on two consecutive runs
- A structural test asserts all 24 raisers name a dependency and that each
  matches its guard

## Rollback

Revert. Skipped rows return to recording the exception class alone.

## Decision Log

- **Derived from the guard, not typed by hand.** 16 hand-written slugs is 16
  chances to name the wrong model, and a wrong slug is actively misleading.
  The guard literal is the same fact, already correct.
- **Asserted the agreement rather than trusting the edit.** The structural test
  checks slug against guard, so a future edit that changes one and not the
  other fails here.
- **The count check is deliberate.** `test_there_are_still_raisers_to_check`
  stops the other two silently passing on an empty set, which is how a
  structural check quietly stops testing anything.

## Open

- `invoke lint` was already failing on `main` before this change, from the
  docstring-led test file added with the force-allow fix. Fixed here.
