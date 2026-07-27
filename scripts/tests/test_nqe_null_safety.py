from __future__ import annotations

import re
import unittest
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]
QUERY_DIR = REPO_ROOT / "forward_netbox" / "queries"

# These NQE built-ins reject null at runtime. Helper calls are added dynamically
# when their declarations contain one or more non-optional String parameters.
NULL_INTOLERANT_BUILTINS = {
    "length",
    "matches",
    "regexMatches",
    "replace",
    "replaceRegexMatches",
    "substring",
    "toLowerCase",
    "toUpperCase",
}
MEMBER_EXPRESSION_RE = re.compile(
    r"\b[A-Za-z_][A-Za-z0-9_]*(?:\.[A-Za-z_][A-Za-z0-9_]*)+\b"
)
FUNCTION_DECLARATION_RE = re.compile(
    r"(?m)^\s*(?:export\s+)?([A-Za-z_][A-Za-z0-9_]*)\s*\(([^)]*)\)\s*="
)


def _query_paths() -> list[Path]:
    return sorted(QUERY_DIR.glob("*.nqe"))


def _possibly_null_members(sources: dict[Path, str]) -> set[str]:
    members: set[str] = set()
    for source in sources.values():
        for match in re.finditer(r"\bisPresent\s*\(([^)]+)\)", source):
            members.update(MEMBER_EXPRESSION_RE.findall(match.group(1)))
        for match in re.finditer(
            r"\b([A-Za-z_][A-Za-z0-9_]*(?:\.[A-Za-z_][A-Za-z0-9_]*)+)\?\.",
            source,
        ):
            members.add(match.group(1))
    return members


def _string_helper_names(sources: dict[Path, str]) -> set[str]:
    helpers = set()
    for source in sources.values():
        for match in FUNCTION_DECLARATION_RE.finditer(source):
            parameters = match.group(2)
            if re.search(r":\s*String\b", parameters):
                helpers.add(match.group(1))
    return helpers


def _balanced_call_end(source: str, open_paren: int) -> int | None:
    depth = 0
    quote = ""
    escaped = False
    for index in range(open_paren, len(source)):
        character = source[index]
        if quote:
            if escaped:
                escaped = False
            elif character == "\\":
                escaped = True
            elif character == quote:
                quote = ""
            continue
        if character in {'"', "'", "`"}:
            quote = character
        elif character == "(":
            depth += 1
        elif character == ")":
            depth -= 1
            if depth == 0:
                return index
    return None


def _call_sites(source: str, function_names: set[str]):
    names = "|".join(sorted(map(re.escape, function_names), key=len, reverse=True))
    pattern = re.compile(rf"\b(?P<name>{names})\s*\(")
    for match in pattern.finditer(source):
        open_paren = source.find("(", match.start())
        close_paren = _balanced_call_end(source, open_paren)
        if close_paren is not None:
            yield (
                match.group("name"),
                match.start(),
                source[open_paren + 1 : close_paren],
            )


def _binding_scope_start(source: str, call_start: int, member: str) -> int:
    root_name = member.split(".", 1)[0]
    binding = re.compile(rf"\bforeach\s+{re.escape(root_name)}\s+in\b")
    starts = [match.start() for match in binding.finditer(source, 0, call_start)]
    return starts[-1] if starts else max(source.rfind(";", 0, call_start), 0)


def _guarded_before_call(source: str, call_start: int, member: str) -> bool:
    context = source[_binding_scope_start(source, call_start, member) : call_start]
    guard = rf"\bisPresent\s*\(\s*{re.escape(member)}\s*\)"
    if re.search(rf"(?m)^\s*where\s+{guard}", context):
        return True
    # An explicit non-null coalesce in the call expression is also safe. The
    # caller passes the member only on the guarded branch and a literal/default
    # on the other branch.
    statement_start = max(
        source.rfind(";", 0, call_start),
        source.rfind("\nlet ", 0, call_start),
        0,
    )
    return bool(re.search(guard, source[statement_start:call_start]))


def _proven_required_by_source(source: str, call_start: int, member: str) -> bool:
    root_name, field_name = member.split(".", 1)[0], member.rsplit(".", 1)[-1]
    prefix = source[:call_start]
    # Named regex captures using a required token such as ``\S+`` always
    # materialize that field before the capture row reaches a helper.
    if re.search(rf"\?<\s*{re.escape(field_name)}(?:\s*:[^>]*)?>", prefix):
        return True
    # Forward command-output response is required for a selected command type.
    # The source must narrow that exact command binding before parsing it.
    return bool(
        field_name == "response"
        and re.search(rf"(?m)^\s*where\s+{re.escape(root_name)}\.commandType\b", prefix)
    )


def null_unsafe_calls(
    source: str, sources: dict[Path, str]
) -> list[tuple[int, str, str]]:
    nullable_members = _possibly_null_members(sources)
    functions = NULL_INTOLERANT_BUILTINS | _string_helper_names(sources)
    failures = []
    for function_name, call_start, arguments in _call_sites(source, functions):
        for member in sorted(nullable_members):
            if not re.search(rf"\b{re.escape(member)}\b", arguments):
                continue
            guard = rf"\bisPresent\s*\(\s*{re.escape(member)}\s*\)"
            if re.search(guard, arguments):
                continue
            if _guarded_before_call(source, call_start, member):
                continue
            if _proven_required_by_source(source, call_start, member):
                continue
            line = source.count("\n", 0, call_start) + 1
            failures.append((line, function_name, member))
    return failures


class NQENullSafetyTest(unittest.TestCase):
    def test_all_bundled_queries_guard_possibly_null_helper_arguments(self):
        paths = _query_paths()
        self.assertGreaterEqual(
            len(paths),
            50,
            "The null-safety audit must cover the complete bundled NQE directory.",
        )
        sources = {path: path.read_text(encoding="utf-8") for path in paths}
        failures = []
        for path, source in sources.items():
            for line, function_name, member in null_unsafe_calls(source, sources):
                failures.append(
                    f"{path.relative_to(REPO_ROOT)}:{line}: "
                    f"{function_name} receives possibly-null {member}"
                )
        self.assertEqual(
            failures,
            [],
            "Possibly-null NQE members must be defaulted or dominated by an "
            "isPresent guard before entering a null-intolerant built-in/helper.",
        )

    def test_checker_rejects_the_platform_regression_shape(self):
        unsafe = """
@query
f() =
foreach device in network.devices
let seen_elsewhere = if isPresent(device.platform.osVersion) then "yes" else "no"
let platform = normalizePlatformName(
  toString(device.platform.os),
  device.platform.osVersion
)
select {platform: platform}
"""
        helper = """
normalizePlatformName(platform_os: String, platform_os_version: String) =
  if matches(platform_os_version, "14.*") then "ACI" else platform_os;
"""
        sources = {
            Path("unsafe.nqe"): unsafe,
            Path("helper.nqe"): helper,
        }

        self.assertEqual(
            null_unsafe_calls(unsafe, sources),
            [(6, "normalizePlatformName", "device.platform.osVersion")],
        )

    def test_checker_accepts_default_and_dominating_guard(self):
        guarded = """
@query
f() =
foreach device in network.devices
where isPresent(device.platform.osVersion)
let platform = normalizePlatformName(
  toString(device.platform.os),
  device.platform.osVersion
)
select {platform: platform}
"""
        helper = """
normalizePlatformName(platform_os: String, platform_os_version: String) =
  if matches(platform_os_version, "14.*") then "ACI" else platform_os;
"""
        sources = {
            Path("guarded.nqe"): guarded,
            Path("helper.nqe"): helper,
        }

        self.assertEqual(null_unsafe_calls(guarded, sources), [])


if __name__ == "__main__":
    unittest.main()
