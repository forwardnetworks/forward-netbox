#!/usr/bin/env python3
"""Build a Forward NQE JSON data file for NetBox feature tag rules."""

import argparse
import json
from pathlib import Path


DATA_ROW_FIELDS = (
    "record_type",
    "feature",
    "tag",
    "tag_slug",
    "tag_color",
    "enabled",
    "match_source",
    "description",
)

DEFAULT_RULES = [
    {
        "record_type": "structured_feature_tag_rule",
        "feature": "bgp",
        "tag": "Prot_BGP",
        "tag_slug": "prot-bgp",
        "tag_color": "2196f3",
        "enabled": True,
        "match_source": "forward_structured_protocol",
        "description": "Tag devices where Forward exposes structured BGP protocol state.",
    },
]


def build_feature_tag_rules(*, include_defaults=True):
    if not include_defaults:
        return []
    return [dict(rule) for rule in DEFAULT_RULES]


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Build netbox_feature_tag_rules.json for Forward NQE data files."
    )
    parser.add_argument(
        "-o",
        "--output",
        type=Path,
        default=Path("netbox_feature_tag_rules.json"),
        help="Output JSON file path.",
    )
    parser.add_argument(
        "--empty",
        action="store_true",
        help="Write an empty rule list instead of the default structured BGP rule.",
    )
    args = parser.parse_args()

    rows = build_feature_tag_rules(include_defaults=not args.empty)
    args.output.write_text(
        json.dumps(rows, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    print(json.dumps({"output": str(args.output), "rows": len(rows)}, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
