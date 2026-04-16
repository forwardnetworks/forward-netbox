# Contributing

This repository now contains the `forward_netbox` plugin. Keep changes aligned with the current Forward-only architecture:

- package name: `forward_netbox`
- user-facing brand: `Forward Networks`
- direct Forward API + built-in NQE queries
- keep the plugin focused on direct Forward inventory syncs into NetBox
- run `pre-commit install --hook-type commit-msg` so commit messages are checked for sensitive identifiers
- use `.sensitive-patterns.local.txt` for local-only customer names or tenant labels that must never be committed
- plain lines in `.sensitive-patterns.local.txt` are treated as case-insensitive literals; prefix a line with `re:` for a regex
