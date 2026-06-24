try:
    from netbox.plugins import PluginConfig
except ModuleNotFoundError:  # pragma: no cover - tooling imports outside NetBox
    PluginConfig = object


class NetboxForwardConfig(PluginConfig):
    name = "forward_netbox"
    verbose_name = "NetBox Forward Plugin"
    description = "Sync Forward data into NetBox using built-in NQE queries."
    version = "2.0.0"
    base_url = "forward"
    min_version = "4.6.3"

    def ready(self):
        super().ready()
        from . import signals  # noqa: F401

        _check_runtime_dependencies()


def _check_runtime_dependencies():
    """Warn at startup about missing/dependency-version drift.

    The resumable multi-branch executor hard-depends on the ``netbox_branching``
    plugin; capability detection elsewhere is presence-only, so a missing or
    surprising dependency version otherwise only surfaces mid-sync as an opaque
    failure. This logs (never raises) so NetBox startup is unaffected, and it
    records the resolved versions for support bundles.
    """
    import logging
    from importlib.metadata import PackageNotFoundError
    from importlib.metadata import version

    log = logging.getLogger("forward_netbox")
    # The single-branch ingest path reuses netbox-branching 1.1.0 internals
    # (SquashMergeStrategy collapse/cycle-split/FK-graph, the
    # squash_dependency_graph_built signal, branch.connection_name, the ObjectChange
    # read router). A pre-1.1.0 install would fail opaquely mid-merge, so assert the
    # floor at startup (log-only; never blocks NetBox boot).
    required = {"netbox-branching": (1, 1, 0)}
    for dist, label in (("netbox-branching", "netbox_branching"),):
        try:
            resolved = version(dist)
            log.info("forward_netbox runtime dependency %s==%s", label, resolved)
            floor = required.get(dist)
            if floor and _version_tuple(resolved) < floor:
                log.warning(
                    "forward_netbox requires %s>=%s but found %s; syncs will fail "
                    "(the plugin reuses netbox-branching 1.1.0 internals). Upgrade "
                    "netbox-branching.",
                    label,
                    ".".join(str(part) for part in floor),
                    resolved,
                )
        except PackageNotFoundError:
            log.warning(
                "forward_netbox requires the %s plugin for branch-staged syncs, "
                "but it is not installed; syncs will fail. Install and enable "
                "netbox_branching.",
                label,
            )
        except Exception:  # pragma: no cover - never block startup
            pass


def _version_tuple(value):
    """Best-effort (major, minor, patch) tuple from a version string."""
    parts = []
    for chunk in str(value).split(".")[:3]:
        digits = ""
        for char in chunk:
            if char.isdigit():
                digits += char
            else:
                break
        parts.append(int(digits) if digits else 0)
    while len(parts) < 3:
        parts.append(0)
    return tuple(parts)


config = NetboxForwardConfig
