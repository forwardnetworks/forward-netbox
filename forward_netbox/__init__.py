try:
    from netbox.plugins import PluginConfig
except ModuleNotFoundError:  # pragma: no cover - tooling imports outside NetBox
    PluginConfig = object


class NetboxForwardConfig(PluginConfig):
    name = "forward_netbox"
    verbose_name = "Forward Field Integration"
    description = "Sync Forward data into NetBox using built-in NQE queries."
    version = "2.0.5"
    base_url = "forward"
    min_version = "4.6.3"

    def ready(self):
        super().ready()
        from . import signals  # noqa: F401

        _check_runtime_dependencies()


def _check_runtime_dependencies():
    """Warn at startup about a missing/old ``netbox_branching`` dependency.

    The single-branch ingest path hard-depends on netbox-branching 1.1.0
    internals (SquashMergeStrategy collapse/cycle-split/FK-graph, the
    squash_dependency_graph_built signal, branch.connection_name, the
    ObjectChange read router). A missing or pre-1.1.0 install otherwise only
    surfaces mid-sync as an opaque failure. This logs (never raises) so NetBox
    startup is unaffected.

    "Installed and enabled" is tested by IMPORTABILITY, not the distribution
    name: NetBox only loads the module when it is listed in PLUGINS, and the
    PyPI distribution is ``netboxlabs-netbox-branching`` (not the import name
    ``netbox_branching``), so keying the check off a single dist name produced a
    false "not installed" warning on every boot even when it was active.
    """
    import logging

    log = logging.getLogger("forward_netbox")
    floor = (1, 1, 0)
    label = "netbox_branching"

    try:
        import netbox_branching  # noqa: F401
    except Exception:
        log.warning(
            "forward_netbox requires the %s plugin for branch-staged syncs, but "
            "it is not installed/enabled; syncs will fail. Install "
            "netboxlabs-netbox-branching (>=1.1.0), add 'netbox_branching' to "
            "PLUGINS in configuration.py, and run migrations.",
            label,
        )
        return

    resolved = _resolved_branching_version()
    if resolved is None:
        # Installed/importable but the version could not be resolved (unusual
        # packaging). Do not cry wolf — the import succeeded, so syncs can run.
        log.info("forward_netbox runtime dependency %s installed", label)
        return

    log.info("forward_netbox runtime dependency %s==%s", label, resolved)
    if _version_tuple(resolved) < floor:
        log.warning(
            "forward_netbox requires %s>=%s but found %s; syncs will fail (the "
            "plugin reuses netbox-branching 1.1.0 internals). Upgrade "
            "netboxlabs-netbox-branching.",
            label,
            ".".join(str(part) for part in floor),
            resolved,
        )


def _resolved_branching_version():
    """Best-effort netbox-branching version, robust to the distribution name.

    The PyPI distribution is ``netboxlabs-netbox-branching``; older/forks may use
    ``netbox-branching``. Fall back to the module's ``__version__``.
    """
    from importlib.metadata import PackageNotFoundError
    from importlib.metadata import version

    for dist in ("netboxlabs-netbox-branching", "netbox-branching"):
        try:
            return version(dist)
        except PackageNotFoundError:
            continue
        except Exception:  # pragma: no cover - never block startup
            return None
    try:
        import netbox_branching

        return getattr(netbox_branching, "__version__", None)
    except Exception:  # pragma: no cover - never block startup
        return None


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
