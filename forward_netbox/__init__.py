from netbox.plugins import PluginConfig


class NetboxForwardConfig(PluginConfig):
    name = "forward_netbox"
    verbose_name = "Forward"
    description = "Sync Forward data into NetBox using built-in NQE queries."
    version = "2.8.6"
    base_url = "forward"
    min_version = "4.6.5"
    max_version = "4.6.99"

    def ready(self):
        super().ready()
        from . import signals  # noqa: F401

        _check_runtime_dependencies()


def _check_runtime_dependencies():
    """Fail startup when the exact Branching runtime contract is unavailable."""
    import logging

    from django.core.exceptions import ImproperlyConfigured

    from .utilities.version_series import series_matches

    log = logging.getLogger("forward_netbox")
    # A release series, not an exact version. This was `== "1.1.1"` and raised
    # ImproperlyConfigured, so a Branching patch upgrade stopped the plugin
    # loading at all — the same hard block `max_version` imposed for NetBox
    # patches. Behaviour we actually depend on is checked per engine against the
    # live runtime, not inferred from a version string.
    required_series = "1.1"
    label = "netbox_branching"

    try:
        import netbox_branching  # noqa: F401
    except ImportError as exc:
        raise ImproperlyConfigured(
            "forward_netbox requires netboxlabs-netbox-branching "
            f"{required_series}.x and the `netbox_branching` plugin must be "
            "enabled."
        ) from exc

    resolved = _resolved_branching_version()
    if not series_matches(resolved, required_series):
        raise ImproperlyConfigured(
            "forward_netbox requires netboxlabs-netbox-branching "
            f"{required_series}.x; found {resolved or 'no package metadata'}."
        )
    log.info("forward_netbox runtime dependency %s==%s", label, resolved)


def _resolved_branching_version():
    """Return the supported distribution version, or ``None`` when absent."""
    from importlib.metadata import PackageNotFoundError
    from importlib.metadata import version

    try:
        return version("netboxlabs-netbox-branching")
    except PackageNotFoundError:
        return None


config = NetboxForwardConfig
