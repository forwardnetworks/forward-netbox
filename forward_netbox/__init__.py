from netbox.plugins import PluginConfig


class NetboxForwardConfig(PluginConfig):
    name = "forward_netbox"
    verbose_name = "Forward"
    description = "Sync Forward data into NetBox using built-in NQE queries."
    version = "2.6.1"
    base_url = "forward"
    min_version = "4.6.5"
    max_version = "4.6.5"

    def ready(self):
        super().ready()
        from . import signals  # noqa: F401

        _check_runtime_dependencies()


def _check_runtime_dependencies():
    """Fail startup when the exact Branching runtime contract is unavailable."""
    import logging

    from django.core.exceptions import ImproperlyConfigured

    log = logging.getLogger("forward_netbox")
    required = "1.1.1"
    label = "netbox_branching"

    try:
        import netbox_branching  # noqa: F401
    except ImportError as exc:
        raise ImproperlyConfigured(
            "forward_netbox requires netboxlabs-netbox-branching==1.1.1 and "
            "the `netbox_branching` plugin must be enabled."
        ) from exc

    resolved = _resolved_branching_version()
    if resolved != required:
        raise ImproperlyConfigured(
            "forward_netbox requires netboxlabs-netbox-branching==1.1.1; "
            f"found {resolved or 'no package metadata'}."
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
