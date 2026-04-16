__all__ = ("ForwardClient", "BUILTIN_QUERY_SPECS")


def __getattr__(name):
    if name == "ForwardClient":
        from .forward_api import ForwardClient

        return ForwardClient
    if name == "BUILTIN_QUERY_SPECS":
        from .query_registry import BUILTIN_QUERY_SPECS

        return BUILTIN_QUERY_SPECS
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
