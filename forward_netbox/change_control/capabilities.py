# What this Forward organisation can do, found by asking.
#
# Read-only, through the SDK: listing change sets answers "may this organisation
# use Predict?" and listing AI chats answers "is Forward AI on?", each with the
# refusal's reason when not. No licence table is consulted, because the tier is
# not readable from the API and a wrong table sends an operator to buy the wrong
# thing. A probe that cannot tell says so instead of guessing.
from django.core.cache import cache

CACHE_SECONDS = 600

UNKNOWN = None


def _probe(call):
    """``{"available": True|False|None, "reason": str}`` for one read-only call."""
    try:
        call()
        return {"available": True, "reason": ""}
    except Exception as exc:  # noqa: BLE001 - classified below, never raised
        cause = getattr(exc, "__cause__", None) or exc
        denial = getattr(cause, "denial", None)
        if denial is not None:
            detail = f": {denial.detail}" if getattr(denial, "detail", "") else ""
            return {"available": False, "reason": f"{denial.kind}{detail}"}
        return {
            "available": UNKNOWN,
            "reason": f"could not tell ({type(exc).__name__})",
        }


def _cache_key(source):
    # The network is part of the key: pointing the source at another network (or
    # another account) must not show the old answer.
    network_id = (source.parameters or {}).get("network_id") or ""
    return f"forward_netbox:capabilities:{source.pk}:{network_id}"


def probe_capabilities(source, *, api=None, use_cache=True):
    """Predict and Forward AI availability for a source's network."""
    from .workspace import ForwardWorkspaceApi

    key = _cache_key(source)
    if use_cache and source.pk:
        cached = cache.get(key)
        if cached:
            return cached
    network_id = (source.parameters or {}).get("network_id") or ""
    try:
        api = api or ForwardWorkspaceApi(source)
        result = api.capabilities(network_id)
    except Exception as exc:  # noqa: BLE001
        reason = f"could not tell ({type(exc).__name__})"
        result = {
            "predict": {"available": UNKNOWN, "reason": reason},
            "ai": {"available": UNKNOWN, "reason": reason},
        }
    if source.pk:
        cache.set(key, result, CACHE_SECONDS)
    return result


def cached_capabilities(source):
    """What the last probe found, or None if nobody has asked recently."""
    return cache.get(_cache_key(source)) if source.pk else None
