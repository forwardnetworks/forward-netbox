import json
from functools import lru_cache
from importlib import resources


@lru_cache(maxsize=1)
def get_default_nqe_map() -> dict[str, dict[str, object]]:
    """Return the default NQE query mapping shipped with the plugin."""

    with resources.files("forward_netbox.data").joinpath("nqe_map.json").open(
        "r", encoding="utf-8"
    ) as handle:
        return json.load(handle)
