import importlib.resources
import json

from django.apps import apps
from django.contrib.contenttypes.models import ContentType


def build_nqe_maps(data: list) -> None:
    """
    Create ForwardNQEMap entries from static JSON data.

    Args:
        data (list): A list of dictionaries representing ForwardNQEMap objects.
    """
    ForwardNQEMap = apps.get_model("forward_netbox", "ForwardNQEMap")

    for entry in data:
        # Parse and convert string to actual ContentType
        try:
            app_label, model = entry.pop("netbox_model").split(".")
            content_type = ContentType.objects.get(app_label=app_label, model=model)
            entry["netbox_model"] = content_type
        except (ValueError, ContentType.DoesNotExist) as e:
            raise ValueError(f"Invalid netbox_model value '{entry.get('netbox_model')}'") from e

        ForwardNQEMap.objects.create(**entry)


def get_nqe_map() -> list:
    """
    Load default NQE mappings from embedded data/nqe_map.json.

    Returns:
        list: A list of mapping dictionaries.

    Raises:
        FileNotFoundError: If nqe_map.json is not found.
    """
    for data_file in importlib.resources.files("forward_netbox.data").iterdir():
        if data_file.name == "nqe_map.json":
            with open(data_file, "rb") as f:
                return json.load(f)

    raise FileNotFoundError("'nqe_map.json' not found in forward_netbox.data")
