import json

from ipfabric_netbox.models import IPFabricTransformMap
from ipfabric_netbox.utilities.transform_map import build_transform_maps


with open("transform_map.json", "r") as file:
    data = file.read()
IPFabricTransformMap.objects.all().delete()
build_transform_maps(json.loads(data))
