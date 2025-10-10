import json

from forward_netbox.models import ForwardTransformMap
from forward_netbox.utilities.transform_map import build_transform_maps


with open("transform_map.json", "r") as file:
    data = file.read()
ForwardTransformMap.objects.all().delete()
build_transform_maps(json.loads(data))
