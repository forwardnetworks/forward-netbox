import json

from django.contrib.contenttypes.models import ContentType
from django.core import serializers

from forward_netbox.models import ForwardRelationshipField
from forward_netbox.models import ForwardTransformField
from forward_netbox.models import ForwardTransformMap


def write_json_to_file(data, filename):
    with open(filename, "w") as file:
        file.write(data)


new_data = []


data = json.loads(serializers.serialize("json", ForwardTransformMap.objects.all()))
for d in data:
    ct = ContentType.objects.get(pk=d["fields"]["target_model"])
    d["fields"]["target_model"] = {"app_label": ct.app_label, "model": ct.model}
    raw = {
        "data": d["fields"],
        "field_maps": [],
        "relationship_maps": [],
    }

    for fm in json.loads(
        serializers.serialize(
            "json", ForwardTransformField.objects.filter(transform_map=d["pk"])
        )
    ):
        fields = fm["fields"]
        fields.pop("transform_map")
        raw["field_maps"].append(fields)

    for rm in json.loads(
        serializers.serialize(
            "json", ForwardRelationshipField.objects.filter(transform_map=d["pk"])
        )
    ):
        ct = ContentType.objects.get(pk=rm["fields"]["source_model"])
        fields = rm["fields"]
        fields["source_model"] = {"app_label": ct.app_label, "model": ct.model}
        fields.pop("transform_map")
        raw["relationship_maps"].append(fields)
    new_data.append(raw)

write_json_to_file(
    json.dumps(new_data),
    "transform_map.json",
)
