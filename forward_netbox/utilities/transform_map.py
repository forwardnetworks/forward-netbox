import importlib.resources
import json

from django.apps import apps as django_apps

# These functions are used in the migration file to prepare the transform maps
# Because of this we have to use historical models
# see https://docs.djangoproject.com/en/5.1/topics/migrations/#historical-models


def build_fields(data, apps, db_alias):
    ContentType = apps.get_model("contenttypes", "ContentType")
    if "target_model" in data:
        ct = ContentType.objects.db_manager(db_alias).get_for_model(
            apps.get_model(
                data["target_model"]["app_label"],
                data["target_model"]["model"],
            )
        )
        data["target_model"] = ct
    elif "source_model" in data:
        ct = ContentType.objects.db_manager(db_alias).get_for_model(
            apps.get_model(
                data["source_model"]["app_label"],
                data["source_model"]["model"],
            )
        )
        data["source_model"] = ct
    return data


def build_transform_maps(data, apps: django_apps = None, db_alias: str = "default"):
    apps = apps or django_apps
    ForwardTransformMap = apps.get_model("forward_netbox", "ForwardTransformMap")
    ForwardTransformField = apps.get_model("forward_netbox", "ForwardTransformField")
    ForwardRelationshipField = apps.get_model(
        "forward_netbox", "ForwardRelationshipField"
    )
    for tm in data:
        field_data = build_fields(tm["data"], apps, db_alias)
        tm_obj = ForwardTransformMap.objects.using(db_alias).create(**field_data)
        for fm in tm["field_maps"]:
            field_data = build_fields(fm, apps, db_alias)
            ForwardTransformField.objects.using(db_alias).create(
                transform_map=tm_obj, **field_data
            )
        for rm in tm["relationship_maps"]:
            relationship_data = build_fields(rm, apps, db_alias)
            ForwardRelationshipField.objects.using(db_alias).create(
                transform_map=tm_obj, **relationship_data
            )


def get_transform_map() -> dict:
    for data_file in importlib.resources.files("forward_netbox.data").iterdir():
        if data_file.name != "transform_map.json":
            continue
        with open(data_file, "rb") as data_file:
            return json.load(data_file)
    raise FileNotFoundError("'transform_map.json' not found in installed package")
