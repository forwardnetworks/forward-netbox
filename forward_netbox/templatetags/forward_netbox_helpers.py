from django import template
from django.apps import apps

register = template.Library()


@register.filter()
def resolve_object(pk, model_path):
    """
    Usage: {{ pk|resolve_object:"app_label.ModelName" }}
    """
    try:
        app_label, model_name = model_path.split(".")
        model = apps.get_model(app_label, model_name)
        return model.objects.get(pk=pk)
    except Exception:
        return None
