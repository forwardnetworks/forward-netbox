from django.urls import include
from django.urls import path
from utilities.urls import get_model_urls

from . import views  # noqa: F401


urlpatterns = (
    path(
        "source/",
        include(get_model_urls("forward_netbox", "forwardsource", detail=False)),
    ),
    path(
        "source/<int:pk>/",
        include(get_model_urls("forward_netbox", "forwardsource")),
    ),
    path(
        "sync/",
        include(get_model_urls("forward_netbox", "forwardsync", detail=False)),
    ),
    path(
        "sync/<int:pk>/",
        include(get_model_urls("forward_netbox", "forwardsync")),
    ),
    path(
        "ingestion/",
        include(get_model_urls("forward_netbox", "forwardingestion", detail=False)),
    ),
    path(
        "ingestion/<int:pk>/",
        include(get_model_urls("forward_netbox", "forwardingestion")),
    ),
    path(
        "nqe-map/",
        include(get_model_urls("forward_netbox", "forwardnqemap", detail=False)),
    ),
    path(
        "nqe-map/<int:pk>/",
        include(get_model_urls("forward_netbox", "forwardnqemap")),
    ),
)
