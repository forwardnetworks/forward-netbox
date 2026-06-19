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
        "execution-run/",
        include(get_model_urls("forward_netbox", "forwardexecutionrun", detail=False)),
    ),
    path(
        "execution-run/<int:pk>/",
        include(get_model_urls("forward_netbox", "forwardexecutionrun")),
    ),
    path(
        "execution-step/",
        include(get_model_urls("forward_netbox", "forwardexecutionstep", detail=False)),
    ),
    path(
        "execution-step/<int:pk>/",
        include(get_model_urls("forward_netbox", "forwardexecutionstep")),
    ),
    path(
        "nqe-map/",
        include(get_model_urls("forward_netbox", "forwardnqemap", detail=False)),
    ),
    path(
        "nqe-map/<int:pk>/",
        include(get_model_urls("forward_netbox", "forwardnqemap")),
    ),
    path(
        "drift-policy/",
        include(get_model_urls("forward_netbox", "forwarddriftpolicy", detail=False)),
    ),
    path(
        "drift-policy/<int:pk>/",
        include(get_model_urls("forward_netbox", "forwarddriftpolicy")),
    ),
    path(
        "device-analysis/",
        include(
            get_model_urls("forward_netbox", "forwarddeviceanalysis", detail=False)
        ),
    ),
    path(
        "device-analysis/<int:pk>/",
        include(get_model_urls("forward_netbox", "forwarddeviceanalysis")),
    ),
    path(
        "validation-run/",
        include(get_model_urls("forward_netbox", "forwardvalidationrun", detail=False)),
    ),
    path(
        "validation-run/<int:pk>/",
        include(get_model_urls("forward_netbox", "forwardvalidationrun")),
    ),
)
