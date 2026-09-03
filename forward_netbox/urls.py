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
    # Both includes are required. Registering the view is not enough: without
    # the `detail=False` include there is no URL for the list route to attach
    # to, so the object view's breadcrumb reverse still raises NoReverseMatch
    # and opening an issue is a 500.
    path(
        "ingestion-issue/",
        include(
            get_model_urls("forward_netbox", "forwardingestionissue", detail=False)
        ),
    ),
    path(
        "ingestion-issue/<int:pk>/",
        include(get_model_urls("forward_netbox", "forwardingestionissue")),
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
        "change/",
        include(get_model_urls("forward_netbox", "forwardchange", detail=False)),
    ),
    path(
        "change/<int:pk>/",
        include(get_model_urls("forward_netbox", "forwardchange")),
    ),
    path(
        "change-policy/",
        include(get_model_urls("forward_netbox", "forwardchangepolicy", detail=False)),
    ),
    path(
        "change-policy/<int:pk>/",
        include(get_model_urls("forward_netbox", "forwardchangepolicy")),
    ),
    path(
        "change-policy-rule/",
        include(
            get_model_urls("forward_netbox", "forwardchangepolicyrule", detail=False)
        ),
    ),
    path(
        "change-policy-rule/<int:pk>/",
        include(get_model_urls("forward_netbox", "forwardchangepolicyrule")),
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
