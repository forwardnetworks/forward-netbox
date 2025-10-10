from django.urls import include
from django.urls import path
from utilities.urls import get_model_urls

from . import views

urlpatterns = (
    # Source
    path("source/", views.ForwardSourceListView.as_view(), name="forwardsource_list"),
    path(
        "source/add/", views.ForwardSourceEditView.as_view(), name="forwardsource_add"
    ),
    path(
        "source/delete/",
        views.ForwardSourceBulkDeleteView.as_view(),
        name="forwardsource_bulk_delete",
    ),
    path(
        "source/<int:pk>/", include(get_model_urls("forward_netbox", "forwardsource"))
    ),
    path(
        "source/<int:pk>/delete/",
        views.ForwardSourceDeleteView.as_view(),
        name="forwardsource_delete",
    ),
    # Snapshot
    path(
        "snapshot/",
        views.ForwardSnapshotListView.as_view(),
        name="forwardsnapshot_list",
    ),
    path(
        "snapshot/delete/",
        views.ForwardSnapshotBulkDeleteView.as_view(),
        name="forwardsnapshot_bulk_delete",
    ),
    path(
        "snapshot/<int:pk>/",
        include(get_model_urls("forward_netbox", "forwardsnapshot")),
    ),
    path(
        "snapshot/<int:pk>/delete/",
        views.ForwardSnapshotDeleteView.as_view(),
        name="forwardsnapshot_delete",
    ),
    # Snapshot Data
    path(
        "data/delete",
        views.ForwardSnapshotDataBulkDeleteView.as_view(),
        name="forwarddata_bulk_delete",
    ),
    path("data/<int:pk>/", include(get_model_urls("forward_netbox", "forwarddata"))),
    path(
        "data/<int:pk>/delete",
        views.ForwardSnapshotDataDeleteView.as_view(),
        name="forwarddata_delete",
    ),
    # Sync
    path("sync/", views.ForwardSyncListView.as_view(), name="forwardsync_list"),
    path("sync/add/", views.ForwardSyncEditView.as_view(), name="forwardsync_add"),
    path(
        "sync/delete/",
        views.ForwardSyncBulkDeleteView.as_view(),
        name="forwardsync_bulk_delete",
    ),
    path("sync/<int:pk>/", include(get_model_urls("forward_netbox", "forwardsync"))),
    path(
        "sync/<int:pk>/delete/",
        views.ForwardSyncDeleteView.as_view(),
        name="forwardsync_delete",
    ),
    # NQE Map
    path(
        "nqe-map/",
        views.ForwardNQEQueryListView.as_view(),
        name="forwardnqequery_list",
    ),
    path(
        "nqe-map/restore/",
        views.ForwardNQEQueryRestoreView.as_view(),
        name="forwardnqequery_restore",
    ),
    path(
        "nqe-map/add",
        views.ForwardNQEQueryEditView.as_view(),
        name="forwardnqequery_add",
    ),
    path(
        "nqe-map/delete/",
        views.ForwardNQEQueryBulkDeleteView.as_view(),
        name="forwardnqequery_bulk_delete",
    ),
    path(
        "nqe-map/<int:pk>/",
        include(get_model_urls("forward_netbox", "forwardnqequery")),
    ),
    path(
        "nqe-map/<int:pk>/delete/",
        views.ForwardNQEQueryDeleteView.as_view(),
        name="forwardnqequery_delete",
    ),
    # Ingestion
    path(
        "ingestion/",
        views.ForwardIngestionListView.as_view(),
        name="forwardingestion_list",
    ),
    path(
        "ingestion/<int:pk>/",
        include(get_model_urls("forward_netbox", "forwardingestion")),
    ),
)
