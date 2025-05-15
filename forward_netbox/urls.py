from django.urls import include, path
from utilities.urls import get_model_urls

from . import views

urlpatterns = (
    # ForwardSource
    path("source/", views.ForwardSourceListView.as_view(), name="forwardsource_list"),
    path("source/add/", views.ForwardSourceEditView.as_view(), name="forwardsource_add"),
    path("source/delete/", views.ForwardSourceBulkDeleteView.as_view(), name="forwardsource_bulk_delete"),
    path("source/<int:pk>/delete/", views.ForwardSourceDeleteView.as_view(), name="forwardsource_delete"),
    path("source/<int:pk>/sync/", views.ForwardSourceSyncView.as_view(), name="forwardsource_sync"),
    path("source/<int:pk>/", include(get_model_urls("forward_netbox", "forwardsource"))),

    # ForwardSync
    path("sync/", views.ForwardSyncListView.as_view(), name="forwardsync_list"),
    path("sync/add/", views.ForwardSyncEditView.as_view(), name="forwardsync_add"),
    path("sync/delete/", views.ForwardSyncBulkDeleteView.as_view(), name="forwardsync_bulk_delete"),
    path("sync/<int:pk>/delete/", views.ForwardSyncDeleteView.as_view(), name="forwardsync_delete"),
    path("sync/<int:pk>/sync/", views.ForwardIngestSyncView.as_view(), name="forwardsync_sync"),
    path("sync/<int:pk>/", include(get_model_urls("forward_netbox", "forwardsync"))),

    # ForwardSnapshot
    path("snapshot/", views.ForwardSnapshotListView.as_view(), name="forwardsnapshot_list"),
    path("snapshot/delete/", views.ForwardSnapshotBulkDeleteView.as_view(), name="forwardsnapshot_bulk_delete"),
    path("snapshot/<int:pk>/delete/", views.ForwardSnapshotDeleteView.as_view(), name="forwardsnapshot_delete"),
    path("snapshot/<int:pk>/", include(get_model_urls("forward_netbox", "forwardsnapshot"))),

    # ForwardNQEMap
    path("nqe-map/", views.ForwardNQEMapListView.as_view(), name="forwardnqemap_list"),
    path("nqe-map/add/", views.ForwardNQEMapEditView.as_view(), name="forwardnqemap_add"),
    path("nqe-map/delete/", views.ForwardNQEMapBulkDeleteView.as_view(), name="forwardnqemap_bulk_delete"),
    path("nqe-map/restore/", views.ForwardNQEMapRestoreView.as_view(), name="forwardnqemap_restore"),
    path("nqe-map/<int:pk>/delete/", views.ForwardNQEMapDeleteView.as_view(), name="forwardnqemap_delete"),
    path("nqe-map/<int:pk>/", include(get_model_urls("forward_netbox", "forwardnqemap"))),
)