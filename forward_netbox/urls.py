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
    # Transform Map Group
    path(
        "transform-map-group/",
        views.ForwardTransformMapGroupListView.as_view(),
        name="forwardtransformmapgroup_list",
    ),
    path(
        "transform-map-group/add",
        views.ForwardTransformMapGroupEditView.as_view(),
        name="forwardtransformmapgroup_add",
    ),
    path(
        "transform-map-group/delete/",
        views.ForwardTransformMapGroupBulkDeleteView.as_view(),
        name="forwardtransformmapgroup_bulk_delete",
    ),
    path(
        "transform-map-group/<int:pk>/",
        include(get_model_urls("forward_netbox", "forwardtransformmapgroup")),
    ),
    path(
        "transform-map-group/<int:pk>/delete/",
        views.ForwardTransformMapGroupDeleteView.as_view(),
        name="forwardtransformmapgroup_delete",
    ),
    # Transform Map
    path(
        "transform-map/",
        views.ForwardTransformMapListView.as_view(),
        name="forwardtransformmap_list",
    ),
    path(
        "transform-map/restore/",
        views.ForwardTransformMapRestoreView.as_view(),
        name="forwardtransformmap_restore",
    ),
    path(
        "transform-map/add",
        views.ForwardTransformMapEditView.as_view(),
        name="forwardtransformmap_add",
    ),
    path(
        "transform-map/delete/",
        views.ForwardTransformMapBulkDeleteView.as_view(),
        name="forwardtransformmap_bulk_delete",
    ),
    path(
        "transform-map/<int:pk>/",
        include(get_model_urls("forward_netbox", "forwardtransformmap")),
    ),
    path(
        "transform-map/<int:pk>/delete/",
        views.ForwardTransformMapDeleteView.as_view(),
        name="forwardtransformmap_delete",
    ),
    # Transform field
    path(
        "transform-field/",
        views.ForwardTransformFieldListView.as_view(),
        name="forwardtransformfield_list",
    ),
    path(
        "transform-field/add/",
        views.ForwardTransformFieldEditView.as_view(),
        name="forwardtransformfield_add",
    ),
    path(
        "transform-field/<int:pk>/",
        include(get_model_urls("forward_netbox", "forwardtransformfield")),
    ),
    path(
        "transform-field/<int:pk>/delete/",
        views.ForwardTransformFieldDeleteView.as_view(),
        name="forwardtransformfield_delete",
    ),
    # Relationship Field
    path(
        "relationship-field/",
        views.ForwardRelationshipFieldListView.as_view(),
        name="forwardrelationshipfield_list",
    ),
    path(
        "relationship-field/add/",
        views.ForwardRelationshipFieldEditView.as_view(),
        name="forwardrelationshipfield_add",
    ),
    path(
        "relationship-field/<int:pk>/",
        include(get_model_urls("forward_netbox", "forwardrelationshipfield")),
    ),
    path(
        "relationship-field/<int:pk>/delete/",
        views.ForwardRelationshipFieldDeleteView.as_view(),
        name="forwardrelationshipfield_delete",
    ),
)
