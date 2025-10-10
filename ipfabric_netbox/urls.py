from django.urls import include
from django.urls import path
from utilities.urls import get_model_urls

from . import views

urlpatterns = (
    # Source
    path("source/", views.IPFabricSourceListView.as_view(), name="ipfabricsource_list"),
    path(
        "source/add/", views.IPFabricSourceEditView.as_view(), name="ipfabricsource_add"
    ),
    path(
        "source/delete/",
        views.IPFabricSourceBulkDeleteView.as_view(),
        name="ipfabricsource_bulk_delete",
    ),
    path(
        "source/<int:pk>/", include(get_model_urls("ipfabric_netbox", "ipfabricsource"))
    ),
    path(
        "source/<int:pk>/delete/",
        views.IPFabricSourceDeleteView.as_view(),
        name="ipfabricsource_delete",
    ),
    # Snapshot
    path(
        "snapshot/",
        views.IPFabricSnapshotListView.as_view(),
        name="ipfabricsnapshot_list",
    ),
    path(
        "snapshot/delete/",
        views.IPFabricSnapshotBulkDeleteView.as_view(),
        name="ipfabricsnapshot_bulk_delete",
    ),
    path(
        "snapshot/<int:pk>/",
        include(get_model_urls("ipfabric_netbox", "ipfabricsnapshot")),
    ),
    path(
        "snapshot/<int:pk>/delete/",
        views.IPFabricSnapshotDeleteView.as_view(),
        name="ipfabricsnapshot_delete",
    ),
    # Snapshot Data
    path(
        "data/delete",
        views.IPFabricSnapshotDataBulkDeleteView.as_view(),
        name="ipfabricdata_bulk_delete",
    ),
    path("data/<int:pk>/", include(get_model_urls("ipfabric_netbox", "ipfabricdata"))),
    path(
        "data/<int:pk>/delete",
        views.IPFabricSnapshotDataDeleteView.as_view(),
        name="ipfabricdata_delete",
    ),
    # Sync
    path("sync/", views.IPFabricSyncListView.as_view(), name="ipfabricsync_list"),
    path("sync/add/", views.IPFabricSyncEditView.as_view(), name="ipfabricsync_add"),
    path(
        "sync/delete/",
        views.IPFabricSyncBulkDeleteView.as_view(),
        name="ipfabricsync_bulk_delete",
    ),
    path("sync/<int:pk>/", include(get_model_urls("ipfabric_netbox", "ipfabricsync"))),
    path(
        "sync/<int:pk>/delete/",
        views.IPFabricSyncDeleteView.as_view(),
        name="ipfabricsync_delete",
    ),
    # Ingestion
    path(
        "ingestion/",
        views.IPFabricIngestionListView.as_view(),
        name="ipfabricingestion_list",
    ),
    path(
        "ingestion/<int:pk>/",
        include(get_model_urls("ipfabric_netbox", "ipfabricingestion")),
    ),
    # Transform Map Group
    path(
        "transform-map-group/",
        views.IPFabricTransformMapGroupListView.as_view(),
        name="ipfabrictransformmapgroup_list",
    ),
    path(
        "transform-map-group/add",
        views.IPFabricTransformMapGroupEditView.as_view(),
        name="ipfabrictransformmapgroup_add",
    ),
    path(
        "transform-map-group/delete/",
        views.IPFabricTransformMapGroupBulkDeleteView.as_view(),
        name="ipfabrictransformmapgroup_bulk_delete",
    ),
    path(
        "transform-map-group/<int:pk>/",
        include(get_model_urls("ipfabric_netbox", "ipfabrictransformmapgroup")),
    ),
    path(
        "transform-map-group/<int:pk>/delete/",
        views.IPFabricTransformMapGroupDeleteView.as_view(),
        name="ipfabrictransformmapgroup_delete",
    ),
    # Transform Map
    path(
        "transform-map/",
        views.IPFabricTransformMapListView.as_view(),
        name="ipfabrictransformmap_list",
    ),
    path(
        "transform-map/restore/",
        views.IPFabricTransformMapRestoreView.as_view(),
        name="ipfabrictransformmap_restore",
    ),
    path(
        "transform-map/add",
        views.IPFabricTransformMapEditView.as_view(),
        name="ipfabrictransformmap_add",
    ),
    path(
        "transform-map/delete/",
        views.IPFabricTransformMapBulkDeleteView.as_view(),
        name="ipfabrictransformmap_bulk_delete",
    ),
    path(
        "transform-map/<int:pk>/",
        include(get_model_urls("ipfabric_netbox", "ipfabrictransformmap")),
    ),
    path(
        "transform-map/<int:pk>/delete/",
        views.IPFabricTransformMapDeleteView.as_view(),
        name="ipfabrictransformmap_delete",
    ),
    # Transform field
    path(
        "transform-field/",
        views.IPFabricTransformFieldListView.as_view(),
        name="ipfabrictransformfield_list",
    ),
    path(
        "transform-field/add/",
        views.IPFabricTransformFieldEditView.as_view(),
        name="ipfabrictransformfield_add",
    ),
    path(
        "transform-field/<int:pk>/",
        include(get_model_urls("ipfabric_netbox", "ipfabrictransformfield")),
    ),
    path(
        "transform-field/<int:pk>/delete/",
        views.IPFabricTransformFieldDeleteView.as_view(),
        name="ipfabrictransformfield_delete",
    ),
    # Relationship Field
    path(
        "relationship-field/",
        views.IPFabricRelationshipFieldListView.as_view(),
        name="ipfabricrelationshipfield_list",
    ),
    path(
        "relationship-field/add/",
        views.IPFabricRelationshipFieldEditView.as_view(),
        name="ipfabricrelationshipfield_add",
    ),
    path(
        "relationship-field/<int:pk>/",
        include(get_model_urls("ipfabric_netbox", "ipfabricrelationshipfield")),
    ),
    path(
        "relationship-field/<int:pk>/delete/",
        views.IPFabricRelationshipFieldDeleteView.as_view(),
        name="ipfabricrelationshipfield_delete",
    ),
)
