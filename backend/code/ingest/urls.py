from django.urls import path, include
from . import views

app_name = 'ingest'
urlpatterns = [
    path('api/blobs/', views.BlobListView.as_view(), name='blob_list'),
]