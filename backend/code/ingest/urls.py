from django.urls import path, include
from . import views

app_name = 'ingest'
urlpatterns = [
    path('', views.IndexView.as_view(), name='index'),
    path("price/", views.PriceListView.as_view(), name="price_list"),
    path("price/<path:name>/", views.PriceDetailView.as_view(), name="price_detail"),
    path('weather/', views.WeatherListView.as_view(), name='weather_list'),
    path('weather/<path:name>/', views.WeatherDetailView.as_view(), name='weather_detail'),
    path('api/blobs/', views.BlobListView.as_view(), name='blob_list'),
]