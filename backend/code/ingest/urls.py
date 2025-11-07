from django.urls import path, include
from . import views

app_name = 'ingest'
urlpatterns = [
    path('api/blobs/', views.BlobListView.as_view(), name='blob_list'),
    path('api/webhook/price/daily', views.DailyPriceWebhook, name='daily_price_webhook'),
    path('api/webhook/price/deadline', views.DeadlinePriceWebhook, name='deadline_price_webhook'),
    path('api/webhook/weather/deadline', views.DeadlineWeatherWebhook, name='deadline_weather_webhook')
]