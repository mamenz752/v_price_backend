from django.urls import path
from . import views

app_name = 'observe'
urlpatterns = [
    # モデル管理関連のURL
    # path('', views.IndexView.as_view(), name="index"),
    path('model/run/', views.run_model_by_webhook, name='run_model_by_webhook'),
]