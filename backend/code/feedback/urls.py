from django.urls import path
from . import views

app_name = 'feedback'
urlpatterns = [
    # モデル管理関連のURL
    path('', views.IndexView.as_view(), name="index"),
    path('model/run/<str:vegetable>/', views.run_model, name='run_model'),
    path('model/run_multiple/<str:vegetable>/', views.run_multiple_models, name='run_multiple_models'),
    path('chinese_cabbage/', views.ChineseCabbageView.as_view(), name="chinese_cabbage"),
    path('cabbage/', views.CabbageView.as_view(), name="cabbage"),
    path('cucumber/', views.CucumberView.as_view(), name="cucumber"),
    # path('tomato/', views.TomatoView.as_view(), name="tomato"),
    path('eggplant/', views.EggplantView.as_view(), name="eggplant"),
    path('radish/', views.RadishView.as_view(), name="radish"),
    # path('potato/', views.PotatoView.as_view(), name="potato"),
    # path('onion/', views.OnionView.as_view(), name="onion"),
]