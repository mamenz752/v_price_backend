from django.urls import path
from . import views

app_name = 'reports'
urlpatterns = [
    path('', views.IndexView.as_view(), name="index"),
    path('chinese_cabbage', views.ChineseCabbageView.as_view(), name="chinese_cabbage"),
    path('cabbage', views.CabbageView.as_view(), name="cabbage"),
    path('cucumber', views.CucumberView.as_view(), name="cucumber"),
    # path('tomato', views.TomatoView.as_view(), name="tomato"),
    path('eggplant', views.EggplantView.as_view(), name="eggplant"),
    path('radish', views.RadishView.as_view(), name="radish"),
    # path('potato', views.PotatoView.as_view(), name="potato"),
    # path('onion', views.OnionView.as_view(), name="onion"),
]