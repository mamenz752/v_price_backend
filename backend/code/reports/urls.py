from django.urls import path
from . import views

app_name = 'reports'
urlpatterns = [
    path('', views.IndexView.as_view(), name="index"),
    path('chinese_cabbage', views.ChineseCabbageView.as_view(), name="chinese_cabbage"),
    path('cabbage', views.CabbageView.as_view(), name="cabbage"),
    path('cucumber', views.CucumberView.as_view(), name="cucumber"),
    path('tomato', views.TomatoView.as_view(), name="tomato"),
    path('eggplant', views.EggplantView.as_view(), name="eggplant"),
    path('radish', views.RadishView.as_view(), name="radish"),
    path('potato', views.PotatoView.as_view(), name="potato"),
    path('onion', views.OnionView.as_view(), name="onion"),
    
    # 新しいIngestデータ表示用URLパス
    path('ingest/cabbage', views.IngestCabbageView.as_view(), name="ingest_cabbage"),
    path('ingest/chinese_cabbage', views.IngestChineseCabbageView.as_view(), name="ingest_chinese_cabbage"),
    path('ingest/cucumber', views.IngestCucumberView.as_view(), name="ingest_cucumber"),
    path('ingest/tomato', views.IngestTomatoView.as_view(), name="ingest_tomato"),
    path('ingest/eggplant', views.IngestEggplantView.as_view(), name="ingest_eggplant"),
    path('ingest/radish', views.IngestRadishView.as_view(), name="ingest_radish"),
    path('ingest/potato', views.IngestPotatoView.as_view(), name="ingest_potato"),
    path('ingest/onion', views.IngestOnionView.as_view(), name="ingest_onion"),
]