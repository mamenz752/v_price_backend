from django.urls import path, include
from . import views

app_name = 'ingest'
urlpatterns = [
    path('', views.IndexView.as_view(), name='index'),
    path("txt/", views.TxtListView.as_view(), name="txt_list"),
    path("txt/<path:name>/", views.TxtDetailView.as_view(), name="txt_detail"),
]