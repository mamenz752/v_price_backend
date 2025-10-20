from django.urls import path
from . import views

app_name = 'forecast'

urlpatterns = [
    path('', views.index, name='index'),
    path('run/', views.run_model, name='run_model'),
    path('run-multiple/', views.run_multiple_models, name='run_multiple_models'),
]