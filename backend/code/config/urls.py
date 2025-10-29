"""
URL configuration for config project.

The `urlpatterns` list routes URLs to views. For more information please see:
    https://docs.djangoproject.com/en/5.2/topics/http/urls/
Examples:
Function views
    1. Add an import:  from my_app import views
    2. Add a URL to urlpatterns:  path('', views.home, name='home')
Class-based views
    1. Add an import:  from other_app.views import Home
    2. Add a URL to urlpatterns:  path('', Home.as_view(), name='home')
Including another URLconf
    1. Import the include() function: from django.urls import include, path
    2. Add a URL to urlpatterns:  path('blog/', include('blog.urls'))
"""
from django.contrib import admin
from django.urls import path, include
from django.views.generic import TemplateView, RedirectView
from django.shortcuts import redirect
from ingest.admin import IngestAdminSite
from compute.admin import (
    compute_all_view,
    compute_market_view,
    compute_weather_view,
    compute_reset_view,
)
from django.contrib.admin.views.decorators import staff_member_required

# 管理サイトのインデックスページをカスタムテンプレートで上書き
admin.site.index_template = 'admin/custom_index.html'

urlpatterns = [
    path('', include('reports.urls')),
    path('feedback/', include('feedback.urls')),
    path('ingest/', include('ingest.urls')),
    path('forecast/', include('forecast.urls')),
    path('accounts/', include('accounts.urls')),
    # カスタム管理ビューのURLを admin.site.urls より先に追加して名前解決を確実にする
    path('admin/import-all/', admin.site.admin_view(IngestAdminSite().import_all_view), name='admin_import_all'),
    path('admin/import-price/', admin.site.admin_view(IngestAdminSite().import_price_view), name='admin_import_price'),
    path('admin/import-weather/', admin.site.admin_view(IngestAdminSite().import_weather_view), name='admin_import_weather'),
    path('admin/delete-all/', admin.site.admin_view(IngestAdminSite().delete_all_view), name='admin_delete_all'),
    path('admin/compute/all/', admin.site.admin_view(compute_all_view), name='admin_compute_all'),
    path('admin/compute/market/', admin.site.admin_view(compute_market_view), name='admin_compute_market'),
    path('admin/compute/weather/', admin.site.admin_view(compute_weather_view), name='admin_compute_weather'),
    path('admin/compute/reset/', admin.site.admin_view(compute_reset_view), name='admin_compute_reset'),
    path('admin/', admin.site.urls),
    # ルートURLを管理画面にリダイレクト
    # path('', RedirectView.as_view(url='/admin/', permanent=False), name='index'),
]
