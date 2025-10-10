from django.contrib import admin
from django.http import HttpResponse, HttpResponseRedirect
from django.template.response import TemplateResponse
from django.urls import path
from django.shortcuts import render, redirect
from django.contrib import messages
from django.conf import settings
import os
from pathlib import Path

# Register your models here.
from django.utils.html import format_html
from .models import IngestMarket, IngestWeather, Vegetable, Region
from .services import DataIngestor

class AdminImportMixin:
    """
    データインポート機能を提供するMixin
    """
    def get_urls(self):
        urls = super().get_urls()
        custom_urls = [
            path('import/', self.admin_site.admin_view(self.import_view), name=f'{self.model._meta.app_label}_{self.model._meta.model_name}_import'),
        ]
        return custom_urls + urls
    
    def import_view(self, request):
        """
        インポート画面を表示するビュー
        """
        context = dict(
            self.admin_site.each_context(request),
            opts=self.model._meta,
            title=f"{self.model._meta.verbose_name}のインポート",
            app_label=self.model._meta.app_label,
            original=None,
        )
        
        # 独自のコンテキストを追加
        context.update(self.get_import_context(request))
        
        # フォーム送信時の処理
        if request.method == 'POST':
            result = self.process_import(request)
            if isinstance(result, HttpResponseRedirect):
                return result
            context.update(result)
        
        return TemplateResponse(
            request, 
            self.get_import_template(),
            context,
        )
    
    def get_import_template(self):
        """
        インポート画面のテンプレートを返す
        """
        return 'admin/import_form.html'
    
    def get_import_context(self, request):
        """
        インポート画面のコンテキストを返す
        """
        return {}
    
    def process_import(self, request):
        """
        インポート処理を実行する
        """
        return {}

@admin.register(IngestMarket)
class IngestMarketAdmin(AdminImportMixin, admin.ModelAdmin):
    list_display = ("id", "target_date", "high_price", "medium_price", "low_price", "average_price", "arrival_amount", "weight_per", "vegetable")
    readonly_fields = ("id",)
    fields = ("target_date", "high_price", "medium_price", "low_price", "average_price", "arrival_amount", "weight_per", "vegetable")
    
    def get_import_context(self, request):
        return {
            'vegetables': Vegetable.objects.all(),
            'is_price_import': True,
        }
    
    def process_import(self, request):
        try:
            # リクエストからデータを取得
            vegetable_id = request.POST.get('vegetable')
            import_type = request.POST.get('import_type', 'file')
            
            if not vegetable_id:
                messages.error(request, '野菜が選択されていません。')
                return {}
            
            vegetable = Vegetable.objects.get(id=vegetable_id)
            
            if import_type == 'file':
                # 単一ファイルのインポート
                file_path = request.POST.get('file_path')
                if not file_path:
                    messages.error(request, 'ファイルパスが指定されていません。')
                    return {}
                
                market = DataIngestor.import_price_txt(file_path, vegetable)
                if market and DataIngestor.save_price_data(market):
                    messages.success(request, f'ファイル {os.path.basename(file_path)} のインポートに成功しました。')
                else:
                    messages.error(request, f'ファイル {os.path.basename(file_path)} のインポートに失敗しました。')
            else:
                # ディレクトリのインポート
                directory_path = request.POST.get('directory_path')
                if not directory_path:
                    messages.error(request, 'ディレクトリパスが指定されていません。')
                    return {}
                
                imported_count = DataIngestor.import_price_files_from_dir(directory_path, vegetable)
                messages.success(request, f'合計 {imported_count} 件のデータをインポートしました。')
            
            return HttpResponseRedirect(request.path)
        except Exception as e:
            messages.error(request, f'インポート処理中にエラーが発生しました: {str(e)}')
            return {}

@admin.register(IngestWeather)
class IngestWeatherAdmin(AdminImportMixin, admin.ModelAdmin):
    list_display = ("id", "target_date", "max_temp", "mean_temp", "min_temp", "sum_precipitation", "sunshine_duration", "ave_humidity", "region")
    readonly_fields = ("id",)
    fields = ("target_date", "max_temp", "mean_temp", "min_temp", "sum_precipitation", "sunshine_duration", "ave_humidity", "region")
    
    def get_import_context(self, request):
        return {
            'regions': Region.objects.all(),
            'is_weather_import': True,
        }
    
    def process_import(self, request):
        try:
            # リクエストからデータを取得
            region_id = request.POST.get('region')
            import_type = request.POST.get('import_type', 'file')
            
            if not region_id:
                messages.error(request, '地域が選択されていません。')
                return {}
            
            region = Region.objects.get(id=region_id)
            
            if import_type == 'file':
                # 単一ファイルのインポート
                file_path = request.POST.get('file_path')
                if not file_path:
                    messages.error(request, 'ファイルパスが指定されていません。')
                    return {}
                
                weather_objects = DataIngestor.import_weather_csv(file_path, region)
                saved_count = DataIngestor.save_weather_data(weather_objects)
                messages.success(request, f'ファイル {os.path.basename(file_path)} から {saved_count} 件のデータをインポートしました。')
            else:
                # ディレクトリのインポート
                directory_path = request.POST.get('directory_path')
                if not directory_path:
                    messages.error(request, 'ディレクトリパスが指定されていません。')
                    return {}
                
                imported_count = DataIngestor.import_weather_files_from_dir(directory_path, region)
                messages.success(request, f'合計 {imported_count} 件のデータをインポートしました。')
            
            return HttpResponseRedirect(request.path)
        except Exception as e:
            messages.error(request, f'インポート処理中にエラーが発生しました: {str(e)}')
            return {}

@admin.register(Vegetable)
class VegetableAdmin(admin.ModelAdmin):
    list_display = ("id", "name", "code")
    readonly_fields = ("id",)
    fields = ("name", "code")

@admin.register(Region)
class RegionAdmin(admin.ModelAdmin):
    list_display = ("id", "name", "p_area_code", "market_code", "fuken_code", "station_code")
    readonly_fields = ("id",)
    fields = ("name", "p_area_code", "market_code", "fuken_code", "station_code")

# カスタム管理サイトを作成し、全データインポート用のビューを追加
class IngestAdminSite(admin.AdminSite):
    site_header = "データ取り込み管理"
    site_title = "データ取り込み管理"
    index_title = "データ取り込み管理インデックス"
    
    def get_urls(self):
        urls = super().get_urls()
        custom_urls = [
            path('import-all/', self.admin_view(self.import_all_view), name='import_all'),
        ]
        return custom_urls + urls
    
    def import_all_view(self, request):
        """
        すべてのデータをインポートするビュー
        """
        context = dict(
            self.each_context(request),
            title="すべてのデータをインポート",
            settings={
                'INGEST_ROOT': settings.INGEST_ROOT,
                'INGEST_PREFIX_PRICE': settings.INGEST_PREFIX_PRICE,
                'INGEST_PREFIX_WEATHER': settings.INGEST_PREFIX_WEATHER
            }
        )
        
        if request.method == 'POST':
            try:
                # 価格データをインポート
                price_results = DataIngestor.import_all_price_data()
                
                # 天気データをインポート
                weather_results = DataIngestor.import_all_weather_data()
                
                context.update({
                    'price_results': price_results,
                    'weather_results': weather_results,
                    'success': True,
                })
                
                messages.success(request, '全データのインポートが完了しました！')
            except Exception as e:
                messages.error(request, f'インポート処理中にエラーが発生しました: {str(e)}')
        
        return TemplateResponse(request, 'admin/import_all.html', context)

# カスタム管理サイトを使用するか否かはプロジェクトの要件に応じて変更してください
# 通常のadmin.siteを使用する場合は、以下のコードをコメントアウトし、
# 代わりにadmin.siteにカスタムビューを追加する方法を選択することもできます。

# インポート機能を通常のadmin.siteに追加
admin.site.site_header = "データ取り込み管理"
admin.site.site_title = "データ取り込み管理"
admin.site.index_title = "データ取り込み管理インデックス"

# 全データインポート用のカスタムビューを追加
# register_viewメソッドがない場合のための代替手段
try:
    # Django 3.1+
    admin.site.register_view('import-all/', view=lambda request: admin.site.admin_view(IngestAdminSite().import_all_view)(request), name='すべてのデータをインポート')
except AttributeError:
    # register_viewがない場合は、URLパターンを直接追加するか、
    # admin.pyに以下の機能を実装する必要があります
    pass

# Django 3.1未満の場合、URLパターンを手動で追加するためのコード
# このコードはurls.pyに追加する必要があります
"""
from django.urls import path
from ingest.admin import IngestAdminSite

urlpatterns = [
    # 他のURLパターン...
    path('admin/import-all/', admin.site.admin_view(IngestAdminSite().import_all_view), name='admin_import_all'),
]
"""