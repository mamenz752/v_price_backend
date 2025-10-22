from django.conf import settings
from django.http import HttpResponse, Http404, JsonResponse
from django.shortcuts import render, redirect
from django.views import View
from django.views.generic import TemplateView
from django.contrib import messages
from azure.storage.blob import BlobServiceClient
import csv
from io import StringIO
import traceback
import os

from .services import DataIngestor
from .models import Vegetable, Region


def _container_client():
    conn_str = settings.AZURE_CONNECTION_STRING
    container_name = settings.AZURE_CONTAINER
    
    print(f"接続文字列: {conn_str[:20]}... (一部のみ表示)")
    print(f"使用コンテナ: {container_name}")
    
    bsc = BlobServiceClient.from_connection_string(conn_str)
    container = bsc.get_container_client(container_name)
    return container

class IndexView(TemplateView):
    template_name = "ingest/index.html"




    
class BlobListView(View):
    """
    Azurite内の全ファイル一覧を表示するビュー
    デバッグ用に作成
    """
    def get(self, request):
        try:
            container = _container_client()
            blobs = []
            
            # 全BLOBを取得
            for blob in container.list_blobs():
                size = blob.size if hasattr(blob, 'size') else 'unknown'
                blobs.append({
                    'name': blob.name,
                    'size': size,
                    'last_modified': blob.last_modified.isoformat() if hasattr(blob, 'last_modified') else 'unknown'
                })
                
            # プレフィックス別にグループ化
            grouped = {
                'price': [b for b in blobs if b['name'].startswith(settings.INGEST_PREFIX_PRICE)],
                'weather': [b for b in blobs if b['name'].startswith(settings.INGEST_PREFIX_WEATHER)],
                'other': [b for b in blobs if not (b['name'].startswith(settings.INGEST_PREFIX_PRICE) or 
                                                b['name'].startswith(settings.INGEST_PREFIX_WEATHER))]
            }
            
            # JSON形式でファイル一覧を返す
            return JsonResponse({
                'blobs': blobs,
                'grouped': grouped,
                'price_prefix': settings.INGEST_PREFIX_PRICE,
                'weather_prefix': settings.INGEST_PREFIX_WEATHER,
            })
        except Exception as e:
            tb = traceback.format_exc()
            return HttpResponse(f"エラー: {str(e)}\n\n{tb}", content_type="text/plain; charset=utf-8")



class ImportPriceView(View):
    """
    価格データをインポートするためのビュー
    """
    def get(self, request):
        vegetables = Vegetable.objects.all()
        return render(request, 'ingest/import_price.html', {'vegetables': vegetables})
    
    def post(self, request):
        try:
            # リクエストからデータを取得
            vegetable_id = request.POST.get('vegetable')
            import_type = request.POST.get('import_type', 'file')  # ファイルまたはディレクトリ
            
            vegetable = Vegetable.objects.get(id=vegetable_id)
            
            if import_type == 'file':
                # 単一ファイルのインポート
                file_path = request.POST.get('file_path')
                if not file_path:
                    messages.error(request, 'ファイルパスが指定されていません。')
                    return redirect('ingest:import_price')
                
                try:
                    # ファイルの存在確認
                    if not os.path.exists(file_path):
                        messages.error(request, f'ファイル {file_path} が存在しません。')
                        return redirect('ingest:import_price')
                    
                    # ファイルからデータを読み込む
                    markets = DataIngestor.import_price_txt(file_path, vegetable)
                    print(f"市場データ読み込み結果: {len(markets) if markets else 0}件")
                    
                    if markets and len(markets) > 0:
                        # データを保存
                        saved_count = DataIngestor.save_price_data(markets)
                        print(f"データ保存結果: {saved_count}件")
                        messages.success(request, f'ファイル {os.path.basename(file_path)} から {saved_count}件のデータをインポートしました。')
                    else:
                        print("市場データが取得できませんでした。空のリストが返されました。")
                        messages.error(request, f'ファイル {os.path.basename(file_path)} のインポートに失敗しました。データが取得できません。')
                except Exception as e:
                    print(f"価格データインポート中の詳細エラー: {str(e)}")
                    print(traceback.format_exc())
                    messages.error(request, f'ファイル {os.path.basename(file_path)} のインポート中にエラー発生: {str(e)}')
            else:
                # ディレクトリのインポート
                directory_path = request.POST.get('directory_path')
                if not directory_path:
                    messages.error(request, 'ディレクトリパスが指定されていません。')
                    return redirect('ingest:import_price')
                
                imported_count = DataIngestor.import_price_files_from_dir(directory_path, vegetable)
                messages.success(request, f'合計 {imported_count} 件のデータをインポートしました。')
            
            return redirect('ingest:import_price')
        except Exception as e:
            messages.error(request, f'インポート処理中にエラーが発生しました: {str(e)}')
            return redirect('ingest:import_price')

class ImportWeatherView(View):
    """
    天気データをインポートするためのビュー
    """
    def get(self, request):
        regions = Region.objects.all()
        return render(request, 'ingest/import_weather.html', {'regions': regions})
    
    def post(self, request):
        try:
            # リクエストからデータを取得
            region_id = request.POST.get('region')
            import_type = request.POST.get('import_type', 'file')  # ファイルまたはディレクトリ
            
            region = Region.objects.get(id=region_id)
            
            if import_type == 'file':
                # 単一ファイルのインポート
                file_path = request.POST.get('file_path')
                if not file_path:
                    messages.error(request, 'ファイルパスが指定されていません。')
                    return redirect('ingest:import_weather')
                
                weather_objects = DataIngestor.import_weather_csv(file_path, region)
                saved_count = DataIngestor.save_weather_data(weather_objects)
                messages.success(request, f'ファイル {os.path.basename(file_path)} から {saved_count} 件のデータをインポートしました。')
            else:
                # ディレクトリのインポート
                directory_path = request.POST.get('directory_path')
                if not directory_path:
                    messages.error(request, 'ディレクトリパスが指定されていません。')
                    return redirect('ingest:import_weather')
                
                imported_count = DataIngestor.import_weather_files_from_dir(directory_path, region)
                messages.success(request, f'合計 {imported_count} 件のデータをインポートしました。')
            
            return redirect('ingest:import_weather')
        except Exception as e:
            messages.error(request, f'インポート処理中にエラーが発生しました: {str(e)}')
            return redirect('ingest:import_weather')

class ImportAllDataView(View):
    """
    全てのデータを一括インポートするためのビュー
    """
    def get(self, request):
        return render(request, 'ingest/import_all.html')
    
    def post(self, request):
        try:
            # 価格データをインポート
            price_results = DataIngestor.import_all_price_data()
            
            # 天気データをインポート
            weather_results = DataIngestor.import_all_weather_data()
            
            context = {
                'price_results': price_results,
                'weather_results': weather_results,
            }
            
            return render(request, 'ingest/import_results.html', context)
        except Exception as e:
            messages.error(request, f'インポート処理中にエラーが発生しました: {str(e)}')
            return redirect('ingest:import_all')