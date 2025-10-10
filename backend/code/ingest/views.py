from django.conf import settings
from django.http import HttpResponse, Http404, JsonResponse
from django.shortcuts import render
from django.views import View
from django.views.generic import TemplateView
from azure.storage.blob import BlobServiceClient
import csv
from io import StringIO
import traceback


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

class PriceListView(TemplateView):
    template_name = "ingest/txt_list.html"

    def get(self, request):
        container = _container_client()
        # priceのみを対象（必要なければ name_starts_with を使わず全部）
        prefix = settings.INGEST_PREFIX_PRICE  # 例: "txt/"
        # サーバー側で prefix 絞り込み
        blobs_iter = container.list_blobs(name_starts_with=prefix)
        # .txt のみ、リンク用に prefix を除いた相対名も持たせる
        items = []
        for b in blobs_iter:
            name = str(b.name)
            if not name.lower().endswith(".txt"):
                continue
            rel = name[len(prefix):] if name.startswith(prefix) else name
            items.append({"full": name, "rel": rel})
        # 表示の見やすさでソート（任意）
        items.sort(key=lambda x: x["rel"])
        return render(request, self.template_name, {"items": items})

class PriceDetailView(View):
   def get(self, request, name: str):
        if ".." in name:
            raise Http404("Invalid path")
        
        # 先頭と末尾のスラッシュを除去（安全のため）
        name = name.strip('/')
        
        prefix = settings.INGEST_PREFIX_PRICE  # 例: "price/"
        full_name = f"{prefix}{name}"
        
        print(f"アクセスしているファイル: {full_name}")  # デバッグ用
        
        container = _container_client()
        try:
            # ファイルの存在確認
            blob_client = container.get_blob_client(full_name)
            
            # 存在確認（これがないと存在チェックせずにダウンロード試行してエラーになる）
            properties = blob_client.get_blob_properties()
            print(f"ファイルサイズ: {properties.size} バイト")
            
            # 存在する場合のみ読み込み
            data = blob_client.download_blob().readall().decode("utf-8", errors="replace")
            
        except Exception as e:
            print(f"エラー詳細: {str(e)}")  # デバッグ用
            raise Http404(f"ファイルが見つかりません: {full_name}")
            
        return HttpResponse(data, content_type="text/plain; charset=utf-8")

class WeatherListView(TemplateView):
    template_name = "ingest/weather_list.html"

    def get(self, request):
        container = _container_client()
        prefix = settings.INGEST_PREFIX_WEATHER  # settings.pyから値を取得
        blobs_iter = container.list_blobs(name_starts_with=prefix)
        items = []
        for b in blobs_iter:
            name = str(b.name)
            rel = name[len(prefix):] if name.startswith(prefix) else name
            # CSVかどうかをフラグとして追加
            is_csv = name.lower().endswith('.csv')
            items.append({
                "full": name, 
                "rel": rel,
                "is_csv": is_csv
            })
        items.sort(key=lambda x: x["rel"])
        return render(request, self.template_name, {"items": items})
    
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

class WeatherDetailView(View):
    def get(self, request, name: str):
        if ".." in name:
            raise Http404("Invalid path")
        
        # 先頭と末尾のスラッシュを除去（安全のため）
        name = name.strip('/')
        
        prefix = settings.INGEST_PREFIX_WEATHER  # "weather/"
        full_name = f"{prefix}{name}"
        
        print(f"アクセスしているWeatherファイル: {full_name}")  # デバッグ用
        
        container = _container_client()
        try:
            # ファイルの存在確認
            blob_client = container.get_blob_client(full_name)
            
            try:
                # 存在確認（これがないと存在チェックせずにダウンロード試行してエラーになる）
                properties = blob_client.get_blob_properties()
                print(f"ファイルサイズ: {properties.size} バイト")
            except Exception as e:
                print(f"ファイル存在確認エラー: {str(e)}")
                raise Http404(f"ファイルが見つかりません: {full_name}")
            
            try:
                # ファイルの内容を取得
                download_stream = blob_client.download_blob()
                file_data = download_stream.readall()
                print(f"ダウンロードしたデータサイズ: {len(file_data)} バイト")
            except Exception as e:
                print(f"ダウンロードエラー: {str(e)}")
                raise Http404(f"ファイルの読み込みに失敗しました: {full_name}")
            
            # 複数のエンコーディングを試す
            encodings = ['utf-8', 'shift-jis', 'euc-jp', 'iso-2022-jp']
            csv_data = None
            
            for enc in encodings:
                try:
                    csv_data = file_data.decode(enc, errors="replace")
                    print(f"エンコーディング {enc} で正常に読み込みました")
                    break
                except UnicodeDecodeError:
                    continue
            
            if csv_data is None:
                print("すべてのエンコーディングで失敗しました")
                csv_data = file_data.decode('utf-8', errors='replace')  # フォールバック
            
            # CSVファイルの場合のみCSVとして処理
            if name.lower().endswith('.csv'):
                try:
                    # CSVデータをパース
                    csv_reader = csv.reader(StringIO(csv_data))
                    headers = next(csv_reader, [])  # ヘッダー行
                    print(f"CSV ヘッダー: {headers}")
                    
                    # 行データを取得（エラーハンドリング付き）
                    rows = []
                    for i, row in enumerate(csv_reader):
                        rows.append(row)
                        if i < 5:  # 最初の数行だけ出力
                            print(f"CSV 行 {i}: {row}")
                    
                    print(f"合計 {len(rows)} 行のデータを読み込みました")
                    
                    context = {
                        'filename': name,
                        'headers': headers,
                        'rows': rows
                    }
                    
                    return render(request, 'ingest/csv_detail.html', context)
                except Exception as e:
                    print(f"CSVパースエラー: {str(e)}")
                    print(traceback.format_exc())  # スタックトレースを出力
                    # CSVとして解析できなくても、テキストとして表示
                    return HttpResponse(f"CSVパースエラー: {str(e)}\n\n元データ:\n\n{csv_data[:1000]}...", 
                                       content_type="text/plain; charset=utf-8")
            else:
                # 非CSVファイルはプレーンテキストとして表示
                return HttpResponse(csv_data, content_type="text/plain; charset=utf-8")
            
        except Exception as e:
            tb = traceback.format_exc()
            print(f"予期しないエラー: {str(e)}")  # デバッグ用
            print(tb)  # スタックトレースを出力
            raise Http404(f"ファイルの読み込みに失敗しました: {full_name} ({str(e)})")