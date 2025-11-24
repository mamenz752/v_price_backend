# CSVテストスクリプト - シェルでテストする用
# python manage.py shell < test_csv.py

from django.conf import settings
from config.storage.azure_blob import get_blob_service_client
import csv
from io import StringIO
import traceback

def debug_csv_file(blob_name):
    """
    特定のCSVファイルを読み込んでデバッグ情報を表示する
    """
    print(f"[DEBUG] CSVファイル読み込みテスト: {blob_name}")
    
    # Azuriteに接続
    conn_str = settings.AZURE_CONNECTION_STRING
    container_name = settings.AZURE_CONTAINER
    
    print(f"接続文字列: {conn_str[:20]}... (一部のみ表示)")
    print(f"使用コンテナ: {container_name}")
    
    try:
        container = get_blob_service_client()
        # ファイルが存在するか確認
        print(f"- ファイル存在確認: {blob_name}")
        blob_client = container.get_blob_client(blob_name)
        
        try:
            properties = blob_client.get_blob_properties()
            print(f"  ファイルサイズ: {properties.size} バイト")
        except Exception as e:
            print(f"  ファイル存在確認エラー: {str(e)}")
            return
        
        # ファイル内容を取得
        print("- ファイル内容取得")
        try:
            download_stream = blob_client.download_blob()
            file_data = download_stream.readall()
            print(f"  ダウンロードサイズ: {len(file_data)} バイト")
        except Exception as e:
            print(f"  ダウンロードエラー: {str(e)}")
            return
            
        # エンコーディング検出
        print("- エンコーディング検出")
        encodings = ['utf-8', 'shift-jis', 'euc-jp', 'iso-2022-jp']
        csv_data = None
        used_encoding = None
        
        for enc in encodings:
            try:
                csv_data = file_data.decode(enc, errors="strict")
                used_encoding = enc
                print(f"  エンコーディング {enc} で正常に読み込みました")
                break
            except UnicodeDecodeError:
                print(f"  {enc} は不適合")
                continue
                
        if csv_data is None:
            print("  すべてのエンコーディングで失敗、replace指定でデコード")
            csv_data = file_data.decode('utf-8', errors='replace')
            used_encoding = 'utf-8 (replace)'
        
        # CSVパース
        print("- CSVデータのパース")
        try:
            csv_reader = csv.reader(StringIO(csv_data))
            headers = next(csv_reader, [])
            print(f"  ヘッダー ({len(headers)}列): {headers}")
            
            # 最初の数行を表示
            for i, row in enumerate(csv_reader):
                if i < 5:
                    print(f"  行 {i+1}: {row}")
                else:
                    break
                    
            # 行数を数える
            count = i + 1
            for _ in csv_reader:
                count += 1
            print(f"  総行数: {count}")
            
            print("CSVファイルの読み込み成功")
            return True
            
        except Exception as e:
            print(f"CSVパースエラー: {str(e)}")
            print(traceback.format_exc())
            
            # エラーの詳細診断
            print("- CSV形式診断")
            lines = csv_data.split('\n')
            print(f"  行数: {len(lines)}")
            print(f"  先頭5行:")
            for i, line in enumerate(lines[:5]):
                print(f"  {i+1}: {line[:100]}")
                
            return False
        
    except Exception as e:
        print(f"予期しないエラー: {str(e)}")
        print(traceback.format_exc())
        return False

# テスト実行
weather_file = "weather/2025/04/2025_04_mid.csv"
debug_csv_file(weather_file)