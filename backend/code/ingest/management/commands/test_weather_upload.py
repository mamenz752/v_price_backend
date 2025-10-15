from django.core.management.base import BaseCommand
from django.conf import settings
from pathlib import Path
import os
from azure.storage.blob import BlobServiceClient

class Command(BaseCommand):
    help = 'Azuriteに天気データファイルをテストアップロード'
    
    def handle(self, *args, **options):
        # Azure接続情報確認
        self.stdout.write(f"AZURE_CONNECTION_STRING: {'設定あり' if hasattr(settings, 'AZURE_CONNECTION_STRING') and settings.AZURE_CONNECTION_STRING else '設定なし'}")
        self.stdout.write(f"AZURE_CONTAINER: {getattr(settings, 'AZURE_CONTAINER', '設定なし')}")
        
        # ディレクトリ確認
        weather_dir = '/data/weather'
        self.stdout.write(f"ディレクトリ確認 {weather_dir}: {os.path.exists(weather_dir)}")
        if os.path.exists(weather_dir):
            dirs = [f for f in os.listdir(weather_dir) if os.path.isdir(os.path.join(weather_dir, f))]
            self.stdout.write(f"サブディレクトリ: {dirs}")
        
        # ファイル確認
        test_file = '/data/weather/2018/01/2018_01_mid.csv'
        self.stdout.write(f"ファイル確認 {test_file}: {os.path.exists(test_file)}")
        
        try:
            # Azure接続テスト
            bsc = BlobServiceClient.from_connection_string(settings.AZURE_CONNECTION_STRING)
            container = bsc.get_container_client(settings.AZURE_CONTAINER)
            self.stdout.write(self.style.SUCCESS("Azure接続成功"))
            
            # ファイルアップロードテスト
            if os.path.exists(test_file):
                with open(test_file, 'rb') as f:
                    content = f.read()
                    self.stdout.write(f"ファイル読み込み: {len(content)} バイト")
                    
                    blob_name = 'weather/2018/01/2018_01_mid.csv'
                    container.upload_blob(name=blob_name, data=content, overwrite=True)
                    self.stdout.write(self.style.SUCCESS(f"アップロード成功: {blob_name}"))
            else:
                self.stdout.write(self.style.ERROR("テストファイルが見つかりません"))
        
        except Exception as e:
            self.stdout.write(self.style.ERROR(f"エラー発生: {str(e)}"))