from django.core.management.base import BaseCommand
from django.conf import settings
from azure.storage.blob import BlobServiceClient

class Command(BaseCommand):
    help = 'コンテナ内のBlobファイルをリストアップします'

    def handle(self, *args, **options):
        conn_str = settings.AZURE_CONNECTION_STRING
        container_name = settings.AZURE_CONTAINER
        
        self.stdout.write(f"接続文字列: {conn_str[:20]}... (一部のみ表示)")
        self.stdout.write(f"使用コンテナ: {container_name}")
        
        try:
            bsc = BlobServiceClient.from_connection_string(conn_str)
            container = bsc.get_container_client(container_name)
            
            # コンテナ内のすべてのBlobをリスト
            self.stdout.write("コンテナ内のファイル一覧:")
            blob_count = 0
            for blob in container.list_blobs():
                self.stdout.write(f"  - {blob.name} ({blob.size} バイト)")
                blob_count += 1
            
            if blob_count == 0:
                self.stdout.write(self.style.WARNING("ファイルが見つかりません！"))
            else:
                self.stdout.write(self.style.SUCCESS(f"合計 {blob_count} ファイルが見つかりました。"))
            
            # プレフィックス別にも確認
            price_prefix = settings.INGEST_PREFIX_PRICE
            weather_prefix = settings.INGEST_PREFIX_WEATHER
            
            price_blobs = list(container.list_blobs(name_starts_with=price_prefix))
            weather_blobs = list(container.list_blobs(name_starts_with=weather_prefix))
            
            self.stdout.write(f"\n'{price_prefix}'で始まるファイル: {len(price_blobs)}件")
            self.stdout.write(f"'{weather_prefix}'で始まるファイル: {len(weather_blobs)}件")
            
        except Exception as e:
            self.stdout.write(self.style.ERROR(f"エラーが発生しました: {str(e)}"))