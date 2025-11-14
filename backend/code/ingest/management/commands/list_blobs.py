from django.core.management.base import BaseCommand
from django.conf import settings
from azure.storage.blob import BlobServiceClient
import os

class Command(BaseCommand):
    help = 'コンテナ内のBlobファイルをリストアップし、ローカルデータフォルダの内容をAzuriteにアップロードします'
    
    def add_arguments(self, parser):
        parser.add_argument('--upload', action='store_true', help='ローカルのデータファイルをAzuriteにアップロードします')
        parser.add_argument('--list-only', action='store_true', help='既存のBlobのリストのみを表示します')
        parser.add_argument('--prefix', type=str, help='特定のプレフィックスを持つBlobだけをリストアップします（例: 2019/01/）')

    def handle(self, *args, **options):
        conn_str = settings.AZURE_CONNECTION_STRING
        container_name = settings.AZURE_CONTAINER
        
        self.stdout.write(f"接続文字列: {conn_str[:20]}... (一部のみ表示)")
        self.stdout.write(f"使用コンテナ: {container_name}")
        
        try:
            bsc = BlobServiceClient.from_connection_string(conn_str)
            container = bsc.get_container_client(container_name)
            
            # ローカルファイルのアップロード要求があれば処理
            if options['upload']:
                self.upload_local_files(container)
                return
            
            # 特定のプレフィックスでフィルタリング
            prefix = options.get('prefix')
            if prefix:
                self.list_blobs_with_prefix(container, prefix)
                return
            
            # コンテナ内のすべてのBlobをリスト
            if not options['list_only']:
                self.list_all_blobs(container)
            
            # 年別にファイル数を集計
            self.list_blobs_by_year(container)
            
            # カテゴリ別（price/weatherディレクトリ）に集計
            self.list_blobs_by_category(container)
            
        except Exception as e:
            self.stdout.write(self.style.ERROR(f"エラーが発生しました: {str(e)}"))
    
    def list_all_blobs(self, container):
        """すべてのBlobをリストアップ"""
        self.stdout.write("コンテナ内のファイル一覧:")
        blob_count = 0
        for blob in container.list_blobs():
            self.stdout.write(f"  - {blob.name} ({blob.size} バイト)")
            blob_count += 1
            # 出力量が多すぎる場合は制限
            if blob_count >= 200:
                self.stdout.write(self.style.WARNING("出力が多すぎるため表示を省略しました..."))
                break
        
        if blob_count == 0:
            self.stdout.write(self.style.WARNING("ファイルが見つかりません！"))
        else:
            self.stdout.write(self.style.SUCCESS(f"合計 {blob_count} ファイルが見つかりました。"))
    
    def list_blobs_with_prefix(self, container, prefix):
        """指定プレフィックスのBlobをリストアップ"""
        self.stdout.write(f"プレフィックス '{prefix}' のファイル一覧:")
        blobs = list(container.list_blobs(name_starts_with=prefix))
        
        for blob in blobs:
            self.stdout.write(f"  - {blob.name} ({blob.size} バイト)")
        
        self.stdout.write(self.style.SUCCESS(f"合計 {len(blobs)} ファイルが見つかりました。"))
    
    def list_blobs_by_year(self, container):
        """年別にファイル数を集計"""
        # 年別にファイルをカウント
        year_counts = {}
        
        # すべてのBlobをリスト
        for blob in container.list_blobs():
            name = blob.name
            parts = name.split('/')
            if len(parts) >= 1 and parts[0].isdigit() and len(parts[0]) == 4:  # YYYY形式の年
                year = parts[0]
                if year not in year_counts:
                    year_counts[year] = 0
                year_counts[year] += 1
        
        # 年別の集計を表示
        if year_counts:
            self.stdout.write("\n年別ファイル数:")
            for year, count in sorted(year_counts.items()):
                self.stdout.write(f"  {year}年: {count} ファイル")
    
    def list_blobs_by_category(self, container):
        """カテゴリ別にファイルを集計"""
        # ローカルディレクトリ構造に基づくカテゴリ
        categories = {
            'price': '価格データ',
            'weather': '天気データ'
        }
        
        # カテゴリごとに検索とカウント
        self.stdout.write("\nカテゴリ別ファイル情報:")
        
        # Azurite内のBlobをカウント
        for category, label in categories.items():
            # カテゴリごとに年別分布も確認
            self.stdout.write(f"\n{label}(Azurite内):")
            
            # 年別にファイル数をカウント
            year_counts = {}
            total_count = 0
            
            # カテゴリに対応するローカルディレクトリ
            local_dir = f"/data/{category}"
            if os.path.exists(local_dir):
                # ローカルディレクトリ内の年フォルダを取得
                years = [d for d in os.listdir(local_dir) if os.path.isdir(os.path.join(local_dir, d)) and d.isdigit()]
                
                # 各年ごとにAzurite内のファイルをカウント
                for year in sorted(years):
                    # この年のファイルをカウント
                    year_path = f"{category}/{year}"  # 正しいプレフィックスを付与
                    year_blobs = list(container.list_blobs(name_starts_with=year_path))
                    year_count = len(year_blobs)
                    
                    if year_count > 0:
                        year_counts[year] = year_count
                        total_count += year_count
                        self.stdout.write(f"  {year}年: {year_count} ファイル")
            
            if total_count == 0:
                self.stdout.write(f"  {label}のファイルはAzurite内に見つかりません")
            else:
                self.stdout.write(f"  合計: {total_count} ファイル")
            
            # ローカルディレクトリの情報も表示
            self.stdout.write(f"\n{label}(ローカルディレクトリ):")
            self.list_local_files_by_category(category)
    
    def list_local_files_by_category(self, category):
        """指定カテゴリのローカルファイル情報を表示"""
        local_dir = f"/data/{category}"
        if not os.path.exists(local_dir):
            self.stdout.write(f"  ローカルディレクトリ {local_dir} が見つかりません")
            return
        
        # 年別ディレクトリを取得
        years = [d for d in os.listdir(local_dir) if os.path.isdir(os.path.join(local_dir, d)) and d.isdigit()]
        
        if not years:
            self.stdout.write("  年別ディレクトリが見つかりません")
            return
        
        # 各年のファイル数をカウント
        total_count = 0
        for year in sorted(years):
            year_dir = os.path.join(local_dir, year)
            year_count = 0
            
            # 価格データの場合は月別ディレクトリがある
            if category == 'price':
                months = [d for d in os.listdir(year_dir) if os.path.isdir(os.path.join(year_dir, d))]
                for month in months:
                    month_dir = os.path.join(year_dir, month)
                    month_files = [f for f in os.listdir(month_dir) if os.path.isfile(os.path.join(month_dir, f))]
                    year_count += len(month_files)
            else:
                # 天気データも月別ディレクトリがある
                months = [d for d in os.listdir(year_dir) if os.path.isdir(os.path.join(year_dir, d))]
                year_count = 0
                for month in months:
                    month_dir = os.path.join(year_dir, month)
                    month_files = [f for f in os.listdir(month_dir) if os.path.isfile(os.path.join(month_dir, f))]
                    year_count += len(month_files)
            
            self.stdout.write(f"  {year}年: {year_count} ファイル")
            total_count += year_count
        
        self.stdout.write(f"  合計: {total_count} ファイル")
    
    def upload_local_files(self, container):
        """ローカルディレクトリからファイルをAzuriteにアップロード"""
        categories = ['price', 'weather']
        total_uploaded = 0
        
        try:
            for category in categories:
                local_dir = f"/{settings.MEDIA_ROOT}/{category}"
                if not os.path.exists(local_dir):
                    self.stdout.write(self.style.WARNING(f"ディレクトリが見つかりません: {local_dir}"))
                    continue
                
                self.stdout.write(f"{category}データのアップロードを開始します...")
                
                # 年別ディレクトリを処理
                years = [d for d in os.listdir(local_dir) if os.path.isdir(os.path.join(local_dir, d)) and d.isdigit()]
                for year in years:
                    year_dir = os.path.join(local_dir, year)
                    
                    # 価格データは月別ディレクトリがある
                    if category == 'price':
                        months = [d for d in os.listdir(year_dir) if os.path.isdir(os.path.join(year_dir, d))]
                        for month in months:
                            month_dir = os.path.join(year_dir, month)
                            files = [f for f in os.listdir(month_dir) if os.path.isfile(os.path.join(month_dir, f))]
                            
                            for file in files:
                                # 価格データのAzuriteパスは: price/YYYY/MM/YYYY-MM-DD.txt
                                local_path = os.path.join(month_dir, file)
                                azure_path = f"price/{year}/{month}/{file}"
                                
                                with open(local_path, 'rb') as data:
                                    container.upload_blob(name=azure_path, data=data, overwrite=True)
                                
                                total_uploaded += 1
                                if total_uploaded % 10 == 0:
                                    self.stdout.write(f"  {total_uploaded}ファイルをアップロード済み...")
                    else:
                        # 天気データも月別ディレクトリがある
                        months = [d for d in os.listdir(year_dir) if os.path.isdir(os.path.join(year_dir, d))]
                        for month in months:
                            month_dir = os.path.join(year_dir, month)
                            files = [f for f in os.listdir(month_dir) if os.path.isfile(os.path.join(month_dir, f))]
                            
                            for file in files:
                                # 天気データのAzuriteパスは: weather/YYYY/MM/file.csv
                                local_path = os.path.join(month_dir, file)
                                azure_path = f"weather/{year}/{month}/{file}"
                                
                                with open(local_path, 'rb') as data:
                                    container.upload_blob(name=azure_path, data=data, overwrite=True)
                                
                                total_uploaded += 1
                                if total_uploaded % 10 == 0:
                                    self.stdout.write(f"  {total_uploaded}ファイルをアップロード済み...")
            
            self.stdout.write(self.style.SUCCESS(f"アップロード完了: 合計{total_uploaded}ファイル"))
        except Exception as e:
            self.stdout.write(self.style.ERROR(f"アップロード中にエラーが発生しました: {str(e)}"))