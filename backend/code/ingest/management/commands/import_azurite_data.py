from django.core.management.base import BaseCommand
from django.conf import settings
import os
import logging

from ingest.services import FileProcessor

logger = logging.getLogger(__name__)

class Command(BaseCommand):
    help = 'Azuriteストレージからデータをインポートしてデータベースに保存します'

    def add_arguments(self, parser):
        parser.add_argument('--price-only', action='store_true', help='価格データのみをインポートする')
        parser.add_argument('--weather-only', action='store_true', help='天気データのみをインポートする')
        parser.add_argument('--price-dir', type=str, help='価格データのディレクトリパス')
        parser.add_argument('--weather-dir', type=str, help='天気データのディレクトリパス')

    def handle(self, *args, **options):
        self.stdout.write('Azuriteからのデータインポートを開始します...')
        
        price_dir = options.get('price_dir')
        weather_dir = options.get('weather_dir')
        
        price_only = options.get('price_only')
        weather_only = options.get('weather_only')
        
        # どちらもFalseならば両方インポート
        if not price_only and not weather_only:
            self.import_all_data(price_dir, weather_dir)
        elif price_only:
            self.import_price_data(price_dir)
        elif weather_only:
            self.import_weather_data(weather_dir)
        
        self.stdout.write(self.style.SUCCESS('インポート処理が完了しました！'))
        
    def import_all_data(self, price_dir=None, weather_dir=None):
        """全データのインポート"""
        self.stdout.write('全データのインポートを実行中...')
        
        # 価格データのインポート
        self.import_price_data(price_dir)
        
        # 天気データのインポート
        self.import_weather_data(weather_dir)
        
    def import_price_data(self, price_dir=None):
        """価格データのインポート"""
        self.stdout.write('価格データのインポートを実行中...')
        
        if price_dir is None:
            price_dir = os.path.join(settings.MEDIA_ROOT, settings.INGEST_PREFIX_PRICE)
        
        results = FileProcessor.process_all_price_data(price_dir)
        
        total_count = sum(results.values())
        self.stdout.write(self.style.SUCCESS(f'価格データのインポート完了: 合計 {total_count} 件'))
        
        # 詳細結果を表示
        for vegetable_name, count in results.items():
            self.stdout.write(f'  - {vegetable_name}: {count} 件')
            
    def import_weather_data(self, weather_dir=None):
        """天気データのインポート"""
        self.stdout.write('天気データのインポートを実行中...')
        
        if weather_dir is None:
            weather_dir = os.path.join(settings.MEDIA_ROOT, settings.INGEST_PREFIX_WEATHER)
        
        results = FileProcessor.process_all_weather_data(weather_dir)
        
        total_count = sum(results.values())
        self.stdout.write(self.style.SUCCESS(f'天気データのインポート完了: 合計 {total_count} 件'))
        
        # 詳細結果を表示
        for region_name, count in results.items():
            self.stdout.write(f'  - {region_name}: {count} 件')