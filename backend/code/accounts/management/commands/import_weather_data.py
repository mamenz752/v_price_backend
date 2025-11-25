from django.core.management.base import BaseCommand
from ingest.services import FileProcessor
from django.conf import settings

class Command(BaseCommand):
    help = '天気データ（IngestWeather）を一括インポートします。'

    def add_arguments(self, parser):
        parser.add_argument('--base_dir', type=str, default=None, help='インポート元ディレクトリ（未指定ならsettingsから取得）')

    def handle(self, *args, **options):
        base_dir = options.get('base_dir')
        self.stdout.write(self.style.NOTICE(f'天気データのインポートを開始します...'))
        result = FileProcessor.process_all_weather_data(base_dir=base_dir)
        for region_name, count in result.items():
            self.stdout.write(f'  {region_name}: {count}件インポート')
        self.stdout.write(self.style.SUCCESS('天気データのインポートが完了しました。'))