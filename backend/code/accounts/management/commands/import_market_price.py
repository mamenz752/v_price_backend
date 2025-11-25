from django.core.management.base import BaseCommand
from ingest.services import FileProcessor
from ingest.models import Vegetable
from django.conf import settings

class Command(BaseCommand):
    help = '価格データ（IngestMarket）を一括インポートします。'

    def add_arguments(self, parser):
        parser.add_argument('--base_dir', type=str, default=None, help='インポート元ディレクトリ（未指定ならsettingsから取得）')

    def handle(self, *args, **options):
        base_dir = "/"
        self.stdout.write(self.style.NOTICE(f'価格データのインポートを開始します...'))
        result = FileProcessor.process_all_price_data(base_dir=base_dir)
        self.stdout.write(self.style.SUCCESS('価格データのインポートが完了しました。'))