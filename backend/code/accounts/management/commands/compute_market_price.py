from django.core.management.base import BaseCommand
from compute.service import aggregate_market_data

class Command(BaseCommand):
    help = '価格データの集計処理を実行します。'

    def handle(self, *args, **options):
        self.stdout.write(self.style.NOTICE('価格データの集計処理を開始します...'))
        # ここで集計ロジックを呼び出す
        count = aggregate_market_data()  # 例: 実装済みの集計メソッド
        self.stdout.write(self.style.SUCCESS(f'集計処理が完了しました。集計件数: {count}'))