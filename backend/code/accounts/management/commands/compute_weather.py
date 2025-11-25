from django.core.management.base import BaseCommand
from compute.models import ComputeWeather

class Command(BaseCommand):
    help = '気象データの集計処理を実行します。'

    def handle(self, *args, **options):
        self.stdout.write(self.style.NOTICE('気象データの集計処理を開始します...'))
        # 集計ロジック（例: aggregate_weather_data）を呼び出す
        count = ComputeWeather.aggregate_weather_data()  # 実装済みの集計メソッド名に合わせて修正
        self.stdout.write(self.style.SUCCESS(f'集計処理が完了しました。集計件数: {count}'))