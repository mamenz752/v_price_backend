from django.core.management.base import BaseCommand
from django.db import transaction
from ingest.models import IngestMarket, IngestWeather

class Command(BaseCommand):
    help = '市場データと気象データを一括削除するコマンド'

    def add_arguments(self, parser):
        parser.add_argument(
            '--price-only',
            action='store_true',
            help='価格データのみ削除',
        )
        parser.add_argument(
            '--weather-only',
            action='store_true',
            help='気象データのみ削除',
        )
        parser.add_argument(
            '--confirm',
            action='store_true',
            help='確認せずに削除を実行',
        )

    def handle(self, *args, **options):
        price_only = options['price_only']
        weather_only = options['weather_only']
        confirm = options['confirm']
        
        if not confirm:
            self.stdout.write('警告: この操作は取り消せません。データを完全に削除します。')
            response = input('続行しますか？ [y/N]: ')
            if response.lower() != 'y':
                self.stdout.write(self.style.WARNING('操作をキャンセルしました。'))
                return

        try:
            with transaction.atomic():
                delete_price = not weather_only
                delete_weather = not price_only
                
                if delete_price:
                    price_count = IngestMarket.objects.count()
                    IngestMarket.objects.all().delete()
                    self.stdout.write(self.style.SUCCESS(f'価格データを {price_count} 件削除しました。'))
                
                if delete_weather:
                    weather_count = IngestWeather.objects.count()
                    IngestWeather.objects.all().delete()
                    self.stdout.write(self.style.SUCCESS(f'気象データを {weather_count} 件削除しました。'))
                
            self.stdout.write(self.style.SUCCESS('データの削除が完了しました。'))
        except Exception as e:
            self.stdout.write(self.style.ERROR(f'エラーが発生しました: {str(e)}'))