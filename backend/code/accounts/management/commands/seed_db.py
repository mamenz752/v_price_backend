from django.core.management.base import BaseCommand
from django.db import transaction
from compute.models import Region, Vegetable
from forecast.models import ForecastModelKind, ForecastModelVariable
import datetime

class Command(BaseCommand):
    help = 'Seed the database with initial data'

    def handle(self, *args, **options):
        self.stdout.write(self.style.SUCCESS('Database seeding started...'))
        
        with transaction.atomic():
            # 地域データの作成
            self.seed_regions()
            # 野菜データの作成
            self.seed_vegetables()
            # 予測モデル種別の作成
            self.seed_forecast_model_kinds()
            # 予測モデル変数の作成
            self.seed_forecast_model_variables()
        
        self.stdout.write(self.style.SUCCESS('Database seeding completed successfully'))

    def seed_regions(self):
        """地域データを作成"""
        regions = [
            {
                "name": "広島",
                "p_area_code": "034",
                "market_code": "34300",
                "fuken_code": "67",
                "station_code": "47765",
            }
        ]

        for region in regions:
            obj, created = Region.objects.get_or_create(
                name=region["name"],
                defaults={
                    "p_area_code": region["p_area_code"],
                    "market_code": region["market_code"],
                    "fuken_code": region["fuken_code"],
                    "station_code": region["station_code"],
                }
            )
            status = 'created' if created else 'already exists'
            self.stdout.write(f'Region: {region["name"]} ({status})')

    def seed_vegetables(self):
        """野菜データを作成"""
        vegetables = [
            {
                "name": "キャベツ",
                "code": "31700",
            },
            {
                "name": "はくさい",
                "code": "31100",
            },
            {
                "name": "なす",
                "code": "34300",
            },
            {
                "name": "だいこん",
                "code": "30100",
            },
            {
                "name": "きゅうり",
                "code": "34100",
            },
            {
                "name": "ばれいしょ",
                "code": "36200",
            },
            {
                "name": "トマト",
                "code": "34400",
            },
            {
                "name": "たまねぎ",
                "code": "36600",
            },
        ]
        
        for v in vegetables:
            obj, created = Vegetable.objects.get_or_create(
                name=v["name"],
                defaults={
                    "code": v["code"],
                }
            )
            status = 'created' if created else 'already exists'
            self.stdout.write(f'Vegetable: {v["name"]} ({status})')

    def seed_forecast_model_kinds(self):
        """予測モデル種別を作成"""
        vegetables = Vegetable.objects.all()
        
        for kind in vegetables:
            tag_name = f'テスト_{kind.name}'
            model_kind, created = ForecastModelKind.objects.get_or_create(
                tag_name=tag_name,
                defaults={
                    'vegetable': kind,
                }
            )
            status = 'created' if created else 'already exists'
            self.stdout.write(f'ForecastModelKind: {kind.name} ({status})')

    def seed_forecast_model_variables(self):
        weather_variables = ["mean_temp", "min_temp", "max_temp", "sunshine_duration", "sum_precipitation", "ave_humidity"]

        const = ForecastModelVariable.objects.get_or_create(
            name="const",
            defaults={
                "previous_term": 0,
            }
        )
        prev_price = ForecastModelVariable.objects.get_or_create(
            name="prev_price",
            defaults={
                "previous_term": 0,
            }
        )
        prev_volume = ForecastModelVariable.objects.get_or_create(
            name="prev_volume",
            defaults={
                "previous_term": 0,
            }
        )
        years_price = ForecastModelVariable.objects.get_or_create(
            name="years_price",
            defaults={
                "previous_term": 0,
            }
        )
        years_volume = ForecastModelVariable.objects.get_or_create(
            name="years_volume",
            defaults={
                "previous_term": 0,
            }
        )
        self.stdout.write(f'ForecastModelVariable: const (created)')

        for wv in weather_variables:
            for term in range(1, 25):
                variable, created = ForecastModelVariable.objects.get_or_create(
                    name=wv.name,
                    defaults={
                        "previous_term": term,
                    }
                )
                status = 'created' if created else 'already exists'
                self.stdout.write(f'ForecastModelVariable: {wv.name} ({status})')