from django.core.management.base import BaseCommand
from django.db import transaction, connection
from forecast.models import (
    ForecastModelKind,
    ForecastModelVariable,
    ForecastModelFeatureSet
)
from ingest.models import Vegetable
import logging

logger = logging.getLogger(__name__)

class Command(BaseCommand):
    help = 'Seed forecast model data (model kinds, variables, and feature sets)'

    def add_arguments(self, parser):
        parser.add_argument(
            '--reset',
            action='store_true',
            help='Delete all existing forecast model data before seeding and reset IDs to 1',
        )

    def handle(self, *args, **options):
        reset = options.get('reset', False)

        with transaction.atomic():
            if reset:
                self.stdout.write(self.style.WARNING('Deleting all existing forecast model data...'))
                ForecastModelFeatureSet.objects.all().delete()
                ForecastModelVariable.objects.all().delete()
                ForecastModelKind.objects.all().delete()
                
                # IDシーケンスをリセット
                self.reset_sequences()
                
                self.stdout.write(self.style.SUCCESS('Existing forecast model data deleted and sequences reset.'))

            # 1. シードモデル変数の作成
            self.seed_model_variables()
            
            # 2. シードモデル種類の作成
            self.seed_model_kinds()
            
            # 3. シードモデル特徴セットの作成
            self.seed_model_feature_sets()

            self.stdout.write(self.style.SUCCESS('Successfully seeded forecast model data.'))
            
    def reset_sequences(self):
        """IDシーケンスをリセットし、次のIDを1から始まるようにする"""
        self.stdout.write('Resetting ID sequences...')
        
        # データベースエンジンに応じてシーケンスをリセット
        db_engine = connection.vendor
        
        with connection.cursor() as cursor:
            if db_engine == 'postgresql':
                # PostgreSQLの場合
                cursor.execute("SELECT setval('forecast_forecastmodelkind_id_seq', 1, false);")
                cursor.execute("SELECT setval('forecast_forecastmodelvariable_id_seq', 1, false);")
                cursor.execute("SELECT setval('forecast_forecastmodelfeatureset_id_seq', 1, false);")
                self.stdout.write('PostgreSQL sequences reset.')
            
            elif db_engine == 'sqlite':
                # SQLiteの場合
                cursor.execute("DELETE FROM sqlite_sequence WHERE name='forecast_forecastmodelkind';")
                cursor.execute("DELETE FROM sqlite_sequence WHERE name='forecast_forecastmodelvariable';")
                cursor.execute("DELETE FROM sqlite_sequence WHERE name='forecast_forecastmodelfeatureset';")
                self.stdout.write('SQLite sequences reset.')
            
            elif db_engine == 'mysql':
                # MySQLの場合
                cursor.execute("ALTER TABLE forecast_forecastmodelkind AUTO_INCREMENT = 1;")
                cursor.execute("ALTER TABLE forecast_forecastmodelvariable AUTO_INCREMENT = 1;")
                cursor.execute("ALTER TABLE forecast_forecastmodelfeatureset AUTO_INCREMENT = 1;")
                self.stdout.write('MySQL sequences reset.')
            
            else:
                self.stdout.write(self.style.WARNING(f'Sequence reset not implemented for {db_engine}. IDs might not start from 1.'))

    def seed_model_variables(self):
        """モデル変数のシードデータを作成"""
        self.stdout.write('Creating model variables...')
        
        # 天気関連の変数名
        weather_variables = [
            'max_temp',     # 最高気温
            'mean_temp',    # 平均気温
            'min_temp',     # 最低気温
            'sum_precipitation',  # 降水量
            'sunshine_duration',  # 日照時間
            'ave_humidity'  # 平均湿度
        ]
        
        # 各変数について、1〜24までのprevious_termを作成
        count = 0
        for var_name in weather_variables:
            for term in range(1, 25):  # 1〜24ヶ月前
                var, created = ForecastModelVariable.objects.get_or_create(
                    name=var_name,
                    previous_term=term,
                )
                if created:
                    count += 1
                    self.stdout.write(f'  - Created weather variable: {var.name} (term: {var.previous_term})')
        
        self.stdout.write(self.style.SUCCESS(f'Created {count} model variables.'))

    def seed_model_kinds(self):
        """モデル種類のシードデータを作成"""
        self.stdout.write('Creating model kinds...')
        
        # 野菜の取得
        try:
            cabbage = Vegetable.objects.get(name='キャベツ')
        except Vegetable.DoesNotExist:
            self.stdout.write(self.style.ERROR('キャベツが見つかりません。先に野菜データをシードしてください。'))
            cabbage = None
        
        # 他の野菜も取得
        vegetables = Vegetable.objects.exclude(name='キャベツ') if cabbage else Vegetable.objects.all()
        
        count = 0
        # キャベツ用のモデル種類
        if cabbage:
            # キャベツ専用のモデル種類
            cabbage_model_types = [
                {'tag_name': 'キャベツ春まき', 'vegetable': cabbage},
                {'tag_name': 'キャベツ秋まき', 'vegetable': cabbage},
                {'tag_name': 'キャベツ冬まき', 'vegetable': cabbage},
            ]
            
            for kind_data in cabbage_model_types:
                kind, created = ForecastModelKind.objects.get_or_create(
                    tag_name=kind_data['tag_name'],
                    vegetable=kind_data['vegetable'],
                )
                if created:
                    count += 1
                    self.stdout.write(f'  - Created model kind: {kind.tag_name}')
        
        # 他の野菜用のモデル種類（オプション）
        for vegetable in vegetables:
            model_types = [
                {'tag_name': f'{vegetable.name}_標準', 'vegetable': vegetable},
            ]
            
            for kind_data in model_types:
                kind, created = ForecastModelKind.objects.get_or_create(
                    tag_name=kind_data['tag_name'],
                    vegetable=kind_data['vegetable'],
                )
                if created:
                    count += 1
                    self.stdout.write(f'  - Created model kind: {kind.tag_name}')
        
        self.stdout.write(self.style.SUCCESS(f'Created {count} model kinds.'))

    def seed_model_feature_sets(self):
        """モデル特徴セットのシードデータを作成"""
        self.stdout.write('Creating model feature sets...')
        
        # キャベツ春まきのモデル種類を取得
        try:
            cabbage_spring_model = ForecastModelKind.objects.get(tag_name='キャベツ春まき')
        except ForecastModelKind.DoesNotExist:
            self.stdout.write(self.style.ERROR('キャベツ春まきのモデル種類が見つかりません。先にモデル種類をシードしてください。'))
            return
        
        # 対象月は5月, 6月, 7月
        target_months = [5, 6, 7]
        
        # 特徴セット作成用の変数を取得
        # ここでは変数の設定は行いますが、具体的な関連付けは後ほど行うため
        # 実際の特徴セット作成は行いません（コメントアウト）
        
        self.stdout.write('Setting up feature sets for cabbage spring model in months 5, 6, 7...')
        self.stdout.write('NOTE: Actual feature set associations will be done separately.')
        
        # 以下はコメントアウト - 実際の関連付けは後ほど行う
        """
        variables = ForecastModelVariable.objects.filter(
            name__in=['max_temp', 'mean_temp', 'min_temp', 'sum_precipitation', 'sunshine_duration', 'ave_humidity']
        )
        
        count = 0
        for month in target_months:
            for variable in variables:
                feature_set, created = ForecastModelFeatureSet.objects.get_or_create(
                    target_month=month,
                    model_kind=cabbage_spring_model,
                    variable=variable,
                )
                if created:
                    count += 1
                    self.stdout.write(f'  - Created feature set: Month {month}, Model {cabbage_spring_model.tag_name}, Variable {variable.name}')
        """
        
        self.stdout.write(self.style.SUCCESS('Feature set structure prepared for months 5, 6, 7 with キャベツ春まき model.'))
