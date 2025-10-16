from django.core.management.base import BaseCommand
from django.db import transaction
from ingest.models import Vegetable, Region

class Command(BaseCommand):
    help = '野菜と地域のシードデータを登録するコマンド'

    def handle(self, *args, **kwargs):
        self.stdout.write('野菜と地域のシードデータ登録を開始します...')
        
        try:
            # トランザクション内で処理を行う
            with transaction.atomic():
                # 野菜データ
                self._create_vegetables()
                # 地域データ
                self._create_regions()
                
            self.stdout.write(self.style.SUCCESS('シードデータの登録が完了しました！'))
        except Exception as e:
            self.stdout.write(self.style.ERROR(f'エラーが発生しました: {str(e)}'))
    
    def _create_vegetables(self):
        """野菜のシードデータを作成する"""
        # 既存データを確認
        if Vegetable.objects.exists():
            self.stdout.write('野菜のデータは既に存在します。スキップします。')
            return
            
        # 野菜データのリスト
        vegetables = [
            {'name': 'キャベツ', 'code': '31700'},
            {'name': 'はくさい', 'code': '31100'},
            {'name': 'だいこん', 'code': '30100'},
            {'name': 'たまねぎ', 'code': '36600'},
            {'name': 'ばれいしょ', 'code': '36200'},
            {'name': 'トマト', 'code': '34400'},
            {'name': 'なす', 'code': '34300'},
            {'name': 'きゅうり', 'code': '34100'},
        ]
        
        # データの作成
        for veg_data in vegetables:
            Vegetable.objects.create(
                name=veg_data['name'],
                code=veg_data['code']
            )
            self.stdout.write(f'野菜を登録しました: {veg_data["name"]}')
    
    def _create_regions(self):
        """地域のシードデータを作成する"""
        # 既存データを確認
        if Region.objects.exists():
            self.stdout.write('地域のデータは既に存在します。スキップします。')
            return
            
        # 地域データのリスト
        regions = [
            {'name': '広島', 'p_area_code': '034', 'market_code': '34300', 'fuken_code': '67', 'station_code': '47765'},
        ]
        
        # データの作成
        for region_data in regions:
            Region.objects.create(
                name=region_data['name'],
                p_area_code=region_data['p_area_code'],
                market_code=region_data['market_code'],
                fuken_code=region_data['fuken_code'],
                station_code=region_data['station_code']
            )
            self.stdout.write(f'地域を登録しました: {region_data["name"]}')