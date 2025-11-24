#!/usr/bin/env python
"""
prepare_regression_data のテスト
"""
import os
import sys
import django

# /code がワーキングディレクトリ
sys.path.insert(0, '/code')
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'config.settings_local')
django.setup()

from forecast.service.run_ols import ForecastOLSRunner, ForecastOLSConfig
from forecast.service.build_matrix import ForecastModelDataBuilder
from ingest.models import Region

try:
    # 利用可能な地域を確認
    regions = Region.objects.all()
    print(f"利用可能な地域: {list(regions.values_list('name', flat=True))}")
    
    region_name = regions.first().name if regions.exists() else "広島"
    
    cfg = ForecastOLSConfig(region_name=region_name, min_obs_margin=1)
    data_builder = ForecastModelDataBuilder(region_name=region_name)
    runner = ForecastOLSRunner(data_builder=data_builder, config=cfg)
    
    # prepare_regression_data を直接呼び出す
    print("=" * 80)
    print("prepare_regression_data のテスト開始")
    print("=" * 80)
    
    X, y, variable_list = runner.prepare_regression_data(
        model_name="キャベツ春まき",
        target_month=5,
        vals=[10, 12, 13]
    )
    
    print("\n[成功] prepare_regression_data 実行完了")
    print(f"X shape: {X.shape}")
    print(f"X columns: {X.columns.tolist()}")
    print(f"y length: {len(y)}")
    print(f"variable_list: {variable_list}")
    
    print("\n[X の最初の数行]")
    print(X.head())
    
    print("\n[y の最初の数要素]")
    print(y.head())
    
except Exception as e:
    print(f"\n[エラー] {type(e).__name__}: {str(e)}")
    import traceback
    traceback.print_exc()
